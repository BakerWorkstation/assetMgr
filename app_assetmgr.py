# -*- encoding:utf-8 -*-
# @Date    : 2019-04-02
# @Author  : sdc

# 资产管理
import json
import re
import os
import sys
import copy
import urllib2
import ipaddr
import uuid as U
import ConfigParser
from IPy import IP
from flask import Blueprint, g, request, make_response, render_template, redirect, url_for, send_from_directory
from psycopg2 import connect
from psycopg2.extras import RealDictCursor, DictCursor
from elasticsearch import Elasticsearch
from lib.validform import V, ValidateForm
from lib import auth, funcs
from config import config
from lib.table import table
from datetime import datetime, timedelta
import confluent_kafka
reload(sys)
import xlrd
sys.setdefaultencoding('utf-8')
app = Blueprint(__name__ + "_app", __name__)


# 连接/关闭 数据库
@app.before_request
def setupdb():
    g.conn = connect(**config.DatabaseConfig.siem)
    g.cursor = g.conn.cursor(cursor_factory=RealDictCursor)


@app.teardown_request
def unsetdb(exception):
    if g.cursor:
        g.cursor.close()
        g.cursor = None
    if g.conn:
        g.conn.close()
        g.conn = None


def readKafkaInI():
    iniFileUrl = "/opt/CyberSA/src/management_center/api/sap.ini"
    conf = ConfigParser.ConfigParser()
    conf.read(iniFileUrl)
    kafka_ip = conf.get('KAFKA', 'ip')
    kafka_port = conf.get('KAFKA', 'port')
    return kafka_ip, kafka_port


def convert_ip_to_number(ip_str):
    ret = 0
    ip_str=ip_str.strip()
    parts = ip_str.split('.')
    if len(parts) == 4:
        ret = int(parts[0]) * 256 * 256 * 256 + int(parts[1]) * 256 * 256 + int(parts[2]) * 256  + int(parts[3])
    return ret


class IpInfo(object):
    def __init__(self, ipaddr, netmask):
        self.ipaddr = ipaddr
        self.netmask = netmask

    def calIpvalue(self):
        iprange = IP(self.ipaddr).make_net(self.netmask).strNormal()
        ip = IP(iprange)
        data = []
        for x in (ip[0], ip[-1]):
            data.append((x.int()))
        return data


# 组织结构树
def organ_tree(gid, groups):
    try:
        sql = " select node_id from sys_node_info where parent_node_id=%s "
        args = (gid, )
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        g.conn.commit()
        for eachresult in result:
            id = eachresult["node_id"]
            if not id :
                return groups
            groups.append(id)
            groups = organ_tree(id, groups)
    except:
        return []
    return groups


# 资产组树
def assetgroup_tree(gid, groups):
    try:
        sql = " select group_id from h_asset_group where parent_group_id=%s "
        args = (gid, )
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        for eachresult in result:
            id = eachresult["group_id"]
            if not id :
                return groups
            groups.append(id)
            groups = assetgroup_tree(id, groups)
    except:
        return []
    return groups


# 资产类型
def get_type(type_id, groups):
    try:
        sql = " select type_id as id from h_asset_type where parent_type_id=%s "
        args = (int(type_id), )
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        g.conn.commit()
        for eachresult in result:
            id = eachresult["id"]
            if not id :
                return groups
            groups.append(str(id))
            groups = get_type(id, groups)
    except:
        return []
    return groups


def getEveryDay(begin_date, end_date):
    date_list = []
    begin_date = datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += timedelta(days=1)
    return date_list


# 记录资产操作日志
def asset_manul_log(uuid, uid, manul, comment):
    try:
        sql = "insert into h_hardware_asset_operate(asset_id, operate_type, operate_detail, operate_person, operate_time) values(%s, %s, %s, %s, %s)"
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        args = (uuid, manul, comment, uid, time, )
        g.cursor.execute(sql, args)
        g.conn.commit()
    except:
        pass
    return None


# ---------------------- API 接口 ----------------------------------- #

# 查看组列表树 /asset/grouptree
@app.route('/asset/grouptree', methods=['POST'])
@auth.permission("asset")
def group_get(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    tree = body["tree"] if body.has_key("tree") else ""
    # 组织结构
    if tree == "0":
        sql = "select node_id from sys_node_info where parent_node_id = '0'"
        args = ()
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
            gid = data["node_id"]
        except:
            gid = None
        sql = " select node_id as id, node_name as name, node_name as name2, case when parent_node_id is null then '0' else parent_node_id end as pid from sys_node_info sinfo order by pid"
    # 资产组
    elif tree == "1":
        sql = " select group_id from h_asset_group where parent_group_id is null and state = '1' "
        args = ()
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
            gid = data["group_id"]
        except:
            gid = None
        sql = "select group_id as id, group_name as name, group_name as name2, type, sub_type, case when parent_group_id is null then '0'  else parent_group_id end as pid, remark as comment from h_asset_group agroup where state = '1' order by create_time"
    else:
        return json.dumps({"message": u"查询类型错误"})
    args = ()
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchall()
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    if not data:
        return json.dumps({"message": "ok", "data": []})

    # for eachdata in data:
    #     id = eachdata["id"]
    #     name = eachdata["name"]
    #     groups = [id]
    #     if tree == "0":
    #         groups = organ_tree(id, groups)
    #         sql = "select count(asset_id) from h_hardware_asset_info where node_id IN ( SELECT unnest ( array [%s] ) ) and delete_state = '1'"
    #     if tree == "1":
    #         groups = assetgroup_tree(id, groups)
    #         sql = "select count(asset_id) from h_hardware_asset_info where group_id IN ( SELECT unnest ( array [%s] ) ) and delete_state = '1'"
    #     try:
    #         args = (groups, )
    #         g.cursor.execute(sql, args)
    #         count = g.cursor.fetchone()
    #         count = count["count"]
    #         name2 = name + ' (%s)' % count
    #         eachdata['name2'] = name2
    #     except Exception as e:
    #         print 'fail : %s\n' % str(e)
    #         return json.dumps({"message": u"查询数据库失败"})
    # sql = "select count(asset_id) from h_hardware_asset_info where delete_state = '1' "
    # if tree == "0":
    #     sql += " and node_id is null "
    # else:
    #     sql += " and group_id is null "
    # args = ()
    # try:
    #     g.cursor.execute(sql, args)
    #     count = g.cursor.fetchone()
    #     count = count["count"]
    # except:
    #     count = 0

    g.conn.commit()

    return json.dumps({"message": "ok", "data": data})


# 编辑前获取组信息 /asset/group/getinfo
@app.route('/asset/group/getinfo', methods=['POST'])
@auth.permission("asset")
def group_getinfo(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    tree = body["tree"] if body.has_key("tree") else ""
    gid = body["gid"] if body.has_key("gid") else None
    # 资产组信息
    if tree == "1":
        sql = '''
                 select
                    group_name,
                    create_person as cperson_name,
                    manage_person as mperson_name,
                    create_contact as cperson_tel,
                    manage_contact as mperson_tel,
                    parent_group_id as pid,
                    relation.node_id as oid,
                    type,
                    sub_type,
                    area_type as area_type,
                    (select
                     array_agg (
                                 case when  conf_type = '1' then array['1', ip_addr, subnet_mask] when conf_type = '2' then array['2', start_ip_addr, end_ip_addr] else array['3', ip_group, null] end  
                    )
                 from
                    h_network_domain_range 
                 where
                    hgroup.group_id = %s) as ipinfo,
                    remark as comment  
                 from
                    h_asset_group hgroup
                    left join  h_node_group_relation relation on hgroup.group_id = relation.group_id 
                 where
                    hgroup.group_id = %s 
        '''
    else:
        return json.dumps({"message": u"查询类型错误"})
    args = (gid, gid, )
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchone()
        g.conn.commit()
        return json.dumps({"message": "ok", "data": data})
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})


# 编辑组 /asset/group/edit
@app.route('/asset/group/edit', methods=['POST'])
@auth.permission("asset")
def group_edit(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    tree = body["tree"] if body.has_key("tree") else ""
    gid = body["gid"] if body.has_key("gid") else None
    oid = body["oid"] if body.has_key("oid") else None
    name = body["name"] if body.has_key("name") else ""
    type = body["type"] if body.has_key("type") else None
    area_type = body["area_type"] if body.has_key("area_type") else None
    cperson_name = body["cperson_name"] if body.has_key("cperson_name") else ""  #  创建者
    mperson_name = body["mperson_name"] if body.has_key("mperson_name") else ""  #  管理者
    cperson_tel = body["cperson_tel"] if body.has_key("cperson_tel") else ""  #  创建者联系方式
    mperson_tel = body["mperson_tel"] if body.has_key("mperson_tel") else ""  #  管理者联系方式
    pid = body["pid"] if body.has_key("pid") else None
    comment = body["comment"] if body.has_key("comment") else ""
    # 编辑资产组
    if tree == "1":
        netarea = body["netarea"] if body.has_key("netarea") else ""
        sql = "select group_name from h_asset_group where parent_group_id = %s and group_name = %s and group_id <> %s and state='1' "
        args = (pid, name, gid,)
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            tmpname = result["group_name"]
        except:
            tmpname = ""
        if tmpname:
            return json.dumps({"message": u"资产组名称已存在"})
        # 获取父级网络域IP数值信息
        ipvaluelist = []
        sql = "select start_ip_value, end_ip_value from h_network_domain_range where group_id = %s "
        args = (pid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchall()
            for eachresult in result:
                start_ip_value = eachresult["start_ip_value"]
                end_ip_value = eachresult["end_ip_value"]
                ipvaluelist.append([start_ip_value, end_ip_value])
        except:
            pass
        # 获取子级网络域IP数值信息
        sql = "select array_agg(group_id) as group_id from h_asset_group where parent_group_id = %s and state='1' "
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            childgroup = result["group_id"]
        except:
            childgroup = []
        childipValue = []
        sql = "select start_ip_value, end_ip_value from h_network_domain_range where group_id IN ( SELECT unnest ( array [%s] ) ) "
        args = (childgroup, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchall()
            for eachresult in result:
                start_ip_value = eachresult["start_ip_value"]
                end_ip_value = eachresult["end_ip_value"]
                childipValue.append([start_ip_value, end_ip_value])
        except:
            pass
        # 更新资产组信息
        sql = "update h_asset_group set area_type=%s, group_name=%s, remark=%s, manage_person=%s, create_person=%s, create_contact=%s, manage_contact=%s where group_id=%s "
        args = (area_type, name, comment, mperson_name, cperson_name, cperson_tel, mperson_tel, gid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"更新数据库失败"})

        if type == "1":
            # 删除组织结构-网络结构中间表数据
            sql = "delete from h_node_group_relation where group_id = %s"
            args = (gid, )
            try:
                g.cursor.execute(sql, args)
            except:
                return json.dumps({"message": u"删除数据库失败"})
            sql = "insert into h_node_group_relation (group_id, node_id) values(%s, %s)"
            args = (gid, oid, )
            try:
                g.cursor.execute(sql, args)
            except:
                return json.dumps({"message": u"插入数据库失败"})
    
            # 更新资产绑定的组织结构
            update_sql = "update h_hardware_asset_info set node_id = %s where group_id = %s"
            update_args = (oid, gid, )
            try:
                g.cursor.execute(update_sql, update_args)
            except:
                return json.dumps({"message": u"更新数据库失败"})

        # 更新网络结构网络信息
        sql = "delete from h_network_domain_range where group_id = %s"
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"删除数据库失败"})
        sql = "insert into h_network_domain_range(group_id, conf_type, ip_addr, subnet_mask, start_ip_addr, end_ip_addr, start_ip_value, end_ip_value, ip_group, ip_group_value) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        parentipValue = []
        for eachinfo in netarea:
            iptype = eachinfo["type"]
            ipaddr = None
            netmask = None
            startip = None
            endip = None
            start_ip_value = None
            end_ip_value = None
            ip_group = None
            ip_group_value = None
            if iptype == "1":
                ipaddr = eachinfo["ipaddr"]
                netmask = eachinfo["netmask"]
                ip = IpInfo(ipaddr, netmask)
                try:
                    start_ip_value, end_ip_value = ip.calIpvalue()
                    parentipValue.append([start_ip_value, end_ip_value])
                except:
                    return json.dumps({"message": u"IP、掩码格式有误"})
            if iptype == "2":
                startip = eachinfo["startip"]
                endip = eachinfo["endip"]
                try:
                    start_ip_value = IP(startip).int()
                    end_ip_value = IP(endip).int()
                    parentipValue.append([start_ip_value, end_ip_value])
                except:
                    return json.dumps({"message": u"IP网络域格式有误"})
            if iptype == "3":
                ip_group = eachinfo["ip"]
                try:
                    ip_group_value = IP(ip_group).int()
                    #parentipValue.append([start_ip_value, end_ip_value])
                except Exception as e:
                    print 'fail : %s\n' % str(e)
                    return json.dumps({"message": u"IP网络域格式有误"})
            args = (gid, iptype, ipaddr, netmask, startip, endip, start_ip_value, end_ip_value, ip_group, ip_group_value, )
            try:
                g.cursor.execute(sql, args)
            except:
                return json.dumps({"message": u"插入数据库失败"})

        select_sql = "select type, parent_group_id from h_asset_group where group_id = %s"
        select_args = (gid,)
        try:
            g.cursor.execute(select_sql, select_args)
            result = g.cursor.fetchone()
            conf_type = result["type"]
            pid = result["parent_group_id"]
        except:
            pid = None
            conf_type = None

        select_sql = "select type from h_asset_group where group_id = %s"
        select_args = (pid,)
        try:
            g.cursor.execute(select_sql, select_args)
            result = g.cursor.fetchone()
            parent_conf_type = result["type"]
        except:
            parent_conf_type = None

        if parent_conf_type == "2":
            # 判断是否属于父级网络域中
            for eachchild in parentipValue:
                flag = False
                start_child = eachchild[0]
                end_child = eachchild[1]
                for eachparent in ipvaluelist:
                    start_parent = eachparent[0]
                    end_parent = eachparent[1]
                    if start_child >= start_parent and end_child <= end_parent:
                        flag = True
                        break
                if not flag:
                    return json.dumps({"message": u"当前网络域不在父网络域中"})

        if conf_type == "2":
            # 判断是否属于子级网络域中
            for eachchild in childipValue:
                flag = False
                start_child = eachchild[0]
                end_child = eachchild[1]
                for eachparent in parentipValue:
                    start_parent = eachparent[0]
                    end_parent = eachparent[1]
                    if start_child >= start_parent and end_child <= end_parent:
                        flag = True
                        break
                if not flag:
                    return json.dumps({"message": u"当前网络域不在子网络域中"})
    g.conn.commit()

    return json.dumps({"message": "ok"})


# 创建设备组 /asset/group/new
@app.route('/asset/group/new', methods=['POST'])
@auth.permission("asset")
def group_new(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    gid = str(U.uuid1()).replace('-', '')
    tree = body["tree"] if body.has_key("tree") else ""
    name = body["name"] if body.has_key("name") else ""
    pid = body["pid"] if body.has_key("pid") else None
    oid = body["oid"] if body.has_key("oid") else None
    type = body["type"] if body.has_key("type") else None
    sub_type = body["sub_type"] if body.has_key("sub_type") else None
    area_type = body["area_type"] if body.has_key("area_type") else None
    cperson_name = body["cperson_name"] if body.has_key("cperson_name") else ""  #  创建者
    mperson_name = body["mperson_name"] if body.has_key("mperson_name") else ""  #  管理者
    cperson_tel = body["cperson_tel"] if body.has_key("cperson_tel") else ""  #  创建者联系方式
    mperson_tel = body["mperson_tel"] if body.has_key("mperson_tel") else ""  #  管理者联系方式
    comment = body["comment"] if body.has_key("comment") else ""
    # 创建资产组
    if tree == "1":
        netarea = body["netarea"] if body.has_key("netarea") else ""
        sql = "select group_name from h_asset_group where parent_group_id = %s and group_name = %s and state='1' "
        args = (pid, name, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            tmpname = result["group_name"]
        except Exception as e:
            print 'fail : %s\n' % str(e)
            tmpname = ""
        if tmpname:
            return json.dumps({"message": u"资产组名称已存在"})
        # 获取父级网络域IP数值信息
        ipvaluelist = []
        sql = "select start_ip_value, end_ip_value from h_network_domain_range where group_id = %s"
        args = (pid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchall()
            for eachresult in result:
                start_ip_value = eachresult["start_ip_value"]
                end_ip_value = eachresult["end_ip_value"]
                ipvaluelist.append([start_ip_value, end_ip_value])
        except Exception as e:
            print 'fail : %s\n' % str(e)
        sql = "insert into h_asset_group(group_id, type, sub_type, area_type, group_name, parent_group_id, manage_person, remark, create_person, create_contact, manage_contact, state) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        args = (gid, type, sub_type, area_type, name, pid, mperson_name, comment, cperson_name, cperson_tel, mperson_tel, "1", )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"插入数据库失败"})
        if type == "1":
            sql = "insert into h_node_group_relation (group_id, node_id) values(%s, %s)"
            args = (gid, oid, )
            try:
                g.cursor.execute(sql, args)
            except:
                return json.dumps({"message": u"插入数据库失败"})
        parentipValue = []
        sql = "insert into h_network_domain_range(group_id, conf_type, ip_addr, subnet_mask, start_ip_addr, end_ip_addr, start_ip_value, end_ip_value, ip_group, ip_group_value) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for eachinfo in netarea:
            iptype = eachinfo["type"]
            ipaddr = None
            netmask = None
            startip = None
            endip = None
            start_ip_value = None
            end_ip_value = None
            ip_group = None
            ip_group_value = None
            if iptype == "1":
                ipaddr = eachinfo["ipaddr"]
                netmask = eachinfo["netmask"]
                ip = IpInfo(ipaddr, netmask)
                try:
                    start_ip_value, end_ip_value = ip.calIpvalue()
                    parentipValue.append([start_ip_value, end_ip_value])
                except:
                    return json.dumps({"message": u"IP、掩码格式有误"})
            if iptype == "2":
                startip = eachinfo["startip"]
                endip = eachinfo["endip"]
                try:
                    start_ip_value = IP(startip).int()
                    end_ip_value = IP(endip).int()
                    parentipValue.append([start_ip_value, end_ip_value])
                except:
                    return json.dumps({"message": u"IP网络域格式有误"})
            if iptype == "3":
                ip_group = eachinfo["ip"]
                try:
                    ip_group_value = IP(ip_group).int()
                except:
                    return json.dumps({"message": u"IP网络域格式有误"})
            args = (gid, iptype, ipaddr, netmask, startip, endip, start_ip_value, end_ip_value, ip_group, ip_group_value, )
            try:
                g.cursor.execute(sql, args)
            except Exception as e:
                print (e)
                return json.dumps({"message": u"插入数据库失败"})
        print ipvaluelist
        # 判断是否属于父级网络域中
        if len(ipvaluelist) != 0:
            for eachchild in parentipValue:
                flag = False
                start_child = eachchild[0]
                end_child = eachchild[1]
                for eachparent in ipvaluelist:
                    start_parent = eachparent[0]
                    end_parent = eachparent[1]
                    if start_child >= start_parent and end_child <= end_parent:
                        flag = True
                        break
                if not flag:
                    return json.dumps({"message": u"当前网络域不在父网络域中"})
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "新建",
        "remark": "创建组"
    })

    return json.dumps({"message": "ok"})


# 删除组织 /asset/group/delete
@app.route('/asset/group/delete', methods=['POST'])
@auth.permission("asset")
def group_delete(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    tree = body["tree"] if body.has_key("tree") else ""
    gid = body["gid"] if body.has_key("gid") else None

    # 删除设备组
    if tree == "1":
        select_sql = "select count(asset_id) from h_hardware_asset_info where group_id = %s and delete_state='1' "
        select_args = (gid,)
        try:
            g.cursor.execute(select_sql, select_args)
            result = g.cursor.fetchone()
            count = result["count"]
        except:
            count = 0
        sql = "select parent_group_id, type from h_asset_group where group_id=%s"
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            pgroup_id = result["parent_group_id"]
            type = result["type"]
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"查询数据库失败"})
        # 资产 更新组为父级组
        sql = "update h_hardware_asset_info set group_id=%s where group_id=%s"
        args = (pgroup_id, gid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"更新数据库失败"})
        sql = "select group_id from h_asset_group where parent_group_id=%s"
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchall()
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"查询数据库失败"})
        for eachresult in result:
            cgroup_id = eachresult["group_id"]
            # 组 更新组为父级组
            sql = "update h_asset_group set parent_group_id=%s where group_id=%s"
            args = (pgroup_id, cgroup_id, )
            try:
                g.cursor.execute(sql, args)
            except Exception as e:
                print 'fail : %s\n' % str(e)
                return json.dumps({"message": u"更新数据库失败"})
        # 删除组IP信息
        sql = "delete from h_network_domain_range where group_id=%s"
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"删除数据库失败"})
        # 删除组信息
        sql = "update h_asset_group set state = '0' where group_id=%s"
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"删除数据库失败"})
        # 删除IP组信息
        sql = "delete from h_asset_ip_info where group_id=%s"
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"删除数据库失败"})
        if type == "1":
            # 资产组为域，下面绑定了资产，则无法删除
            print count
            if count > 0:
                return json.dumps({"message": u"当前域已绑定资产，无法删除"})
            # 删除组织结构-网络结构中间表数据
            sql = "delete from h_node_group_relation where group_id=%s"
            args = (gid,)
            try:
                g.cursor.execute(sql, args)
            except:
                return json.dumps({"message": u"删除数据库失败"})
    g.conn.commit()

    return json.dumps({"message": "ok"})


# 获取资产类型 /asset/type/main
@app.route('/asset/type/main', methods=['POST'])
@auth.permission("asset")
def asset_gettype(_currUser):
    sql = "select type_id as id, parent_type_id as pid, type_name, case when type_id<=2 then '0'  when type_id>2 and type_id<=7 then '1' else '2' end as flag from h_asset_type order by type_id"
    args = ()
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchall()
        g.conn.commit()
        data.insert(0, {"id": "0", "pid": "", "type_name": u"资产类型", "flag": "0"})
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})

    return json.dumps({"message": "ok", "data": data})


# 新增资产类型 /asset/type/new
@app.route('/asset/type/new', methods=['POST'])
@auth.permission("asset")
def asset_newtype(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    pid = body["pid"] if body.has_key("pid") else None
    type_name = body["type_name"] if body.has_key("type_name") else ""
    comment = body["comment"] if body.has_key("comment") else ""

    sql = "select type_name from h_asset_type where parent_type_id = %s and type_name = %s"
    args = (pid, type_name, )
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchone()
        tmp_type_name = data["type_name"]
    except :
        tmp_type_name = ""
    if tmp_type_name:
        return json.dumps({"message": u"资产类型名称已存在"})
    sql = "insert into h_asset_type (parent_type_id, type_name, remark) values (%s, %s, %s)"
    args = (int(pid), type_name, comment, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"插入数据库失败"})
    g.conn.commit()

    return json.dumps({"message": "ok"})


# 删除资产类型 /asset/type/delete
@app.route('/asset/type/delete', methods=['POST'])
@auth.permission("asset")
def asset_deletetype(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    id = body["id"] if body.has_key("id") else ""
    sql = "select parent_type_id as pid from h_asset_type where type_id=%s"
    args = (id,)
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        pid = str(result["pid"])
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    # 资产 更新类型为父级类型
    sql = "update h_hardware_asset_info set asset_use_type=%s where asset_use_type=%s"
    args = (pid, id,)
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"更新数据库失败"})
    sql = "select type_id as id from h_asset_type where parent_type_id=%s"
    args = (id, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    for eachresult in result:
        eachid = eachresult["id"]
        # 父级类型 更新
        sql = "update h_asset_type set parent_type_id=%s where type_id=%s"
        args = (int(pid), int(eachid),)
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"更新数据库失败"})

    sql = "delete from h_asset_type where type_id=%s"
    args = (int(id), )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"删除数据库失败"})
    g.conn.commit()

    return json.dumps({"message": "ok"})


# 编辑前获取资产类型信息 /asset/type/getinfo
@app.route('/asset/type/getinfo', methods=['POST'])
@auth.permission("asset")
def asset_typegetinfo(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    id = body["id"] if body.has_key("id") else ""
    sql = "select type_name, remark  from h_asset_type where type_id=%s"
    args = (int(id), )
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchone()
        g.conn.commit()
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})

    return json.dumps({"message": "ok", "data": data})


# 编辑资产类型信息 /asset/type/edit
@app.route('/asset/type/edit', methods=['POST'])
@auth.permission("asset")
def asset_typeedit(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    id = body["id"] if body.has_key("id") else ""
    pid = body["pid"] if body.has_key("pid") else None
    type_name = body["type_name"] if body.has_key("type_name") else ""
    comment = body["comment"] if body.has_key("comment") else ""
    sql = "select type_name from h_asset_type where parent_type_id = %s and type_name = %s and type_id <> %s"
    args = (pid, type_name, id, )
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchone()
        tmp_type_name = data["type_name"]
    except :
        tmp_type_name = ""
    if tmp_type_name:
        return json.dumps({"message": u"资产类型名称已存在"})
    sql = "update h_asset_type set type_name=%s, remark=%s where type_id=%s "
    args = (type_name, comment, id, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"更新数据库失败"})
    g.conn.commit()

    return json.dumps({"message": "ok"})



# 资产列表 /asset/main
@app.route('/asset/main', methods=['POST'])
@auth.permission("asset")
def asset_main(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
        print uid
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    type = body["type"] if body.has_key("type") else None
    level = body["level"] if body.has_key("level") else ""
    classify = body["classify"] if body.has_key("classify") else ""
    warn = body["warn"] if body.has_key("warn") else ""
    keywords = body["keywords"].strip() if body.has_key("keywords") else ""
    reg_stime = body["reg_stime"] if body.has_key("reg_stime") else ""
    reg_etime = body["reg_etime"] if body.has_key("reg_etime") else ""
    limit = body['limit'] if body.has_key("limit") else 0
    offset = body['offset'] if body.has_key("offset") else 0
    order = body['order'] if body.has_key("order") else ""
    orderby = body['orderby'] if body.has_key("orderby") else ""
    gid = body["gid"] if body.has_key("gid") else None
    groups = [gid]
    tree = body["tree"] if body.has_key("tree") else ""
    select_sql = '''
                        select distinct
                            info.asset_id,
                            info.asset_name,
                            case when info.asset_id in ( select unnest ( array [ ( select array_agg ( asset_id ) from h_person_asset where person_id = %s ) ] ) ) then '1' else '0' end as att,
                            info.asset_classify,
                            info.type_id,
                            info.asset_level,
                            asset_label,
                            info.group_id,
                            info.node_id,
    '''

    # 查看全部资产
    if classify == '0':
        select_sql += '''
                        to_char(case when info.register_state = '1' and info.retire_state != '1' then info.register_time  when info.retire_state = '1' then info.retire_time else info.create_time end, 'yyyy-mm-dd hh24:mi:ss' ) as time,
                        case when info.register_state = '1' and info.retire_state != '1' then '1' when info.retire_state = '1' then '2' else '0' end as classify 
        '''
    # 查看已登记资产
    elif classify == '1':
        select_sql += '''
                        to_char(info.register_time, 'yyyy-mm-dd hh24:mi:ss' ) as time,
                        case when info.register_state = '1' and info.retire_state != '1' then '1' when info.retire_state = '1' then '2' else '0' end as classify 
        '''
    # 查看未登记资产
    else:
        select_sql += '''
                         to_char(info.create_time, 'yyyy-mm-dd hh24:mi:ss' ) as time,
                         '0' as classify 
        '''

    # 组织结构树
    if tree == "0":
        gids = organ_tree(gid, groups)
        total_sql = " select count(distinct info.asset_id) from h_hardware_asset_info info left join h_asset_ip_info  ipinfo on info.asset_id = ipinfo.asset_id left join h_person_asset passet on info.asset_id = passet.asset_id left join sys_node_info sinfo on sinfo.node_id = info.node_id left join h_asset_group dgroup on dgroup.group_id = info.group_id where  info.delete_state = '1' "
        list_sql = " from h_hardware_asset_info info left join h_asset_ip_info  ipinfo on info.asset_id = ipinfo.asset_id left join sys_node_info sinfo on sinfo.node_id = info.node_id left join h_asset_group dgroup on dgroup.group_id = info.group_id where info.delete_state = '1' "
        list_sql = select_sql + list_sql
        if not gid == "unregister":
            total_sql += " and info.node_id in ( select unnest ( array [%s] ) ) "
            list_sql += " and info.node_id in ( select unnest ( array [%s] ) ) "
            list_args = [uid, gids]
            total_args = [gids]
        else:
            total_sql += " and info.node_id is null "
            list_sql += " and info.node_id is null "
            list_args = [uid]
            total_args = []
    # 资产组树
    else:
        gids = assetgroup_tree(gid, groups)
        total_sql = " select count(distinct info.asset_id) from h_hardware_asset_info info left join h_asset_ip_info  ipinfo on info.asset_id = ipinfo.asset_id  left join h_person_asset passet on info.asset_id = passet.asset_id left join sys_node_info sinfo on sinfo.node_id = info.node_id left join h_asset_group dgroup on dgroup.group_id = info.group_id where  info.delete_state = '1' "
        list_sql = " from h_hardware_asset_info info left join h_asset_ip_info ipinfo on info.asset_id = ipinfo.asset_id left join sys_node_info sinfo on sinfo.node_id = info.node_id left join h_asset_group dgroup on dgroup.group_id = info.group_id where info.delete_state = '1'  "
        list_sql = select_sql + list_sql
        if not gid == "unregister":
            total_sql += " and info.group_id in ( select unnest ( array [%s] ) ) "
            list_sql += " and info.group_id in ( select unnest ( array [%s] ) ) "
            list_args = [uid, gids]
            total_args = [gids]
        else:
            total_sql += " and info.group_id is null "
            list_sql += " and info.group_id is null "
            list_args = [uid]
            total_args = []

    # 查看全部资产
    if classify == '0':
        pass
    # 查看已登记资产
    elif classify == '1':
        total_sql += "  and info.register_state = '1' and info.retire_state = '0' "
        list_sql += " and info.register_state = '1' and info.retire_state = '0' "
    # 查看未登记资产
    else:
        total_sql += "  and info.register_state = '0' "
        list_sql += "  and info.register_state = '0' "
    if "type" in body and body['type'] != "":
        groups = [type]
        types = map(int, get_type(type, groups))
        total_sql += " and info.type_id in ( select unnest ( array [%s] ) ) "
        list_sql += " and info.type_id in ( select unnest ( array [%s] ) ) "
        total_args.append(types)
        list_args.append(types)
    if "level" in body and body['level'] != "":
        total_sql += " and info.asset_level = %s "
        list_sql += " and info.asset_level = %s "
        total_args.append(level)
        list_args.append(level)
    # 已关注
    if "attention" in body and body['attention'] == "1":
        sql = "select asset_id from h_person_asset where person_id=%s"
        args = (uid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchall()
            uuidList = []
            for eachresult in result:
                uuid = eachresult["asset_id"]
                uuidList.append(uuid)
        except:
            return json.dumps({"message": u"查询数据库失败"})
        if uuidList:
            total_sql += ' and info.asset_id IN ( SELECT unnest ( array [%s] ) ) '
            list_sql += ' and info.asset_id IN ( SELECT unnest ( array [%s] ) ) '
            total_args.append(uuidList)
            list_args.append(uuidList)
        else:
            return json.dumps({"message": "ok", "data": [], 'total': 0})
    # 未关注
    if "attention" in body and body['attention'] == "2":
        sql = "select asset_id from h_person_asset where person_id=%s"
        args = (uid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchall()
            uuidList = []
            for eachresult in result:
                uuid = eachresult["asset_id"]
                uuidList.append(uuid)
        except:
            return json.dumps({"message": u"查询数据库失败"})
        if uuidList:
            total_sql += ' and info.asset_id NOT IN ( SELECT unnest ( array [%s] ) ) '
            list_sql += ' and info.asset_id NOT IN ( SELECT unnest ( array [%s] ) ) '
            total_args.append(uuidList)
            list_args.append(uuidList)
    if warn == '1':
        total_sql += " and info.warn_flag = '1' "
        list_sql += " and info.warn_flag = '1' "
    if keywords:
        sql = "select label_id from k_asset_label where label_name like %s limit 1"
        args = ('%%%s%%' % keywords,)
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            asset_label = result["label_id"]
            total_args.append('%%' + asset_label + '%%')
            list_args.append('%%' + asset_label + '%%')
        except:
            total_args.append(None)
            list_args.append(None)
        total_sql += ' and ( info.asset_label like %s or info.asset_name like %s or ipinfo.ip_addr like %s  or dgroup.group_name like %s or sinfo.node_name like %s ) '
        list_sql += ' and ( info.asset_label like %s or info.asset_name like %s or ipinfo.ip_addr like %s or dgroup.group_name like %s or sinfo.node_name like %s ) '
        for i in range(1, 5):
            total_args.append('%%' + keywords + '%%')
            list_args.append('%%' + keywords + '%%')
    if reg_stime and reg_etime:
        if classify == "1":
            total_sql += 'and info.register_time >= %s and info.register_time <= %s '
            list_sql += 'and info.register_time >= %s and info.register_time <= %s '
            total_args += [reg_stime, reg_etime + ' 23:59:59']
            list_args += [reg_stime, reg_etime + ' 23:59:59']
        if classify == "2":
            total_sql += 'and info.create_time >= %s and info.create_time <= %s '
            list_sql += 'and info.create_time >= %s and info.create_time <= %s '
            total_args += [reg_stime, reg_etime + ' 23:59:59']
            list_args += [reg_stime, reg_etime + ' 23:59:59']
    if order and orderby:
        if order == "register_time":
            order = "time"
        if order == "r_time":
            order = "time"
        list_sql += " order by %s %s " % (order, orderby)
    else:
        list_sql += " order by att desc, asset_level asc, time desc "
    list_sql += " limit %s offset %s "
    list_args.extend([limit, offset])
    try:
        g.cursor.execute(total_sql, total_args)
        result = g.cursor.fetchone()
        total = result['count']
    except Exception as e:
        print str(e)
        return json.dumps({"message": u"查询数据库失败"})
    try:
        g.cursor.execute(list_sql, list_args)
        result = g.cursor.fetchall()
        data = []
        # 查看全部资产
        if classify == '0':
            for eachresult in result:
                asset_id = eachresult["asset_id"]
                asset_name = eachresult["asset_name"]
                att = eachresult["att"]
                asset_classify = eachresult["asset_classify"]
                type_id = eachresult["type_id"]
                asset_level = eachresult["asset_level"]
                group_id = eachresult["group_id"]
                node_id = eachresult["node_id"]
                label = eachresult["asset_label"]
                classify = eachresult["classify"]
                time = eachresult["time"]
                # 查询ip信息
                sql = "select array_agg(ip_addr) as ip_addr from h_asset_ip_info where asset_id = %s"
                args = (asset_id, )
                try:
                    g.cursor.execute(sql, args)
                    result = g.cursor.fetchone()
                    ip_addr = result["ip_addr"]
                except Exception as e:
                    print str(e)
                    ip_addr = []
                # 查询标签信息
                if not label:
                    label = []
                else:
                    sql = "select array_agg(label_name) as label_name from k_asset_label where label_id  in ( select unnest ( array [%s] ) ) "
                    args = (label.split(','), )
                    try:
                        g.cursor.execute(sql, args)
                        result = g.cursor.fetchone()
                        label = result["label_name"]
                    except Exception as e:
                        print str(e)
                        label = []
                data.append([asset_id, asset_name, att, asset_classify, type_id, asset_level, ip_addr, label, node_id, group_id, time, classify])
        # 查看已登记资产
        elif classify == '1':
            for eachresult in result:
                asset_id = eachresult["asset_id"]
                asset_name = eachresult["asset_name"]
                att = eachresult["att"]
                asset_classify = eachresult["asset_classify"]
                type_id = eachresult["type_id"]
                asset_level = eachresult["asset_level"]
                group_id = eachresult["group_id"]
                node_id = eachresult["node_id"]
                label = eachresult["asset_label"]
                classify = eachresult["classify"]
                time = eachresult["time"]
                # 查询ip信息
                sql = "select array_agg(ip_addr) as ip_addr from h_asset_ip_info where asset_id = %s"
                args = (asset_id, )
                try:
                    g.cursor.execute(sql, args)
                    result = g.cursor.fetchone()
                    ip_addr = result["ip_addr"]
                except:
                    ip_addr = []
                # 查询标签信息
                if not label:
                    label = []
                else:
                    sql = "select array_agg(label_name) as label_name from k_asset_label where label_id  in ( select unnest ( array [%s] ) ) "
                    args = (label.split(','), )
                    try:
                        g.cursor.execute(sql, args)
                        result = g.cursor.fetchone()
                        label = result["label_name"]
                    except Exception as e:
                        print str(e)
                        label = []
                data.append([asset_id, asset_name, att, asset_classify, type_id, asset_level, ip_addr, label, node_id, group_id, time, classify])
        # 查看未登记资产
        else:
            for eachresult in result:
                asset_id = eachresult["asset_id"]
                asset_name = eachresult["asset_name"]
                att = eachresult["att"]
                asset_classify = eachresult["asset_classify"]
                type_id = eachresult["type_id"]
                group_id = eachresult["group_id"]
                node_id = eachresult["node_id"]
                classify = eachresult["classify"]
                time = eachresult["time"]
                # 查询ip信息
                sql = "select array_agg(ip_addr) as ip_addr from h_asset_ip_info where asset_id = %s"
                args = (asset_id, )
                try:
                    g.cursor.execute(sql, args)
                    result = g.cursor.fetchone()
                    ip_addr = result["ip_addr"]
                except:
                    ip_addr = []
                data.append([asset_id, asset_name, att, asset_classify, type_id, ip_addr, node_id, group_id, time, classify])
    except:
        #print str(e)
        return json.dumps({"message": u"查询数据库失败"})
    if tree == "0":
        sql = "select count(asset_id) from h_hardware_asset_info  where  delete_state='1' "
        if not "unregister" in gids:
            sql += " and node_id in ( select unnest ( array [%s] ) ) "
            args = (gids, )
        else:
            sql += " and node_id is null "
            args = ()

    else:
        sql = "select count(asset_id) from h_hardware_asset_info  where delete_state='1' "
        if not "unregister" in gids:
            sql += " and group_id in ( select unnest ( array [%s] ) ) "
            args = (gids, )
        else:
            sql += " and group_id is null "
            args = ()
    count = {}
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        count["total"] = result["count"]
    except:
        return json.dumps({"message": u"查询数据库失败"})
    sql1 = sql + " and register_state = '1' and retire_state != '1' "
    try:
        g.cursor.execute(sql1, args)
        result = g.cursor.fetchone()
        count["reg"] = result["count"]
    except:
        return json.dumps({"message": u"查询数据库失败"})
    sql2 = sql + " and register_state = '0' "
    try:
        g.cursor.execute(sql2, args)
        result = g.cursor.fetchone()
        count["unreg"] = result["count"]
    except:
        return json.dumps({"message": u"查询数据库失败"})
    g.conn.commit()

    return json.dumps({"message": "ok", "data": data, "total": total, "count": count})


def  getNodeName(node_id, idgroups, namegroups):
    try:
        sql = " select parent_node_id, node_name from sys_node_info where node_id=%s "
        args = (node_id, )
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        g.conn.commit()
        id = result["parent_node_id"]
        name = result["node_name"]
        if id == None or id == '0':
            namegroups.insert(0, name)
            return [idgroups, namegroups]
        idgroups.append(id)
        namegroups.insert(0, name)
        idgroups, namegroups = getNodeName(id, idgroups, namegroups)
    except:
        return [None, None]
    return [idgroups, namegroups]


def  getGroupName(group_id, idgroups, namegroups):
    try:
        sql = " select parent_group_id, group_name from h_asset_group where group_id=%s "
        args = (group_id, )
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        g.conn.commit()
        id = result["parent_group_id"]
        name = result["group_name"]
        if id == None or id == '0':
            namegroups.insert(0, name)
            return [idgroups, namegroups]
        idgroups.append(id)
        namegroups.insert(0, name)
        idgroups, namegroups = getGroupName(id, idgroups, namegroups)
    except:
        return [None, None]
    return [idgroups, namegroups]


# 资产详情 /asset/detail
@app.route('/asset/detail', methods=['POST'])
@auth.permission("asset")
def asset_detail(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
        print uid
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else ""
    classify = body["classify"] if body.has_key("classify") else ""
    sql = "select group_id, node_id from h_hardware_asset_info where asset_id = %s"
    args = (uuid, )
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchone()
        group_id = data["group_id"]
        node_id = data["node_id"]
    except:
        group_id = None
        node_id = None
    idgroups = [node_id]
    namegroups = []
    node_ids, node_names =  getNodeName(node_id, idgroups, namegroups)
    try:
        node_name = '-'.join(node_names)
    except:
        node_name = None
    idgroups = [group_id]
    namegroups = []
    group_ids, group_names =  getGroupName(group_id, idgroups, namegroups)
    try:
        group_name = '-'.join(group_names)
    except:
        group_name = None
    # 未登记资产详情
    if classify == "0":
        sql = '''
                select
                	asset_id, asset_name, type_id, model, asset_level, asset_classify, host_ip, create_person as cperson_name, create_contact as cperson_tel,
                	( select array_agg ( ipinfo.ip_addr ) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id ) as ip_addr,
                    ( select array_agg ( ipinfo.mac_addr ) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id ) as mac_addr,
                	( select  array_agg ( label_name ) from k_asset_label label where ainfo.asset_label ~ label.label_id ) as label,
                	position, manage_person as mperson_name, manage_contact as mperson_tel, to_char ( ainfo.create_time, 'yyyy-mm-dd hh24:mi:ss' ) as create_time,
                case when array_to_string ( ( select array_agg ( passet.asset_id ) from h_person_asset passet where passet.person_id = %s ), ',' ) ~ ainfo.asset_id then '1' else '0' end as att,
                '0' as classify   
                from
                	h_hardware_asset_info ainfo
                where ainfo.asset_id = %s 
        '''
        args = (uid, uuid, )
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
            data["group_name"] = group_name
            data["node_name"] = node_name
            return json.dumps({"message": "ok", "data": data})
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"查询数据库失败"})
    # 登记资产详情
    if classify == "1":
        sql = '''
                select
                	asset_id, asset_name, type_id, model, group_id, asset_level, asset_classify, node_id, host_ip, create_person as cperson_name, create_contact as cperson_tel,
                	( select array_agg ( ipinfo.ip_addr ) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id ) as ip_addr,
                    ( select array_agg ( ipinfo.mac_addr ) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id ) as mac_addr,
                	( select  array_agg ( label_name ) from k_asset_label label where ainfo.asset_label ~ label.label_id ) as label,
                	position, manage_person as mperson_name, manage_contact as mperson_tel, to_char ( ainfo.register_time, 'yyyy-mm-dd hh24:mi:ss' ) as register_time,
                case when array_to_string ( ( select array_agg ( passet.asset_id ) from h_person_asset passet where passet.person_id = %s ), ',' ) ~ ainfo.asset_id then '1' else '0' end as att,
                '1' as classify, to_char ( ainfo.update_time, 'yyyy-mm-dd hh24:mi:ss' ) as update_time    
                from
                	h_hardware_asset_info ainfo
                where ainfo.asset_id = %s 
        '''
        args = (uid, uuid, )
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"查询数据库失败"})
    # 退役资产详情
    else:
        sql = '''
                select
                	asset_id, asset_name, type_id, model, group_id, asset_level, asset_classify, node_id, host_ip, create_person as cperson_name, create_contact as cperson_tel,
                	( select array_agg ( ipinfo.ip_addr ) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id ) as ip_addr,
                    ( select array_agg ( ipinfo.mac_addr ) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id ) as mac_addr,
                	( select  array_agg ( label_name ) from k_asset_label label where ainfo.asset_label ~ label.label_id ) as label,
                	position, manage_person as mperson_name, manage_contact as mperson_tel, to_char ( ainfo.register_time, 'yyyy-mm-dd hh24:mi:ss' ) as register_time,
                case when array_to_string ( ( select array_agg ( passet.asset_id ) from h_person_asset passet where passet.person_id = %s ), ',' ) ~ ainfo.asset_id then '1' else '0' end as att,
                '2' as classify, to_char ( ainfo.update_time, 'yyyy-mm-dd hh24:mi:ss' ) as update_time, to_char ( ainfo.retire_time, 'yyyy-mm-dd hh24:mi:ss' ) as retire_time     
                from
                	h_hardware_asset_info ainfo
                where ainfo.asset_id = %s 
        '''
        args = (uid, uuid, )
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"查询数据库失败"})
    data["group_name"] = group_name
    data["node_name"] = node_name

    # 获取软件资产数据
    sql = "select software_name, install_addr, software_version, case when state = '0' then '已安装' else '未安装' end as install_flag, to_char ( install_time, 'yyyy-mm-dd hh24:mi:ss' ) as install_time from h_software_info where asset_id = %s"
    args = (uuid, )
    try:
        g.cursor.execute(sql, args)
        softwareList = g.cursor.fetchall()
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    data['software'] = softwareList

    # # 获取服务资产数据
    # sql = "select service_name, service_path, case when service_state = '1' then '停止'  when service_state = '4' then '正在运行' else null end as status, case when service_method = '2' then '自动' when service_method = '3'  then '手动' when service_method = '4' then '禁用' else null end as method from h_asset_service where asset_id = %s"
    # args = (uuid, )
    # try:
    #     g.cursor.execute(sql, args)
    #     serviceList = g.cursor.fetchall()
    # except:
    #     return json.dumps({"message": u"查询数据库失败"})
    # data['service'] = serviceList

    # # 获取硬件资产数据
    # sql = "select cpu_info, cpu_core, mem_info, disk_info, net_info, video_info from h_hardware_info where asset_id = %s"
    # args = (uuid, )
    # try:
    #     hardwareDict = {}
    #     g.cursor.execute(sql, args)
    #     result = g.cursor.fetchone()
    # except  Exception as e:
    #     print str(e)
    #     return json.dumps({"message": u"查询数据库失败"})
    # try:
    #     cpu_info = result["cpu_info"]
    #     cpu_core = result["cpu_core"]
    #     mem_info = result["mem_info"]
    #     disk_info = result["disk_info"]
    #     net_info = result["net_info"]
    #     video_info = result["video_info"]
    # except:
    #     data['hardware'] = hardwareDict
    #     return json.dumps({"message": "ok", "data": data})
    #
    # # 获取CPU相关信息
    # try:
    #     cpu_name = cpu_info.split(' ')[0]
    # except:
    #     cpu_name = None
    # try:
    #     cpu_version = cpu_info.split(')')[-1].split('@')[0].replace('CPU', '').strip()
    # except:
    #     cpu_version = None
    # try:
    #     basic_frequency = cpu_info.split('@')[-1].strip()
    # except:
    #     basic_frequency = None
    # hardwareDict["cpu"] = [{"manufacturer": cpu_name, "cpu_version": cpu_version, "basic_frequency": basic_frequency, "cpu_count": str(cpu_core) + '线程'}]
    #
    # # 获取内存相关信息
    # hardwareDict["mem"] = []
    # mem_data = json.loads(mem_info.replace("'", '"').replace(': u', ": ").replace("True", '"True"').replace("False",'"False"'))
    # for eachmem in mem_data:
    #     manufacturer = eachmem['Manufacturer'].strip()
    #     serialnumber = eachmem['SerialNumber'].strip()
    #     model = eachmem['PartNumber'].strip()
    #     size = str(int(eachmem['Capacity'])/1024/1024/1024) + 'GB'
    #     hardwareDict["mem"].append({'manufacturer': manufacturer, 'serialnumber': serialnumber, 'model': model, 'size': size})
    #
    # # 获取硬盘相关信息
    # hardwareDict["disk"] = []
    # disk_data = json.loads(disk_info.replace("'", '"').replace(': u', ": ").replace("True", '"True"').replace("False",'"False"'))
    # for eachdisk in disk_data:
    #     manufacturer = eachdisk['Caption'].strip().split(' ')[0]
    #     try:
    #         model = eachdisk['Caption'].strip().split(' ')[1]
    #     except:
    #         model = None
    #     serialnumber = eachdisk['FirmwareRevision']
    #     size = str(int(eachdisk['Size'])/1024/1024/1024) + 'GB'
    #     hardwareDict["disk"].append({'manufacturer': manufacturer, 'serialnumber': serialnumber, 'model': model, 'size': size})
    #
    # # 获取网卡相关信息
    # hardwareDict["net"] = []
    # net_data = json.loads(net_info.replace("'", '"').replace(': u', ": ").replace("True", '"True"').replace("False",'"False"'))
    # for eachnet in net_data:
    #     manufacturer = eachnet['Manufacturer'].strip()
    #     model = eachnet['Name'].strip()
    #     mac = eachnet['MACAddress'].strip()
    #     hardwareDict["net"].append({'manufacturer': manufacturer, 'model': model, 'mac': mac, 'ip': None})
    #
    # # 获取显卡相关信息
    # hardwareDict["video"] = []
    # video_data = json.loads(video_info.replace(": u", ': ').replace("'", '"').replace('RAM": ', 'RAM": "').replace('L}','"}'))
    # for eachvideo in video_data:
    #     manufacturer = eachvideo['Caption'].strip().split(' ')[0]
    #     model = eachvideo['Caption'].replace(manufacturer, '').strip()
    #     driver_version = eachvideo['DriverVersion'].strip()
    #     size = str(int(eachvideo['AdapterRAM']) / 1024 / 1024 / 1024) + 'GB'
    #     hardwareDict["video"].append({'manufacturer': manufacturer, 'model': model, 'driver_version': driver_version, 'size': size})
    #
    # data['hardware'] = hardwareDict
    g.conn.commit()

    return json.dumps({"message": "ok", "data": data})


# 编辑前获取资产信息 /asset/getinfo
@app.route('/asset/getinfo', methods=['POST'])
@auth.permission("asset")
def asset_getinfo(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else ""
    # 查询资产基础信息
    sql = "select ainfo.asset_id, asset_name, model, asset_level, policy, type_id, group_id, host_ip, node_id, position, ainfo.create_person as cperson_name, ainfo.create_contact as cperson_tel, ainfo.manage_person as mperson_name, ainfo.manage_contact as mperson_tel, string_to_array(asset_label, ',') as label, asset_classify as special from h_hardware_asset_info ainfo  where  ainfo.asset_id = %s"
    args = (uuid, )
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchone()
    except Exception as e:
        print str(e)
        return json.dumps({"message": u"查询数据库失败"})
    # 查询资产网络信息
    sql = "select  ip_addr, mac_addr, group_id as group_addr  from h_asset_ip_info where asset_id = %s  order by r_time asc"
    args = (uuid,)
    ip_addr = []
    mac_addr = []
    group_addr = []
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        if result:
            for eachdata in result:
                ip_addr.append(eachdata["ip_addr"])
                mac_addr.append(eachdata["mac_addr"])
                group_addr.append(eachdata["group_addr"])
    except Exception as e:
        print str(e)
        return json.dumps({"message": u"查询数据库失败"})
    g.conn.commit()
    data["ip_addr"] = ip_addr
    data["mac_addr"] = mac_addr
    data["group_addr"] = group_addr
    return json.dumps({"message": "ok", "data": data})


# 编辑资产 /asset/update
@app.route('/asset/update', methods=['POST'])
@auth.permission("asset")
def asset_update(_currUser):
    manul_user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (manul_user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        g.conn.commit()
        uid = result['person_id']
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else ""
    model = body["model"] if body.has_key("model") else ""
    level = body["level"] if body.has_key("level") else ""
    policy = body["basepolicy"] if body.has_key("basepolicy") else ""
    type = body["type"] if body.has_key("type") else None
    gid = body["gid"] if body.has_key("gid") else None
    child_group = body["child_group"] if body.has_key("child_group") else []
    ip = body["ip"] if body.has_key("ip") else []
    host_ip = body["host_ip"] if body.has_key("host_ip") else ""
    mac = body["mac"] if body.has_key("mac") else []
    if None in child_group:
        return json.dumps({"message": u"IP、MAC和网络结构数量不对应"})
    if len(ip) != len(mac) or len(mac) != len(child_group):
        return json.dumps({"message": u"IP、MAC和网络结构数量不对应"})
    special = body["special"] if body.has_key("special") else ""
    oid = body["oid"] if body.has_key("oid") else None
    position = body["position"] if body.has_key("position") else ""
    cperson_name = body["cperson_name"] if body.has_key("cperson_name") else ""  #  创建者
    mperson_name = body["mperson_name"] if body.has_key("mperson_name") else ""  #  管理者
    cperson_tel = body["cperson_tel"] if body.has_key("cperson_tel") else ""  #  创建者联系方式
    mperson_tel = body["mperson_tel"] if body.has_key("mperson_tel") else ""  #  管理者联系方式
    title = body["title"] if body.has_key("title") else []
    sql = "select ainfo.asset_id, ainfo.delete_state, model, asset_level, policy, type_id, agroup.group_name, (select array_agg(ipinfo.ip_addr) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id) as ip, host_ip, (select array_agg(ipinfo.mac_addr) from h_asset_ip_info ipinfo where ipinfo.asset_id = ainfo.asset_id) as mac, ninfo.node_name, position, ainfo.create_person, ainfo.create_contact, ainfo.manage_person, ainfo.manage_contact, asset_classify, ( select array_agg ( alabel.label_name ) from k_asset_label alabel where ainfo.asset_label ~ alabel.label_id ) as label from h_hardware_asset_info ainfo left join h_asset_group agroup on ainfo.group_id = agroup.group_id left join sys_node_info ninfo on ainfo.node_id = ninfo.node_id  where ainfo.asset_id = %s"
    args = (uuid, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        state = result["delete_state"] if result["delete_state"] else u"空"
        if state == '0':
            return json.dumps({"message": u"资产已删除"})
        old_model = result["model"] if result["model"] else u"空"
        old_level = result["asset_level"] if result["asset_level"] else u"空"
        old_policy = result["policy"] if result["policy"] else u"空"
        old_type = result["type_id"] if result["type_id"] else u"空"
        old_group_name = result["group_name"] if result["asset_level"] else u"空"
        old_ip = result["ip"] if result["ip"] else []
        old_host_ip = result["host_ip"] if result["host_ip"] else u"空"
        old_mac = result["mac"] if result["mac"] else []
        old_node_name = result["node_name"] if result["node_name"] else u"空"
        old_position = result["position"] if result["position"] else u"空"
        old_create_person = result["create_person"] if result["create_person"] else u"空"
        old_create_contact = result["create_contact"] if result["create_contact"] else u"空"
        old_manage_person = result["manage_person"] if result["manage_person"] else u"空"
        old_manage_contact = result["manage_contact"] if result["manage_contact"] else u"空"
        old_label = result["label"] if result["label"] else []
        old_classify = result["asset_classify"] if result["asset_classify"] else ""
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    # 查看所有资产类型
    sql  = "select type_id as id, type_name from h_asset_type "
    args = ()
    typeDict = {}
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        for eachresult in result:
            id = str(eachresult["id"])
            name = eachresult["type_name"]
            typeDict[id] = name
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    # 查看组织结构名称
    sql  = "select node_name from sys_node_info where node_id = %s "
    args = (oid, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        node_name = result["node_name"]
    except:
        node_name = ""
    # 查看资产组名称
    sql  = "select group_name from h_asset_group where group_id =%s "
    args = (gid, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        g.conn.commit()
        group_name = result["group_name"]
    except:
        group_name = ""
    # 查看标签名称
    sql  = "select array_agg ( label_name ) as label from k_asset_label where %s  ~ label_id "
    args = (','.join(title), )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        g.conn.commit()
        label = result["label"]
    except:
        label = []
    message = ''
    if not old_model == model:
        message += u'资产型号(由%s修改为%s), ' % (old_model, model)
    if not str(old_level) == level:
        message += u'资产等级(由%s修改为%s), ' % (old_level, level)
    if not old_policy == policy:
        message += u'基准策略(由%s修改为%s), ' % (old_policy, policy)
    try:
        typeDict[old_type]
    except:
        typeDict[old_type] = u"空"
    try:
        typeDict[type]
    except:
        typeDict[type] = u"空"
    if not typeDict[old_type] == typeDict[type]:
        message += u'资产类型(由%s修改为%s), ' % (typeDict[old_type], typeDict[type])
    if not old_group_name == group_name:
        message += u'所属分组(由%s修改为%s), ' % (old_group_name, group_name)
    if not old_node_name == node_name:
        message += u'所属部门(由%s修改为%s), ' % (old_node_name, node_name)
    if not old_position == position:
        message += u'部署位置(由%s修改为%s), ' % (old_position, position)
    if not old_create_person == cperson_name:
         message += u'创建人(由%s修改为%s), ' % (old_create_person, cperson_name)
    if not old_create_contact == cperson_tel:
         message += u'创建人联系方式(由%s修改为%s), ' % (old_create_contact, cperson_tel)
    if not old_manage_person == mperson_name:
        message += u'管理人(由%s修改为%s), ' % (old_manage_person, mperson_name)
    if not old_manage_contact == mperson_tel:
        message += u'管理人联系方式(由%s修改为%s), ' % (old_manage_contact, mperson_tel)
    if not old_classify == special:
        tmpdict = {"1": u"虚拟设备", "2": u"实体设备"}
        try:
            old_classify = tmpdict[old_classify]
        except:
            old_classify = None
        try:
            new_special = tmpdict[special]
        except:
            new_special = None
        message += u'资产分类(由%s修改为%s), ' % (old_classify, new_special)
    try:
        old_label.sort()
        label.sort()
    except:
        pass
    if not old_label == label:
        if not old_label:
            old_label = u"空"
        else:
            old_label = ','.join(old_label)
        if not label:
            label = u"空"
        else:
            label = ','.join(label)
        message += u'资产标签(由%s修改为%s), ' % (old_label, label)
    #print message
    sql = "update h_hardware_asset_info set model=%s, asset_level=%s, policy=%s, type_id=%s, group_id=%s, host_ip=%s, node_id=%s, position=%s, create_person=%s, create_contact=%s, manage_person=%s, manage_contact=%s, asset_label=%s, asset_classify=%s where asset_id=%s"
    args = (model, level, policy, type, gid, host_ip, oid, position, cperson_name, cperson_tel, mperson_name, mperson_tel, ','.join(title), special, uuid, )
    try:
        g.cursor.execute(sql, args)
        if message:
            asset_manul_log(uuid, uid, u'变更', message)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"更新数据库失败"})

    sql = "delete from h_asset_ip_info where asset_id=%s"
    args = (uuid, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"删除数据库失败"})

    sql = "insert into h_asset_ip_info (asset_id, ip_addr, mac_addr, group_id) values (%s, %s, %s, %s)"
    sql_select = "select  conf_type, start_ip_value,end_ip_value, ip_group_value, start_ip_addr, end_ip_addr, ip_addr, subnet_mask, ip_group from h_network_domain_range where group_id=%s "
    for index, eachip in enumerate(ip):
        eachmac = mac[index]
        try:
            eachcgroup = child_group[index]
        except:
            eachcgroup = None
        select_args = (eachcgroup, )
        try:
            g.cursor.execute(sql_select, select_args)
            result = g.cursor.fetchall()
            flag = False
            if  eachip:
                try:
                    ip = IP(eachip).int()
                except:
                    return json.dumps({"message": u"ip地址格式有误"})
            else:
                ip = None
            error_message = ""
            for eachresult in result:
                type = eachresult["conf_type"]
                ip1 = eachresult["start_ip_value"]
                ip2 = eachresult["end_ip_value"]
                ip3 = eachresult["ip_group_value"]
                start_ip_addr = eachresult["start_ip_addr"]
                end_ip_addr = eachresult["end_ip_addr"]
                ip_addr = eachresult["ip_addr"]
                subnet_mask = eachresult["subnet_mask"]
                ip_group = eachresult["ip_group"]
                if type == "1":
                    if ip >= ip1 and  ip <= ip2:
                        flag = True
                    else:
                        error_message += u'ip掩码:  %s、%s' % (ip_addr, subnet_mask)
                if type == "2":
                    if ip >= ip1 and ip <= ip2:
                        flag = True
                    else:
                        error_message += u'ip区间:  %s ~ %s' % (start_ip_addr, end_ip_addr)
                if type == "3":
                    if ip == ip3:
                        error_message += u'ip集:  %s' % ip_group
            select_sql = "select type from h_asset_group where group_id = %s"
            select_args = (eachcgroup, )
            try:
                g.cursor.execute(select_sql, select_args)
                result = g.cursor.fetchone()
                conf_type = result["type"]
            except:
                conf_type = None
            if conf_type == "2":
                if not flag:
                    return json.dumps({"message": u"ip:%s, 不在绑定的网络域中。 %s" % (eachip, error_message)})
        except Exception as e:
            print 'fail : %s\n' % str(e)
        args = (uuid, eachip, eachmac, eachcgroup, )
        try:
            g.cursor.execute(sql, args)
            g.conn.commit()
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"插入数据库失败"})


    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "资产变更"
    })

    return json.dumps({"message": "ok"})


# 删除资产 /asset/delete
@app.route('/asset/delete', methods=['POST'])
@auth.permission("asset")
def asset_delete(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else ""
    gid = body["gid"] if body.has_key("gid") else None

    ids = uuid
    sql = "delete from  h_software_info where asset_id IN ( SELECT unnest ( array [%s] ) )"
    args = (ids, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"删除数据库失败"})
    sql = "delete from  h_asset_ip_info where asset_id IN ( SELECT unnest ( array [%s] ) )"
    args = (ids, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"删除数据库失败"})
    sql = "update h_hardware_asset_info set delete_state='0' where asset_id IN ( SELECT unnest ( array [%s] ) )"
    args = (ids, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"更新数据库失败"})

    kafka_ip, kafka_port = readKafkaInI()
    for eachuuid in ids:
        try:
            object = confluent_kafka.Producer({"bootstrap.servers": "%s:%s" % (kafka_ip, kafka_port)})
            sendData = {"uuid": eachuuid, "type": 'delete'}
            print sendData
            object.produce("asset_status", json.dumps(sendData), "delete")
            object.flush()
        except Exception as e:
            print 'fail : %s\n' % str(e)
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "删除资产"
    })

    return json.dumps({"message": "ok"})


# 通过组织结构获取域列表 /asset/group/getarea
@app.route('/asset/group/getarea', methods=['POST'])
@auth.permission("asset")
def asset_group_getarea(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    gid = body["gid"] if body.has_key("gid") else ""
    uuid = body["uuid"] if body.has_key("uuid") else ""
    namegroups = []
    data = {}
    if uuid == "-1":
        sql = "select array_agg(array[group_id, group_name]) as group_id from h_asset_group where state = '1' and type = '1' "
        args = ()
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            id = result["group_id"]
        except Exception as e:
            id = []
    else:
        idgroups = [gid]
        ids, names = getNodeName(gid, idgroups, namegroups)
        sql = "select array_agg(array[agroup.group_id, agroup.group_name]) as group_id from h_asset_group agroup left join h_node_group_relation relation on agroup.group_id = relation.group_id where agroup.state = '1' and agroup.type = '1' and relation.node_id = %s "
        id = None
        if ids:
            for eachid in ids:
                args = (eachid, )
                try:
                    g.cursor.execute(sql, args)
                    result = g.cursor.fetchone()
                    id = result["group_id"]
                    if id:
                        break
                except Exception as e:
                    print str(e)
                    id = []
        if not id:
            return json.dumps({"message": u"所选组织机构不包含域"})
    data["group_ip"] = id
    g.conn.commit()

    return json.dumps({"message": "ok", "data": data})


# 通过域获取子域信息 /asset/group/getchildarea
@app.route('/asset/group/getchildarea', methods=['POST'])
@auth.permission("asset")
def asset_group_getchildarea(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    group_id = body["group_id"] if body.has_key("group_id") else ""
    sql = "select group_id, group_name, parent_group_id from h_asset_group where parent_group_id = %s and state = '1' "
    args = (group_id, )
    data = []
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        if not result:
            result = []
    except Exception as e:
        print 'fail : %s\n' % str(e)
        result = []
    for eachresult in result:
        id = eachresult["group_id"]
        name = eachresult["group_name"]
        pid = eachresult["parent_group_id"]
        data.append({"id": id, "pid": pid, "name": name})
    sql = "select group_name, parent_group_id from h_asset_group where group_id = %s"
    args = (group_id, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        pid = result["parent_group_id"]
        name = result["group_name"]
    except Exception as e:
        print 'fail : %s\n' % str(e)
        pid = None
        name = None
    data.insert(0, {"id": group_id, "pid": pid, "name": name})
    g.conn.commit()

    return json.dumps({"message": "ok", "data": data})


# 资产登记 /asset/reg
@app.route('/asset/reg', methods=['POST'])
@auth.permission("asset")
def asset_reg(_currUser):
    manul_user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (manul_user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else None
    source = body["source"] if body.has_key("source") else None
    model = body["model"] if body.has_key("model") else ""
    level = body["level"] if body.has_key("level") else ""
    basepolicy = body["basepolicy"] if body.has_key("basepolicy") else ""
    type = body["type"] if body.has_key("type") else None
    gid = body["gid"] if body.has_key("gid") else None
    ip = body["ip"] if body.has_key("ip") else []
    mac = body["mac"] if body.has_key("mac") else []
    child_group = body["child_group"] if body.has_key("child_group") else []
    if None in child_group:
        return json.dumps({"message": u"IP、MAC和网络结构数量不对应"})
    if len(ip) != len(mac) or len(mac) != len(child_group):
        return json.dumps({"message": u"IP、MAC和网络结构数量不对应"})
    host_ip = body["host_ip"] if body.has_key("host_ip") else ""
    asset_classify = body["special"] if body.has_key("special") else ""
    oid = body["oid"] if body.has_key("oid") else None
    position = body["position"] if body.has_key("position") else ""
    cperson_name = body["cperson_name"] if body.has_key("cperson_name") else ""  #  创建者
    mperson_name = body["mperson_name"] if body.has_key("mperson_name") else ""  #  管理者
    cperson_tel = body["cperson_tel"] if body.has_key("cperson_tel") else ""  #  创建者联系方式
    mperson_tel = body["mperson_tel"] if body.has_key("mperson_tel") else ""  #  管理者联系方式
    title = body["title"] if body.has_key("title") else []
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if source == "1":
        # 新增资产信息
        uuid = str(U.uuid1()).replace('-', '')
        sql = "insert into h_hardware_asset_info (asset_id, node_id, group_id, asset_level, type_id, register_state, net_state, retire_state, delete_state, source, position, model, policy, asset_label, asset_classify, host_ip, register_time, create_person, create_contact, manage_person, manage_contact) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        args = (uuid, oid, gid, level, type, '1', '0', '0', '1', '1', position, model, basepolicy, ','.join(title), asset_classify, host_ip, time, cperson_name, cperson_tel, mperson_name, mperson_tel, )
        try:
            g.cursor.execute(sql, args)
            asset_manul_log(uuid, uid, u'登记', '')
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"插入数据库失败"})
    elif source == "0":
        # 更新资产信息
        sql = "update h_hardware_asset_info set node_id=%s, group_id=%s, host_ip=%s, asset_level=%s, type_id=%s, register_state=%s, position=%s, model=%s, policy=%s, asset_label=%s, asset_classify=%s, register_time=%s, create_person=%s, create_contact=%s, manage_person=%s, manage_contact=%s where asset_id=%s"
        args = (oid, gid, host_ip, level, type, '1', position, model, basepolicy, ','.join(title), asset_classify, time, cperson_name, cperson_tel, mperson_name, mperson_tel, uuid, )
        try:
            g.cursor.execute(sql, args)
            asset_manul_log(uuid, uid, u'登记', '')
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"更新数据库失败"})
        sql = "delete from h_asset_ip_info where asset_id=%s"
        args = (uuid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"删除数据库失败"})

    sql_insert = "insert into h_asset_ip_info (asset_id, ip_addr, mac_addr, group_id) values (%s, %s, %s, %s)"
    sql_select = "select  conf_type, start_ip_value,end_ip_value, ip_group_value, start_ip_addr, end_ip_addr, ip_addr, subnet_mask, ip_group from h_network_domain_range where group_id=%s "
    for index, eachip in enumerate(ip):
        eachmac = mac[index]
        eachcgroup = child_group[index]
        select_args = (eachcgroup, )
        try:
            g.cursor.execute(sql_select, select_args)
            result = g.cursor.fetchall()
            flag = False
            if eachip:
                try:
                    ip = IP(eachip).int()
                except:
                    return json.dumps({"message": u"ip地址格式有误"})
                error_message = ""
                for eachresult in result:
                    type = eachresult["conf_type"]
                    ip1 = eachresult["start_ip_value"]
                    ip2 = eachresult["end_ip_value"]
                    ip3 = eachresult["ip_group_value"]
                    start_ip_addr = eachresult["start_ip_addr"]
                    end_ip_addr = eachresult["end_ip_addr"]
                    ip_addr = eachresult["ip_addr"]
                    subnet_mask = eachresult["subnet_mask"]
                    ip_group = eachresult["ip_group"]
                    if type == "1":
                        if ip >= ip1 and ip <= ip2:
                            flag = True
                        else:
                            error_message += u'ip掩码:  %s、%s' % (ip_addr, subnet_mask)
                    if type == "2":
                        if ip >= ip1 and ip <= ip2:
                            flag = True
                        else:
                            error_message += u'ip区间:  %s ~ %s' % (start_ip_addr, end_ip_addr)
                    if type == "3":
                        if ip == ip3:
                            error_message += u'ip集:  %s' % ip_group
                select_sql = "select type from h_asset_group where group_id = %s"
                select_args = (eachcgroup,)
                try:
                    g.cursor.execute(select_sql, select_args)
                    result = g.cursor.fetchone()
                    conf_type = result["type"]
                except:
                    conf_type = None
                if conf_type == "2":
                    if not flag:
                        return json.dumps({"message": u"ip:%s, 不在绑定的网络域中。 %s" % (eachip, error_message)})
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"查询数据库失败"})
        insert_args = (uuid, eachip, eachmac, eachcgroup)
        try:
            g.cursor.execute(sql_insert, insert_args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"插入数据库失败"})            
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "资产登记"
    })

    return json.dumps({"message": "ok"})


# 资产退役 /asset/retire
@app.route('/asset/retire', methods=['POST'])
@auth.permission("asset")
def asset_retire(_currUser):
    manul_user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (manul_user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else ""
    manul = body["manul"] if body.has_key("manul") else ""
    gid = body["gid"] if body.has_key("gid") else None
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ids = uuid
    sql = "update h_hardware_asset_info set retire_state=%s, retire_time=%s  where asset_id IN ( SELECT unnest ( array [%s] ) )"
    args = (manul, timestamp, ids, )
    if manul == "0":
        message = u"恢复"
    else:
        message = u"退役"
    try:
        g.cursor.execute(sql, args)
        for eachuuid in ids:
            asset_manul_log(eachuuid, uid, message, '')
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"更新数据库失败"})
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "资产退役"
    })

    return json.dumps({"message": "ok"})


# 资产关注 /asset/attention
@app.route('/asset/attention', methods=['POST'])
@auth.permission("asset")
def asset_attention(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else []
    manul = body["manul"] if body.has_key("manul") else ""
    if manul == "1":
        sql = "insert into h_person_asset(person_id, asset_id, r_time) values (%s, %s, %s) "
        time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for eachuuid in uuid:
            args = (uid, eachuuid, time,)
            try:
                g.cursor.execute(sql, args)
            except Exception as e:
                print 'fail : %s\n' % str(e)
                return json.dumps({"message": u"插入数据库失败"})

        auth.logsync(_currUser, {
            "function": "资产管理",
            "type": "更新",
            "remark": "关注资产"
        })
    else:
        sql = "delete from h_person_asset where person_id=%s and asset_id IN ( SELECT unnest ( array [%s] ) )"
        args = (uid, uuid, )
        try:
            g.cursor.execute(sql, args)
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"删除数据库失败"})

        auth.logsync(_currUser, {
            "function": "资产管理",
            "type": "更新",
            "remark": "取消关注资产"
        })
    g.conn.commit()

    return json.dumps({"message": "ok"})


# 创建标签 /asset/createtitle
@app.route('/asset/createtitle', methods=['POST'])
@auth.permission("asset")
def asset_createtitle(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    name = body["name"] if body.has_key("name") else ""
    uuid = str(U.uuid1()).replace('-', '')
    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = "select label_name from k_asset_label"
    args = ()
    data = []
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        for eachresult in result:
            label_name = eachresult["label_name"]
            data.append(label_name)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    if name in data:
        return json.dumps({"message": u"标签名称已存在"})

    sql = "insert into k_asset_label(label_id, label_name, r_time) values (%s, %s, %s)"
    args = (uuid, name, time, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"插入数据库失败"})
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "创建标签"
    })

    return json.dumps({"message": "ok"})


# 获取标签列表 /asset/listtitle
@app.route('/asset/listtitle', methods=['POST'])
@auth.permission("asset")
def asset_listtitle(_currUser):
    sql = "select label_id, label_name from k_asset_label"
    args = ()
    data = []
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        for eachresult in result:
            id = eachresult["label_id"]
            name = eachresult["label_name"]
            data.append([id, name])
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})

    return json.dumps({"message": "ok", "data": data, "total": len(data)})


# 修改标签名称 /asset/renametitle
@app.route('/asset/renametitle', methods=['POST'])
@auth.permission("asset")
def asset_renametitle(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    id = body["id"] if body.has_key("id") else ""
    newname = body["newname"] if body.has_key("newname") else ""
    sql = "select label_name from k_asset_label"
    args = ()
    data = []
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        for eachresult in result:
            tname = eachresult["label_name"]
            data.append(tname)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    if newname in data:
        return json.dumps({"message": u"标签名称已存在"})
    sql = "update k_asset_label set label_name=%s where label_id=%s"
    args = (newname, id, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"更新数据库失败"})
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "更改标签名称"
    })

    return json.dumps({"message": "ok"})


# 删除标签 /asset/deletetitle
@app.route('/asset/deletetitle', methods=['POST'])
@auth.permission("asset")
def asset_deletetitle(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else []
    sql = "select "
    sql = "delete from k_asset_label where  label_id IN ( SELECT unnest ( array [%s] ) )"
    args = (uuid, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"删除数据库失败"})
    sql = "select asset_id, asset_label from h_hardware_asset_info "
    args = ()
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    for eachresult in result:
        asset_id = eachresult["asset_id"]
        asset_label = eachresult["asset_label"]
        try:
            label_list = asset_label.split(',')
        except:
            label_list = []
        for eachuuid in uuid:
            if eachuuid in label_list:
                try:
                    new_label_list = label_list.remove(eachuuid)
                except Exception as e:
                    print 'fail : %s\n' % str(e)
                    new_label_list = label_list
                sql = "update h_hardware_asset_info set asset_label=%s where asset_id=%s"
                try:
                    args = (','.join(new_label_list), asset_id, )
                except:
                    args = (None, asset_id,)
                try:
                    g.cursor.execute(sql, args)
                except Exception as e:
                    print 'fail : %s\n' % str(e)
                    return json.dumps({"message": u"更新数据库失败"})
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "删除标签"
    })
    return json.dumps({"message": "ok"})


# 资产绑定标签 /asset/settitle
@app.route('/asset/settitle', methods=['POST'])
@auth.permission("asset")
def asset_settitle(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else ""
    title = body["title"] if body.has_key("title") else []
    sql = " update h_hardware_asset_info set asset_label=%s where asset_id=%s "
    args = (','.join(title), uuid, )
    try:
        g.cursor.execute(sql, args)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"更新数据库失败"})
    g.conn.commit()

    auth.logsync(_currUser, {
        "function": "资产管理",
        "type": "更新",
        "remark": "资产绑定标签"
    })

    return json.dumps({"message": "ok"})


# 根据IP地址查询相关联设备组 /asset/group/ipaddr
@app.route('/asset/group/ipaddr', methods=['POST'])
@auth.permission("asset")
def asset_group_ipaddr(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    ip = body["ip"] if body.has_key("ip") else []
    try:
        data = map(lambda x: IP(x).int(), ip)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"IP地址格式有误"})
    sql = "select distinct group_id from h_network_domain_range where start_ip_value <= %s and end_ip_value >= %s"
    groupList = []
    for eachvalue in data:
        args = (eachvalue, eachvalue, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchall()
            for eachresult in result:
                group_id = eachresult["group_id"]
                if group_id in groupList:
                    continue
                groupList.append(group_id)
        except:
            pass
    sql = "select group_id as id, group_name as name, parent_group_id as pid from h_asset_group where group_id in ( select unnest ( array [%s] ) )"
    args = (groupList, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
    except Exception as e:
        print 'fail : %s\n' % str(e)
        result = []
    g.conn.commit()

    return json.dumps({"message": "ok", "data": result})


# 资产列表-统计信息 /asset/census
@app.route('/asset/census', methods=['POST'])
@auth.permission("asset")
def asset_census(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    tree = body["tree"] if body.has_key("tree") else ""
    gid = body["gid"] if body.has_key("gid") else None
    manul = body["manul"] if body.has_key("manul") else ""
    groups = [gid]
    # 组织结构
    if tree == "0":
        sql = '''
                with RECURSIVE cte as
                        (
                        select * from sys_node_info a where node_id=%s
                        union all
                        select k.* from sys_node_info k inner join cte c on c.node_id = k.parent_node_id
                        )select array_agg(node_id) as ids from cte;
        '''
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
            if data["ids"]:
                gids = data["ids"]
            else:
                gids = []
        except:
            return json.dumps({"message": u"查询数据库失败"})

        sql = "select node_id as group_id, node_name as group_name from sys_node_info where node_id=%s"
        args = (gid, )
        #gids = organ_tree(gid, groups)
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
        except:
            return json.dumps({"message": u"查询数据库失败"})
    else:
        gids = assetgroup_tree(gid, groups)
        sql = '''
                select group_id, group_name, manage_person as mperson_name, create_person as cperson_name, manage_contact as mperson_tel, create_contact as cperson_tel, remark as comment, type, sub_type
                from
	                  h_asset_group dgroup 
                where
	                  group_id = %s 
        '''
        if not gid == "unregister":
            args = (gid, )
        else:
            args = (None, )
        try:
            g.cursor.execute(sql, args)
            data = g.cursor.fetchone()
        except Exception as e:
            print 'fail : %s\n' % str(e)
            return json.dumps({"message": u"查询数据库失败"})

        try:
            data["ipinfo"] = []
            type = data["type"]
            sub_type = data["sub_type"]
            sql = "select conf_type, ip_addr, subnet_mask, start_ip_addr, end_ip_addr, ip_group from h_network_domain_range where group_id = %s"
            args = (gid, )
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchall()
            except:
                result = []
            if type == "2":
                # ip 掩码
                if sub_type == "1":
                    for eachresult in result:
                        conf_type = eachresult["conf_type"]
                        ip_addr = eachresult["ip_addr"]
                        subnet_mask = eachresult["subnet_mask"]
                        if conf_type == "1":
                            data["ipinfo"].append([conf_type, ip_addr, subnet_mask])
                # ip区间  ip集
                if sub_type == "2" or sub_type == "3":
                    for eachresult in result:
                        conf_type = eachresult["conf_type"]
                        ip_group = eachresult["ip_group"]
                        start_ip_addr = eachresult["start_ip_addr"]
                        end_ip_addr = eachresult["end_ip_addr"]
                        if conf_type == "2":
                            data["ipinfo"].append([conf_type, start_ip_addr, end_ip_addr])
                        if conf_type == "3":
                            data["ipinfo"].append([conf_type, ip_group])
        except:
            pass
    if not data:
        data = {}
        data["ipinfo"] = []

    # 获取设备曲线数据
    tmpdict = {}
    sql = "select type_id, type_name from h_asset_type where parent_type_id = '1' or parent_type_id = '2'"
    args = ()
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        for eachdata in result:
            type_name = eachdata["type_name"]
            type_id = eachdata["type_id"]
            groups = [type_id]
            typeids = map(int, get_type(type_id, groups))
            if not tmpdict.has_key(type_name):
                tmpdict[type_name] = typeids
            else:
                tmpdict[type_name].extend(typeids)
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    today = datetime.now()
    before = (today - timedelta(days=19)).strftime("%Y-%m-%d")
    dateList = getEveryDay(before, today.strftime("%Y-%m-%d"))
    sql1 = " select count(asset_id) from h_hardware_asset_info where  type_id in ( select unnest ( array [%s] ) ) and  create_time <= %s "
    sql2 = " select count(asset_id) from h_hardware_asset_info where  type_id is null and  create_time <= %s "
    if tree == "0":
        if  not "unregister" in gids:
            sql1 += " and node_id in ( select unnest ( array [%s] ) ) "
            sql2 += " and node_id in ( select unnest ( array [%s] ) ) "
            args1 = [gids]
            args2 = [gids]
        else:
            sql1 += " and node_id is null "
            sql2 += " and node_id is null "
            args1 = []
            args2 = []
    else:
        if  not "unregister" in gids:
            sql1 += " and group_id in ( select unnest ( array [%s] ) ) "
            sql2 += " and group_id in ( select unnest ( array [%s] ) ) "
            args1 = [gids]
            args2 = [gids]
        else:
            sql1 += " and group_id is null "
            sql2 += " and group_id is null "
            args1 = []
            args2 = []
    if manul == "0":
        sql1 += " and delete_state = '1' "
        sql2 += " and delete_state = '1' "
    elif manul == "1":
        sql1 += " and delete_state = '1' and register_state = '1' and retire_state = '0' "
        sql2 += " and delete_state = '1' and register_state = '1' and retire_state = '0' "
    else:
        sql1 += " and delete_state = '1' and register_state = '0' "
        sql2 += " and delete_state = '1' and register_state = '0' "
    censusDict = {}
    for type_name, idList in tmpdict.items():
        censusDict[type_name] = []
        for eachdate in dateList:
            args_new = copy.copy(args1)
            args_new.insert(0, idList)
            args_new.insert(1, eachdate + ' 23:59:59')
            try:
                g.cursor.execute(sql1, args_new)
                result = g.cursor.fetchone()
                count = result["count"]
            except Exception as e:
                print 'fail : %s\n' % str(e)
                return json.dumps({"message": u"查询数据库失败"})
            censusDict[type_name].append({"date": eachdate, "count": count})
    # 统计各类型数据（按日期）
    tmpdict = {}
    for type_name, countList in censusDict.items():
        for eachdata in countList:
            date = eachdata["date"]
            count = eachdata["count"]
            if not tmpdict.has_key(date):
                tmpdict[date] = int(count)
            else:
                tmpdict[date] = tmpdict[date] + int(count)
    dateList = tmpdict.keys()
    dateList.sort()
    totalData = []
    # 统计未知设备数据（按日期）
    for eachdate in dateList:
        try:
            args_new = copy.copy(args2)
            args_new.insert(0, eachdate + ' 23:59:59')
            g.cursor.execute(sql2, args_new)
            result = g.cursor.fetchone()
            count = result["count"]
        except Exception as e:
            count = 0
        totalData.append({"date": eachdate, "count": count})
    censusDict[u"未知设备"] = totalData

    totalData = []
    # 统计设备总数数据（按日期）
    for index, eachdate in enumerate(dateList):
        sum = 0
        for key, eachitem in censusDict.items():
            try:
                count = eachitem[index]["count"]
            except:
                count = 0
            sum += count
        totalData.append({"date": eachdate, "count": sum})

    censusDict[u"设备总数"] = totalData

    try:
        data["census"] = censusDict
    except Exception as e:
        print 'fail : %s\n' % str(e)
        data = {"census": {}}

    g.conn.commit()
    return json.dumps({"message": "ok", "data": data})


# 查询当前登录的用户信息 /asset/user/getinfo
@app.route('/asset/user/getinfo', methods=['POST'])
@auth.permission("asset")
def asset_user_getinfo(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    sql = "select person_id as id, person_name as name, mobile_phone as tel from sys_person where person_id = %s"
    args = (uid, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        g.conn.commit()
        name = result["name"]
        id = result["id"]
        tel = result["tel"]
    except Exception as e:
        print 'fail : %s\n' % str(e)
        return json.dumps({"message": u"查询数据库失败"})
    data = {"id": id, "name": name, "tel": tel}

    return json.dumps({"message": "ok", "data": data})



def  getGroupInfo(group_id, dataList):
    try:
        sql = " select group_id as id, case when parent_group_id  is null then '0' else parent_group_id end as pid, group_name as name, type, sub_type, area_type, remark as comment from h_asset_group where group_id=%s and state='1' "
        args = (group_id, )
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        pid = result["pid"]
        if pid == None or pid == '0':
            dataList.insert(0, result)
            return dataList
        dataList.insert(0, result)
        dataList = getGroupInfo(pid, dataList)
    except Exception as e:
        print str(e)
        return []
    return dataList


def  getNodeInfo(node_id, dataList):
    try:
        sql = " select node_id as id, case when parent_node_id  is null then '0' else parent_node_id end as pid, node_name as name, node_type_id from sys_node_info where node_id=%s and state='1' "
        args = (node_id, )
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        pid = result["pid"]
        if pid == None or pid == '0':
            dataList.insert(0, result)
            return dataList
        dataList.insert(0, result)
        dataList = getNodeInfo(pid, dataList)
    except Exception as e:
        print str(e)
        return []
    return dataList


# 通过资产查询网络结构信息 /asset/searchtree
@app.route('/asset/searchtree', methods=['POST'])
@auth.permission("asset")
def asset_searchtree(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    uuid = body["uuid"] if body.has_key("uuid") else ""
    tree = body["tree"] if body.has_key("tree") else ""
    dataList = []
    if tree == "1":
        sql = "select group_id from h_hardware_asset_info where asset_id = %s"
        args = (uuid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            if result:
                group_id = result["group_id"]
            else:
                group_id = None
        except Exception as e:
            print str(e)
            return json.dumps({"message": u"查询数据库失败"})
        if  group_id:
            dataList = getGroupInfo(group_id, dataList)
    else:
        sql = "select node_id from h_hardware_asset_info where asset_id = %s"
        args = (uuid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            if result:
                node_id = result["node_id"]
            else:
                node_id = None
        except Exception as e:
            print str(e)
            return json.dumps({"message": u"查询数据库失败"})
        if  node_id:
            dataList = getNodeInfo(node_id, dataList)
    g.conn.commit()

    return json.dumps({"message": "ok", "data": dataList})


# 资产批量导入模板下载 /asset/assetmodel/download
@app.route('/asset/assetmodel/download', methods=['GET'])
@auth.permission("asset")
def asset_assetmodel_download(_currUser):
    filename = "asset_model.xls"
    directory = "/opt/CyberSA/src/management_center/api/lib"
    response = make_response(send_from_directory(directory, filename, as_attachment=True))
    return response


# 资产批量导入  /asset/assetmodel/upload
@app.route('/asset/assetmodel/upload',methods=['POST','GET'])
@auth.permission("asset")
def asset_model_upload(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    assetlevelList = [1, 2, 3, 4, 5]
    if request.method=='POST':
        import time
        a = time.time()
        file = request.files['file']
        filetype = file.content_type   #application/vnd.ms-excel
        if  not filetype in "application/vnd.ms-excel":
            return json.dumps({"message": u"excel导入失败"})

        f = file.read()  # 文件内容
        data = xlrd.open_workbook(file_contents=f)
        table = data.sheets()[0]
        names = data.sheet_names()  # 返回book中所有工作表的名字
        status = data.sheet_loaded(names[0])  # 检查sheet1是否导入完毕
        if not status:
            return json.dumps({"message": u"excel导入失败"})
        nrows = table.nrows  # 获取该sheet中的有效行数
        for eachline in range(nrows):
            message_type = u"成功"  # message_type: 错误类型     "0": 成功
            if eachline < 7:
                # 标题栏， 跳过
                continue
            uuid = str(U.uuid1()).replace('-', '')  # 生成UUID
            content = table.row_values(eachline)
            # 处理一行数据
            # 第1列 存在形式
            asset_classify = content[1]
            if not asset_classify:
                break
            if not asset_classify in ["实体", "虚拟"]:
                message_type = u"存在形式有误"
            # 第2列 类型
            asset_type = content[2]
            if not asset_type:
                message_type = u"未输入类型"
            try:
                tmptype = asset_type.strip().split("/")
                pname = tmptype[0]
                cname = tmptype[-1]
            except:
                cname = None
                pname = None
            type_sql = " select parent_type_id from h_asset_type where type_name = %s"
            type_args = (cname, )
            try:
                g.cursor.execute(type_sql, type_args)
                result = g.cursor.fetchone()
                id = result["parent_type_id"]
            except:
                id = None
            type_sql = " select type_name from h_asset_type where type_id = %s"
            type_args = (id, )
            try:
                g.cursor.execute(type_sql, type_args)
                result = g.cursor.fetchone()
                ppname = result["type_name"]
            except:
                ppname = None
            if  not pname == ppname:
                message_type = u"资产类型有误"
            # 第3列 型号
            asset_model = content[3]
            # 第4列 级别
            asset_level = content[4]
            if not asset_level:
                message_type = u"未输入级别"
            if not int(asset_level) in assetlevelList:
                message_type = u"资产级别有误"
            # 第5列 标签
            asset_label = content[5]
            try:
                label_list = asset_label.strip().split(";")
                if len(label_list) > 3:
                    message_type = u"标签数量超限"
                for eachlabel in label_list:
                    if len(eachlabel) > 10:
                        message_type = u"标签长度超限"
            except:
                message_type = u"标签分隔格式错误"
            # 第6列 组织结构
            node_id = content[6]
            if not node_id:
                message_type = u"未输入组织结构"
            try:
                nodeList = node_id.strip().split("/")[::-1]  # 从子级往上判断
                node_sql = "select node_id, parent_node_id from sys_node_info where node_name = %s"
                cpid = None
                for index, nodename in enumerate(nodeList):
                    node_args = (nodename, )
                    try:
                        g.cursor.execute(node_sql, node_args)
                        result = g.cursor.fetchone()
                        id = result["node_id"]
                        if not index == 0:
                            if not id == cpid:
                                message_type = u"组织结构有误"
                        pid = result["parent_node_id"]
                        cpid = pid
                    except:
                        message_type = u"组织结构有误"
            except:
                message_type = u"组织结构格式有误"
            # 第7列 域
            group_id = content[7]
            if not group_id:
                message_type = u"未输入域"
            try:
                groupList = group_id.strip().split("/")[::-1]  # 从子级往上判断
                group_sql = "select group_id, parent_group_id from h_asset_group where group_name = %s"
                cpid = None
                for index, groupname in enumerate(groupList):
                    group_args = (groupname, )
                    try:
                        g.cursor.execute(group_sql, group_args)
                        result = g.cursor.fetchone()
                        id = result["group_id"]
                        if not index == 0:
                            if not id == cpid:
                                message_type = u"域有误"
                        pid = result["parent_group_id"]
                        cpid = pid
                    except:
                        message_type = u"域有误"
            except:
                message_type = u"域格式有误"
            # 第8列 IP
            ipinfo = content[8]
            if not ipinfo:
                message_type = u"未输入ip"
            try:
                ipaddr = ipinfo.strip().split(";")
                ipcount = len(ipaddr)
                for eachip in ipaddr:
                    try:
                        IP(eachip).int()
                    except:
                        message_type = u"IP地址有误"
            except:
                ipcount = 0
                message_type = u"IP地址格式有误"
            # 判断域下只有一个存在的IP
            new_group_id = getGroupid(group_id.strip().split("/"))
            sql = "select array_agg(asset_id) as asset_ids from h_hardware_asset_info where group_id = %s"
            args = (new_group_id, )
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if not result:
                    asset_ids = []
                else:
                    asset_ids = result["asset_ids"]
            except:
                asset_ids = []
            sql = "select array_agg(distinct  ip_addr) as ips from h_asset_ip_info where asset_id in ( select unnest ( array [%s]))"
            args = (asset_ids, )
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if not result:
                    ips = []
                else:
                    ips = result["ips"]
            except:
                ips = []
            sql = "select ip_addr from h_asset_import where create_person=%s and group_id=%s "
            args = (uid, group_id, )
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if not result:
                    ips_import = ""
                else:
                    ips_import = result["ip_addr"]
            except:
                ips_import = ""
            try:
                ips_import = map(str, ips_import.strip().split(";"))
            except:
                ips_import = []
            if not ips:
                ips = ips_import
            else:
                ips.extend(ips_import)
            if ips:
                try:
                    ipaddrs = map(str, ipinfo.strip().split(";"))
                    for eachip in ipaddrs:
                        if eachip in ips:
                            message_type = u"域下存在相同IP"
                except:
                    pass
            # 第9列 MAC
            macinfo = content[9]
            if not macinfo:
                message_type = u"未输入mac"
            try:
                macaddr = macinfo.strip().split(";")
                maccount = len(macaddr)
                for eachmac in macaddr:
                    if not re.match(r"^\s*([0-9a-fA-F]{2,2}-){5,5}[0-9a-fA-F]{2,2}\s*$", eachmac):
                        message_type = u"MAC地址有误"
            except:
                maccount = 0
                message_type = u"IP地址格式有误"
            # 判断mac地址是否重复
            sql = "select array_agg(distinct mac_addr) as mac_addr from h_asset_ip_info"
            args = ()
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if not result:
                    macs = []
                else:
                    macs = result["mac_addr"]
            except:
                macs = []
            sql = "select mac_addr from h_asset_import where create_person=%s and group_id=%s "
            args = (uid, group_id, )
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if not result:
                    mac_import = ""
                else:
                    mac_import = result["mac_addr"]
            except:
                mac_import = ""
            try:
                mac_import = map(str, mac_import.strip().split(";"))
            except:
                mac_import = []
            macs.extend(mac_import)
            if macs:
                try:
                    macaddrs = map(str, macinfo.strip().split(";"))
                    #print ipaddrs
                    for eachip in macaddrs:
                        if eachip in macs:
                            message_type = u"域下存在相同MAC"
                except:
                    pass
            # 第10列 域下网络结构
            netgroup_id = content[10]
            if not netgroup_id:
                message_type = u"未输入网络结构"
            try:
                netgroup_ids = netgroup_id.strip().split(";")
                netgroupcount = len(netgroup_ids)
                for eachgroup_id in netgroup_ids:
                    try:
                        groupList = eachgroup_id.strip().split("/")[::-1]  # 从子级往上判断
                        group_sql = "select group_id, parent_group_id from h_asset_group where group_name = %s and state = '1'"
                        cpid = None
                        for index, groupname in enumerate(groupList):
                            group_args = (groupname,)
                            try:
                                g.cursor.execute(group_sql, group_args)
                                result = g.cursor.fetchone()
                                id = result["group_id"]
                                if not index == 0:
                                    if not id == cpid:
                                        message_type = u"域下网络结构有误"
                                pid = result["parent_group_id"]
                                cpid = pid
                            except Exception as e:
                                message_type = u"域下网络结构有误"
                    except:
                        message_type = u"域下网络结构格式有误"
            except:
                netgroupcount = 0
                message_type = u"域下网络结构格式有误"
            if ipcount != maccount or maccount != netgroupcount:
                message_type = u"ip-mac-网络结构未对应"
            #print message_type
            # 第11列 部署位置
            position = content[11]
            # 第12列 操作系统
            os_ver = content[12]
            # 第13列 应用
            app = content[13]
            # 第14列 应用管理
            app_manage = content[14]
            # 第15列 硬件管理
            hardware_manage = content[15]
            # 第16列 管理人
            manage_person = content[16]
            # 第17列 管理人联系方式
            manage_contact = int(content[17])
            # 将这行资产数据插入缓存中间表
            sql = "insert into h_asset_import(asset_id, asset_classify, type_id, os_ver, application, application_manage, hardware_manage, model, asset_level, asset_label, node_id, group_id, ip_addr, mac_addr, netgroup_addr, position, manage_person, manage_contact, remarks, create_person, flag) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            assetlist = [uuid, asset_classify, asset_type, os_ver, app, app_manage, hardware_manage, asset_model, asset_level, asset_label, node_id, group_id, ipinfo, macinfo, netgroup_id, position, manage_person, manage_contact, message_type, uid, "0"]
            try:
                g.cursor.execute(sql, assetlist)
                g.conn.commit()
            except Exception as e:
                print 'fail: %s' % str(e)

        b = time.time()
        print '耗时: %s秒' % (b-a)
        return json.dumps({"message": "ok"})

    return  render_template('upload.html')


# 资产批量导入后资产列表 /asset/import/main
@app.route('/asset/import/main', methods=['POST'])
@auth.permission("asset")
def asset_assetimport_main(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    select = body["select"] if body.has_key("select") else ""
    sql = "select asset_id, asset_classify, model, type_id, asset_level, asset_label, node_id, position, os_ver, application as app, application_manage as app_manage, hardware_manage, group_id, ip_addr, mac_addr, netgroup_addr, manage_person, manage_contact as manage_tel, remarks as message, flag from h_asset_import where create_person = %s"
    args = (uid, )
    if select == "1":
        sql += " and remarks != '成功' "
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        g.conn.commit()
        data = result
        total_count = len(data)
    except:
        return json.dumps({"message": u"查询数据库失败"})
    sql = "select count(asset_id) from h_asset_import where remarks != '成功' and create_person = %s"
    args = (uid, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        g.conn.commit()
        fail_count = result["count"]
    except:
        return json.dumps({"message": u"查询数据库失败"})
    g.conn.commit()

    return json.dumps({"message": "ok", "data": data, "totalcount": total_count, "failcount": fail_count})


# 资产批量资产列表编辑 /asset/import/update
@app.route('/asset/import/update', methods=['POST'])
@auth.permission("asset")
def asset_assetimport_update(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    assetlevelList = [1, 2, 3, 4, 5]
    request_data  = request.get_data()
    body = json.loads(request_data)
    asset_id = body["asset_id"] if body.has_key("asset_id") else ""
    manage_person = body["manage_person"] if body.has_key("manage_person") else ""
    manage_tel = body["manage_tel"] if body.has_key("manage_tel") else ""
    type_id = body["type_id"] if body.has_key("type_id") else ""
    ip_addr = body["ip_addr"] if body.has_key("ip_addr") else ""
    node_id = body["node_id"] if body.has_key("node_id") else ""
    asset_level = body["asset_level"] if body.has_key("asset_level") else ""
    netgroup_addr = body["netgroup_addr"] if body.has_key("netgroup_addr") else ""
    mac_addr = body["mac_addr"] if body.has_key("mac_addr") else ""
    position = body["position"] if body.has_key("position") else ""
    model = body["model"] if body.has_key("model") else ""
    asset_classify = body["asset_classify"] if body.has_key("asset_classify") else ""
    group_id = body["group_id"] if body.has_key("group_id") else ""
    asset_label = body["asset_label"] if body.has_key("asset_label") else ""
    os_ver = body["os_ver"] if body.has_key("os_ver") else ""
    app = body["app"] if body.has_key("app") else ""
    app_manage = body["app_manage"] if body.has_key("app_manage") else ""
    hardware_manage = body["hardware_manage"] if body.has_key("hardware_manage") else ""

    datadict ={
                "asset_id": asset_id,
                "manage_person": manage_person,
                "ip_addr": ip_addr,
                "type_id": type_id,
                "asset_level": asset_level,
                "netgroup_addr": netgroup_addr,
                "node_id": node_id,
                "mac_addr": mac_addr,
                "position": position,
                "group_id": group_id,
                "asset_label": asset_label,
                "manage_tel": manage_tel,
                "model": model,
                "asset_classify": asset_classify,
                "os_ver": os_ver,
                "app": app,
                "app_manage": app_manage,
                "hardware_manage": hardware_manage
    }
    message_type = u"成功"
    # 第1列 存在形式
    asset_classify = datadict["asset_classify"]
    if not asset_classify in ["实体", "虚拟"]:
        message_type = u"存在形式有误"
    # 第2列 类型
    asset_type = datadict["type_id"]
    if not asset_type:
        message_type = u"未输入类型"
    try:
        tmptype = asset_type.strip().split("/")
        pname = tmptype[0]
        cname = tmptype[-1]
    except:
        cname = None
        pname = None
    type_sql = " select parent_type_id from h_asset_type where type_name = %s"
    type_args = (cname,)
    try:
        g.cursor.execute(type_sql, type_args)
        result = g.cursor.fetchone()
        id = result["parent_type_id"]
    except:
        id = None
    type_sql = " select type_name from h_asset_type where type_id = %s"
    type_args = (id,)
    try:
        g.cursor.execute(type_sql, type_args)
        result = g.cursor.fetchone()
        ppname = result["type_name"]
    except:
        ppname = None
    if not pname == ppname:
        message_type = u"资产类型有误"
    # 第3列 型号
    model = datadict["model"]
    # 第4列 级别
    asset_level = datadict["asset_level"]
    if not asset_level:
        message_type = u"未输入级别"
    if not int(asset_level) in assetlevelList:
        message_type = u"资产级别有误"
    # 第5列 标签
    asset_label = datadict["asset_label"]
    try:
        label_list = asset_label.strip().split(";")
        if len(label_list) > 3:
            message_type = u"标签数量超限"
        for eachlabel in label_list:
            if len(eachlabel) > 10:
                message_type = u"标签长度超限"
    except:
        message_type = u"标签分隔格式错误"
    # 第6列 组织结构
    node_id = datadict["node_id"]
    if not node_id:
        message_type = u"未输入组织结构"
    try:
        nodeList = node_id.strip().split("/")[::-1]  # 从子级往上判断
        node_sql = "select node_id, parent_node_id from sys_node_info where node_name = %s"
        cpid = None
        for index, nodename in enumerate(nodeList):
            node_args = (nodename,)
            try:
                g.cursor.execute(node_sql, node_args)
                result = g.cursor.fetchone()
                id = result["node_id"]
                if not index == 0:
                    if not id == cpid:
                        message_type = u"组织结构有误"
                pid = result["parent_node_id"]
                cpid = pid
            except:
                message_type = u"组织结构有误"
    except:
        message_type = u"组织结构格式有误"
    # 第7列 域
    group_id = datadict["group_id"]
    if not group_id:
        message_type = u"未输入域"
    try:
        groupList = group_id.strip().split("/")[::-1]  # 从子级往上判断
        group_sql = "select group_id, parent_group_id from h_asset_group where group_name = %s and state = '1'"
        cpid = None
        for index, groupname in enumerate(groupList):
            group_args = (groupname,)
            try:
                g.cursor.execute(group_sql, group_args)
                result = g.cursor.fetchone()
                id = result["group_id"]
                if not index == 0:
                    if not id == cpid:
                        message_type = u"域有误"
                pid = result["parent_group_id"]
                cpid = pid
            except:
                message_type = u"域有误"
    except:
        message_type = u"域格式有误"
    # 第8列 IP
    ipinfo = datadict["ip_addr"]
    if not ipinfo:
        message_type = u"未输入ip"
    try:
        ipaddr = ipinfo.strip().split(";")
        ipcount = len(ipaddr)
        for eachip in ipaddr:
            try:
                IP(eachip).int()
            except:
                message_type = u"IP地址有误"
    except:
        ipcount = 0
        message_type = u"IP地址格式有误"
    # 第9列 MAC
    macinfo = datadict["mac_addr"]
    if not macinfo:
        message_type = u"未输入mac"
    try:
        macaddr = macinfo.strip().split(";")
        maccount = len(macaddr)
        for eachmac in macaddr:
            if not re.match(r"^\s*([0-9a-fA-F]{2,2}-){5,5}[0-9a-fA-F]{2,2}\s*$", eachmac):
                message_type = u"MAC地址有误"
    except:
        maccount = 0
        message_type = u"IP地址格式有误"
    # 第10列 域下网络结构
    netgroup_id = datadict["netgroup_addr"]
    if not netgroup_id:
        message_type = u"未输入网络结构"
    try:
        netgroup_ids = netgroup_id.strip().split(";")
        netgroupcount = len(netgroup_ids)
        for eachgroup_id in netgroup_ids:
            try:
                groupList = eachgroup_id.strip().split("/")[::-1]  # 从子级往上判断
                group_sql = "select group_id, parent_group_id from h_asset_group where group_name = %s and state = '1'"
                cpid = None
                for index, groupname in enumerate(groupList):
                    group_args = (groupname,)
                    try:
                        g.cursor.execute(group_sql, group_args)
                        result = g.cursor.fetchone()
                        id = result["group_id"]
                        if not index == 0:
                            if not id == cpid:
                                message_type = u"域下网络结构有误"
                        pid = result["parent_group_id"]
                        cpid = pid
                    except:
                        message_type = u"域下网络结构有误"
            except:
                message_type = u"域下网络结构格式有误"
    except:
        netgroupcount = 0
        message_type = u"域下网络结构格式有误"
    if ipcount != maccount or maccount != netgroupcount:
        message_type = u"ip-mac-网络结构未对应"
    # 第11列 部署位置
    position = datadict["position"]
    # 第12列 操作系统
    os_ver = datadict["os_ver"]
    # 第13列 应用
    app = datadict["app"]
    # 第14列 应用管理
    app_manage = datadict["app_manage"]
    # 第15列 硬件管理
    hardware_manage = datadict["hardware_manage"]
    # 第16列 管理人
    manage_person = datadict["manage_person"]
    # 第17列 管理人联系方式
    manage_tel = datadict["manage_tel"]
    if  message_type == "成功":
        sql = "update h_asset_import set asset_classify=%s, type_id=%s, os_ver=%s, application=%s, application_manage=%s, hardware_manage=%s, model=%s, asset_level=%s, asset_label=%s, node_id=%s, group_id=%s, ip_addr=%s, mac_addr=%s, netgroup_addr=%s, position=%s, manage_person=%s, manage_contact=%s, remarks=%s where asset_id=%s and create_person=%s "
        assetlist = [asset_classify, asset_type, os_ver, app, app_manage, hardware_manage, model, asset_level, asset_label, node_id, group_id, ipinfo, macinfo, netgroup_id, position, manage_person, manage_tel, message_type, asset_id, uid]
        try:
            g.cursor.execute(sql, assetlist)
            g.conn.commit()
        except Exception as e:
            print str(e)
            return json.dumps({"message": u"更新数据库失败"})
        return json.dumps({"message": "ok"})
    else:
        return json.dumps({"message": "%s" % message_type})



# 批量导入资产列表删除 /asset/import/delete
@app.route('/asset/import/delete', methods=['POST'])
@auth.permission("asset")
def asset_assetimport_delete(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    request_data  = request.get_data()
    body = json.loads(request_data)
    asset_id = body["asset_id"] if body.has_key("asset_id") else ""
    sql = "delete from h_asset_import where asset_id = %s and create_person=%s"
    args = (asset_id, uid, )
    try:
        g.cursor.execute(sql, args)
        g.conn.commit()
    except Exception as e:
        print str(e)
        return json.dumps({"message": u"删除数据库失败"})

    return json.dumps({"message": "ok"})


def getNodeid(node_ids):
    sql = "select node_id from sys_node_info where node_name = %s and parent_node_id=%s"
    pnode_id = None
    for index, eachnodeid in enumerate(node_ids):
        if index == 0:
            pnode_id = '0'
        args = (eachnodeid, pnode_id)
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            pnode_id = result["node_id"]
        except:
            pass
    return pnode_id


def getGroupid(group_ids):
    pgroup_id = None
    for index, eachgroupid in enumerate(group_ids):
        if index == 0:
            sql = "select group_id from h_asset_group where group_name = %s and parent_group_id is null and state='1'"
            args = (eachgroupid, )
        else:
            sql = "select group_id from h_asset_group where group_name = %s and parent_group_id=%s and state='1'"
            args = (eachgroupid, pgroup_id, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            pgroup_id = result["group_id"]
        except:
            pass
    return pgroup_id

def getTypeid(type_ids, type_classify):
    sql = "select type_id from h_asset_type where type_name = %s and parent_type_id=%s"
    ptype_id = None
    for index, eachtypeid in enumerate(type_ids):
        if index == 0:
            if type_classify == u"实体":
                ptype_id = '1'
            else:
                ptype_id = '2'
        args = (eachtypeid, ptype_id)
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            ptype_id = result["type_id"]
        except:
            ptype_id  = None
    return ptype_id


# 批量导入执行全部导入操作 /asset/import/exec
@app.route('/asset/import/exec', methods=['POST'])
@auth.permission("asset")
def asset_assetimport_exec(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    sql = "select person_id as id, person_name as name, mobile_phone as tel from sys_person where person_id = %s"
    args = (uid, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        create_person = result["name"]
        create_contact = result["tel"]
    except:
        return json.dumps({"message": u"查询数据库失败"})

    try:
        sql = "update h_asset_import set flag=%s where create_person=%s"
        args = ("1", uid, )
        g.cursor.execute(sql, args)
        g.conn.commit()
    except Exception as e:
        print str(e)
        return json.dumps({"message": u"更新数据库失败"})

    sql = "select asset_id, node_id, group_id, type_id, os_ver, application, application_manage, hardware_manage, asset_level, asset_label, model, manage_person, position, manage_contact, ip_addr, mac_addr, netgroup_addr, asset_classify from h_asset_import where create_person = %s and remarks = '成功'"
    args = (uid, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchall()
        if result:
            for eachresult in result:
                message_type = "成功"
                asset_id = eachresult["asset_id"]
                node_id = eachresult["node_id"]
                group_id = eachresult["group_id"]
                type_id = eachresult["type_id"]
                asset_classify = eachresult["asset_classify"]
                type_id = getTypeid(type_id.strip().split("/"), asset_classify)
                if asset_classify == "实体":
                    asset_classify = "2"
                else:
                    asset_classify = "1"
                node_id = getNodeid(node_id.strip().split("/"))
                group_id = getGroupid(group_id.strip().split("/"))
                # 校验资产类型
                sql = "select type_id from h_asset_type where type_id=%s"
                args = (type_id, )
                try:
                    g.cursor.execute(sql, args)
                    result = g.cursor.fetchone()
                    if not result:
                        message_type = u"资产类型有误"
                except:
                    return json.dumps({"message": u"查询数据库失败"})
                # 校验组织结构
                sql = "select node_id from sys_node_info where node_id=%s"
                args = (node_id, )
                try:
                    g.cursor.execute(sql, args)
                    result = g.cursor.fetchone()
                    if not result:
                        message_type = u"组织结构有误"
                except:
                    return json.dumps({"message": u"查询数据库失败"})
                # 校验域
                sql = "select group_id from h_asset_group where group_id=%s"
                args = (group_id, )
                try:
                    g.cursor.execute(sql, args)
                    result = g.cursor.fetchone()
                    if not result:
                        message_type = u"域有误"
                except:
                    return json.dumps({"message": u"查询数据库失败"})
                asset_level = eachresult["asset_level"]
                model = eachresult["model"]
                manage_person = eachresult["manage_person"]
                position = eachresult["position"]
                os_ver = eachresult["os_ver"]
                app = eachresult["application"]
                app_manage = eachresult["application_manage"]
                hardware_manage = eachresult["hardware_manage"]
                manage_contact = eachresult["manage_contact"]
                ip_addr = eachresult["ip_addr"].split(";")
                mac_addr = eachresult["mac_addr"].split(";")
                netgroup_addr = eachresult["netgroup_addr"].split(";")
                asset_label = eachresult["asset_label"].strip().split(";")
                label_list = []
                select_sql = "select label_id from k_asset_label where label_name = %s limit 1"
                for eachlabelname in asset_label:
                    args = (eachlabelname, )
                    try:
                        g.cursor.execute(select_sql, args)
                        result = g.cursor.fetchone()
                        if not result:
                            uuid = str(U.uuid1()).replace('-', '')
                            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            insert_sql = "insert into k_asset_label(label_id, label_name, r_time) values (%s, %s, %s)"
                            args = (uuid, eachlabelname, time,)
                            try:
                                g.cursor.execute(insert_sql, args)
                                label_list.append(uuid)
                            except:
                                return json.dumps({"message": u"插入数据库失败"})
                        else:
                            label_id = result["label_id"]
                            label_list.append(label_id)
                    except Exception as e:
                        print str(e)
                label_str = ','.join(label_list)
                create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                sql = "insert into h_hardware_asset_info(asset_id, asset_classify, type_id, os_ver, application, application_manage, hardware_manage, model, asset_level, asset_label, node_id, group_id, position, manage_person, manage_contact, create_person, create_contact, create_time, register_time, register_state, net_state, retire_state, delete_state, source) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                assetlist = [asset_id, asset_classify, type_id, os_ver, app, app_manage, hardware_manage, model, asset_level, label_str, node_id, group_id, position, manage_person, manage_contact, create_person, create_contact, create_time, create_time, "1", "0", "0", "1", "3"]
                try:
                    g.cursor.execute(sql, assetlist)
                except Exception as e:
                    print str(e)
                    return json.dumps({"message": u"插入数据库失败"})
                insert_sql = "insert into h_asset_ip_info(asset_id, ip_addr, mac_addr, group_id) values(%s, %s, %s, %s)"
                for index, eachip in enumerate(ip_addr):
                    group_id = getGroupid(netgroup_addr[index].strip().split("/"))
                    # 校验域下网络结构
                    select_sql = "select group_id from h_asset_group where group_id=%s"
                    args = (group_id,)
                    try:
                        g.cursor.execute(select_sql, args)
                        result = g.cursor.fetchone()
                        if not result:
                            message_type = u"域下网络结构有误"
                    except:
                        return json.dumps({"message": u"查询数据库失败"})

                    args = (asset_id, eachip, mac_addr[index], group_id, )
                    try:
                        g.cursor.execute(insert_sql, args)
                    except Exception as e:
                        print str(e)
                        return json.dumps({"message": u"插入数据库失败"})
                print message_type
                if message_type == "成功":
                    g.conn.commit()
                else:
                    g.conn.rollback()
                    try:
                        sql = "update h_asset_import set remarks=%s, flag=%s where asset_id=%s and create_person=%s"
                        args = (message_type, "1", asset_id, uid, )
                        g.cursor.execute(sql, args)
                        g.conn.commit()
                    except Exception as e:
                        print str(e)
                        return json.dumps({"message": u"更新数据库失败"})

    except Exception as e:
        print str(e)
        return json.dumps({"message": u"查询数据库失败"})

    return json.dumps({"message": "ok"})


# 批量导入 取消-清空操作 /asset/import/clean
@app.route('/asset/import/clean', methods=['POST'])
@auth.permission("asset")
def asset_assetimport_clean(_currUser):
    user = _currUser['user']['user_name']
    sql = "select person_id from sys_login where user_name = %s"
    args = (user, )
    try:
        g.cursor.execute(sql, args)
        result = g.cursor.fetchone()
        uid = result['person_id']
    except:
        uid = None
    sql = "delete from h_asset_import where create_person=%s"
    args = (uid, )
    try:
        g.cursor.execute(sql, args)
        g.conn.commit()
    except Exception as e:
        print str(e)
        return json.dumps({"message": u"删除数据库失败"})

    return json.dumps({"message": "ok"})



# 批量修改组  /asset/group/update
@app.route('/asset/group/update', methods=['POST'])
@auth.permission("asset")
def asset_assetgroup_update(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    asset_id = body["asset_id"] if body.has_key("asset_id") else []
    gid = body["gid"] if body.has_key("gid") else ""
    cid = body["cid"] if body.has_key("cid") else ""
    tree = body["tree"] if body.has_key("tree") else ""
    data = []
    if tree == "0":
        # 批量修改组织结构
        for uuid in asset_id:
            sql = "select node_id from h_asset_organ_record where asset_id=%s"
            node_id = None
            args = (uuid, )
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if result:
                    node_id = result["node_id"]
            except:
                return json.dumps({"message": u"查询数据库失败"})
            if not node_id:
                sql = "insert into h_asset_organ_record(asset_id, node_id) values(%s, %s)"
                args = (uuid, gid, )
                try:
                    g.cursor.execute(sql, args)
                except:
                    return json.dumps({"message": u"插入数据库失败"})
            else:
                sql = "update h_asset_organ_record set node_id=%s where asset_id=%s"
                args = (gid, uuid, )
                try:
                    g.cursor.execute(sql, args)
                except:
                    return json.dumps({"message": u"更新数据库失败"})

        sql = "update h_hardware_asset_info set node_id=%s where asset_id in ( select unnest ( array [%s]))"
        args = (gid, asset_id, )
        try:
            g.cursor.execute(sql, args)
        except:
            return json.dumps({"message": u"更新数据库失败"})
    else:
        # 批量修改网络结构
        print gid
        sql = "select node_id from h_node_group_relation where group_id = %s limit 1"
        args = (gid, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            if result["node_id"]:
                node_id = result["node_id"]
            else:
                return json.dumps({"message": u"网络结构没有找到归属的组织结构"})
        except:
            return json.dumps({"message": u"查询数据库失败"})
        sql = "select array_agg(asset_id) as asset_ids from h_hardware_asset_info where group_id=%s"
        args = (gid, )
        asset_ids = []
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            if result["asset_ids"]:
                asset_ids = result["asset_ids"]
        except:
            return json.dumps({"message": u"查询数据库失败"})

        group_ips = []
        sql = "select array_agg(ip_addr) as ip_addr from h_asset_ip_info where asset_id in ( select unnest ( array [%s]))"
        args = (asset_ids, )
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            if result["ip_addr"]:
                group_ips = result["ip_addr"]
        except:
            return json.dumps({"message": u"查询数据库失败"})

        sql = "select type from h_asset_group where group_id = %s"
        args = (cid, )
        type = None
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            if result["type"]:
                type = result["type"]
        except:
            return json.dumps({"message": u"查询数据库失败"})
        sql = "select start_ip_value, end_ip_value from h_network_domain_range where group_id = %s"
        ipValue = []
        try:
            g.cursor.execute(sql, args)
            result = g.cursor.fetchone()
            if result:
                start_ip_value = result["start_ip_value"]
                end_ip_value = result["end_ip_value"]
                ipValue.extend([start_ip_value, end_ip_value])
        except:
            return json.dumps({"message": u"查询数据库失败"})
        for uuid in asset_id:
            sql = "select array_agg(ip_addr) as ip_addr from h_asset_ip_info where asset_id = %s"
            status = "ok"
            ips = []
            args = (uuid, )
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if result["ip_addr"]:
                    ips = result["ip_addr"]
            except Exception as e:
                print str(e)
                return json.dumps({"message": u"查询数据库失败"})
            for eachip in ips:
                if eachip in group_ips:
                    status = "域内IP冲突 (%s)" % eachip
                # 类型为区间时，判断IP是否符合
                if type == "2":
                    if ipValue:
                        ip = IP(eachip).int()
                        if ip < ipValue[0] or ip > ipValue[-1]:
                            status = "资产ip不符合网络结构内ip范围 (%s)" % eachip
            if status == "ok":
                sql = "update h_hardware_asset_info set group_id=%s, node_id=%s where asset_id = %s"
                args = (gid, node_id, uuid, )
                try:
                    g.cursor.execute(sql, args)
                except Exception as e:
                    print str(e)
                    return json.dumps({"message": u"更新数据库失败"})
                sql = "update h_asset_ip_info set group_id=%s where asset_id = %s"
                args = (cid, uuid, )
                try:
                    g.cursor.execute(sql, args)
                except Exception as e:
                    print str(e)
                    return json.dumps({"message": u"更新数据库失败"})
            sql = "select asset_name from h_hardware_asset_info where asset_id = %s"
            args = (uuid, )
            asset_name = ""
            try:
                g.cursor.execute(sql, args)
                result = g.cursor.fetchone()
                if result:
                    asset_name = result["asset_name"]
            except Exception as e:
                print str(e)
                return json.dumps({"message": u"查询数据库失败"})
            data.append({"name": asset_name, "status": status, "asset_id": uuid})
    g.conn.commit()

    return json.dumps({"message": "ok", "data": data})



# 获取公司组织结构 /asset/company/tree
@app.route('/asset/company/tree', methods=['POST'])
@auth.permission("asset")
def company_get(_currUser):
    sql = "select node_id as id, parent_node_id as pid, node_name as name from sys_node_info where state='1'  and node_type_id = '1'"
    args = ()
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchall()
    except:
        data = []

    return json.dumps({"message": "ok", "data": data})



# 获取组织结构下包含的资产 /asset/company/asset
@app.route('/asset/company/asset', methods=['POST'])
@auth.permission("asset")
def company_asset(_currUser):
    request_data  = request.get_data()
    body = json.loads(request_data)
    id = body["id"] if body.has_key("id") else ""
    groups = [id]
    gid = id
    groups = organ_tree(gid, groups)
    sql = "select array_agg(asset_id) as assets from h_hardware_asset_info where node_id in ( select unnest ( array [%s]))"
    args = (groups, )
    try:
        g.cursor.execute(sql, args)
        data = g.cursor.fetchone()
        if data["assets"]:
            data = data["assets"]
    except:
        data = []

    return json.dumps({"message": "ok", "data": data, "total": len(data)})