#!/usr/bin/env python26
import redis
import MySQLdb
import logging
import logging.config
import string
from buss import buss
from alert import *
import time
import pdb
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase.THBaseService import *
from hbase.ttypes import *

mylogger = logging.getLogger('app')
myhdlr = logging.handlers.RotatingFileHandler('./app.log', maxBytes=100*1024*1024, backupCount=3)
myformatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s][%(lineno)d] : %(message)s')
myhdlr.setFormatter(myformatter)
mylogger.addHandler(myhdlr)
mylogger.setLevel(logging.DEBUG)
def send_databus_msg(msg, retry_times=3):
    service = "databus"
    alert_object = "online"
    subject = msg
    content = msg
    msgto = "new_nosql"
    mailto = "new_nosql"
    retry_ct = 0
    try:
        while retry_ct < retry_times:
            ret = send_alert(service=service, object=alert_object, subject=subject, content=content, gmsgto=msgto, gmailto=mailto)
            if ret == "1":
                mylogger.info("send succ: %s", msg)
                return True
            mylogger.error("send fail,msg=%s retry:%d", msg, retry_ct)
            retry_ct += 1
    except Exception as e:
        mylogger.error('send fail,msg=%s error:%s', msg, e)
        return False
    return False

def get_db_tm(bisname, dbip, dbport, dbuser, dbpass, dbcharset):
    now = int(time.time())
    cursor = None
    try:
        conn = MySQLdb.connect(host = dbip, port = string.atoi(dbport), user = dbuser, passwd = dbpass, charset = dbcharset)
        cursor = conn.cursor();
        sql = "select time from zjmdmm.heart_beat order by id desc limit 1"
        ret = cursor.execute(sql)
        data = cursor.fetchall()
        cursor.close()
    except Exception, ex:
        if cursor is not None:
            cursor.close()
        msg = "[%s] get db [%s:%s] tm except:%s" % (bisname, dbip, dbport, ex)
        return [5, msg]
    if ret != 1:
        msg = "[%s] get mysql [%s:%s] tm fail:%s" % (bisname, dbip, dbport, data)
        return [9, msg]
    dbtm = string.atoi(data[0][0])
    if now - dbtm > 300:
        msg = "[%s] up [%s:%s] stop update heart beat" % (bisname, dbip, dbport)
        return [7, msg]
    return [0, dbtm]
def get_dst_tm(bisname, dst_type, rip, rport, source_port):
    if dst_type == "redis":
        return get_redis_tm(bisname, rip, rport, source_port)
    if dst_type == "rediscounter":
        return get_redis_tm(bisname, rip, rport, source_port)
    elif dst_type == "hbase":
        return get_hbase_tm(bisname, rip, rport, source_port)
    else:
        return [10, "dst_type is not exist"]

def get_redis_tm(bisname, rip, rport, source_port):
    conn = redis.Redis(host = rip, port = string.atoi(rport))
    ret = None
    key = source_port + "_tm"
    try:
        ret = conn.execute_command("get", key)
    except Exception, ex:
        msg = "[%s] get redis [%s:%s] tm fail, except:%s" % (bisname, rip, rport, ex)
        return [6, msg]
    if ret is None:
        msg = "[%s] redis %s %s tm not exist" % (bisname, rip, rport)
        return [8, msg]
    rtm = string.atoi(ret)
    return [0, rtm]

def hbase_read(dst_ip, dst_port, cond):
    transport = TSocket.TSocket(dst_ip, string.atoi(dst_port))
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Client(protocol)
    transport.open()
    ret = client.get('databus', cond)
    transport.close()
    if ret.row is None:
        return None
    return ret.columnValues[0].value
def get_hbase_tm(bisname, rip, rport,  source_port):
    tm_rowkey = "%s_tm_%s" % (bisname, source_port)
    tm_col = TColumn('bus', 'tm')
    tm_cond = TGet(tm_rowkey, [tm_col])
    tm_value = None
    try:
        tm_value = hbase_read(rip, rport, tm_cond)
    except Exception, ex:
        msg = "[%s] get hbase [%s:%s] tm fail, except:%s" % (bisname, rip, rport, ex)
        return [6, msg]
    if tm_value is None:
        msg = "[%s] hbase %s %s tm not exist" % (bisname, rip, rport)
        return [8, msg]
    rtm = string.atoi(tm_value)
    return [0,  rtm]

def control_alert(ct, msg):
    alert_ct = [10, 120]
    if ct in alert_ct:
        mylogger.error("send msg:%d,%s", msg)
        send_databus_msg(msg)
    elif ct % 240 == 0:
        mylogger.error("send msg:%d, %s", msg)
        send_databus_msg(msg)

sleep_sec = 30
########set your redis ip/port
redis_ip = "127.0.0.1"
redis_port = 8866
if __name__ == "__main__":
    while True:
        allbis = buss.get_all_bis(redis_ip, redis_port)
        for bis in allbis:
            curbis = buss(redis_ip, redis_port, bis)
            config_ct = 0
            while not curbis.init_buss():
                montype = 4
                config_ct += 1
                msg = "[configserver-%s:%s:%s][init fail]" % (redis_ip, redis_port, curbis.buss)
                mylogger.error(msg)
                control_alert(config_ct, msg)
                time.sleep(sleep_sec)
                continue
            bus_list = curbis.get_bus_status()
            dst_type = curbis.dst_type
            for bus_item in bus_list:
                busip = bus_item["ip"]
                busport = bus_item["port"]
                bus_mon = bus_item["bus_mon"]
                bus_stat = bus_item["bus_info"]
                if bus_mon == "no":
                    continue
                old_alert_data = curbis.get_alert_data(busip, busport)
                alert_data = {}
                if bus_stat == "TRAN_FAIL" or bus_stat == "bus_stop":
                    montype = 1
                    ct = 1
                    msg = "[%s][%s][%s]" % (curbis.buss, bus_item["bus_item"], bus_stat)
                    mylogger.error(msg)
                    if old_alert_data.has_key("bus_status"):
                        ct += old_alert_data["bus_status"]
                    alert_data["bus_status"] = ct
                    curbis.set_alert_data(busip, busport, alert_data)
                    control_alert(ct, msg)
                    continue
                bus_ip_port = bus_item["bus_item"]
                src_list = curbis.task[bus_ip_port]
                target_list = curbis.get_target_status()
                for src_item in src_list:
                    value_list = src_item.split(":")
                    srcip = value_list[0]
                    srcport = value_list[1]
                    retcode, dbtm = get_db_tm(curbis.buss, srcip, srcport, curbis.username, curbis.password, "utf8")
                    if retcode != 0:
                        msg = dbtm
                        montype = retcode
                        ct = 1
                        mylogger.error(msg)
                        if old_alert_data.has_key(src_item):
                            ct += old_alert_data[src_item]
                        alert_data[src_item] = ct
                        control_alert(ct, msg)
                        continue
                    time.sleep(1)
                    for dst_item in target_list:
                        if dst_item["status"] != "yes":
                            continue
                        dstip = dst_item["ip"]
                        dstport = dst_item["port"]
                        dstitem = dstip + ":" + dstport
                        retcode, rtm = get_dst_tm(curbis.mbuss, curbis.dst_type, dstip, dstport, srcport)
                        if retcode != 0:
                            ct = 1
                            msg = rtm
                            montype = retcode
                            pdb.set_trace()
                            mylogger.error(msg)
                            if old_alert_data.has_key(dstitem):
                                ct += old_alert_data[dstitem]
                            alert_data[dstitem] = ct
                            control_alert(ct, msg)
                            continue
                        if dbtm > rtm:
                            alert_key = srcport + ":" + dstport
                            msg = "[%s] up:[%s] down:[%s] delay" % (curbis.buss, src_item, dstitem)
                            ct = 1
                            montype = 3
                            if old_alert_data.has_key(alert_key):
                                ct += old_alert_data[alert_key]
                            alert_data[alert_key] = ct
                            control_alert(ct, msg)
                            mylogger.error(msg)
                        if curbis.dst_type == "hbase":
                            break;
                curbis.set_alert_data(busip, busport, alert_data)
        time.sleep(sleep_sec)
