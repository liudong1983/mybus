#!/bin/env python26
import redis
import string
import sys
import logging
import logging.config
import logging.handlers
import time
import pdb
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase.THBaseService import *
from hbase.ttypes import *

_logger = logging.getLogger('buss')
hdlr = logging.handlers.RotatingFileHandler('./buss.log', maxBytes=100*1024*1024, backupCount=3)
formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(funcName)s][%(lineno)d] : %(message)s')
hdlr.setFormatter(formatter)
_logger.addHandler(hdlr)
_logger.setLevel(logging.DEBUG)

def check_pos(pos_value):
    value_list = pos_value.split(":")
    value_len = len(value_list)
    if value_len != 3:
        return False
    lrange = ord('0')
    hrange = ord('9')
    for item in  value_list:
        for char_item in item:
            v = ord(char_item)
            if v < lrange or v > hrange:
                return False
    return True
class buss:
    @staticmethod
    def get_all_bis(redis_ip, redis_port):
        r = redis.Redis(host = redis_ip, port = redis_port, db = 0)
        all_bis = None
        try:
            all_bis = r.execute_command("smembers", "all_bis")
            all_bis.sort()
        except Exception, ex:
            _logger.error("get all bis fail" % ex);
        return all_bis
    @staticmethod
    def add_bis(redis_ip, redis_port, new_bis):
        r = redis.Redis(host = redis_ip, port = redis_port, db = 0)
        ret = 0
        try:
            ret = r.execute_command("sadd", "all_bis", new_bis)
        except Exception, ex:
            _logger.error("add new bis %s fail, error: %s" % (new_bis, ex))
            return "fail"
        if ret != 1:
            _logger.error("add bis %s fail, exist", new_bis)
            return "exist"
        return "ok"
        
    @staticmethod
    def get_bus_info(bus_ip, bus_port, infomap):
        r = redis.Redis(host= bus_ip, port = string.atoi(bus_port),
                socket_timeout=0.3)
        info_list = []
        try:
            info_msg = r.execute_command("info").strip()
        except Exception, ex:
            _logger.error("bus %s:%s fail, error:%s" % (bus_ip, bus_port, ex))
            return False
        info_list = info_msg.split("\r\n")
        for item in info_list:
            value = item.split(":")
            v1 = value[0]
            v2 = value[1]
            infomap[v1] = v2
        return True
    def __init__(self, ip, port, buss):
        self.mip = ip
        self.mport = port
        self.buss = buss
        self.mbuss = buss.split("_")[0]
        self.src_type = None
        self.dst_type = None
        self.source = None;
        self.dst = None
        self.dst_score = None
        self.bus = None
        self.task = None
        self.pos = None
        self.username = None
        self.password = None
        self.pos_list = None
        self.transfer_type = None
        self.charset = None
        self.target_count = None
        self.client = redis.Redis(host = self.mip, port = self.mport, db = 0)
    def get_user_info(self):
        key = self.buss + "_user"
        try:
            user_info = self.client.execute_command("get", key)
        except Exception, ex:
            _logger.error("get user info fail, error:%s" % ex)
            return False
        if user_info is None:
            self.username = None
            self.password = None
        else:
            info_value = user_info.split(":")
            self.username = info_value[0]
            self.password = info_value[1]
        return True
    def get_transfer_type(self):
        key = self.buss + "_transfer_type"
        try:
            self.transfer_type = self.client.execute_command("get", key)
        except Exception, ex:
            _logger.error("get transfer type fail, error:%s" % ex)
            return False
        return True
    def get_charset(self):
        key = self.buss + "_charset"
        try:
            self.charset = self.client.execute_command("get", key)
        except Exception, ex:
            _logger.error("get charset type fail, error:%s" % ex)
            return False
        return True
    def get_target_count(self):
        key = self.buss + "_target_size"
        try:
            self.target_count = self.client.execute_command("get", key)
        except Exception, ex:
            _logger.error("get target count fail, error:%s" % ex)
            return False
        return True
    def is_buss_run(self):
        for item in self.bus:
            value = item.split(":")
            bus_ip = value[0]
            bus_port = value[1]
            infomap = {}
            if buss.get_bus_info(bus_ip, bus_port, infomap):
                return "bus_run"
        return "bus_stop"
    def delete_buss(self):
        try:
            for item in self.bus:
                value = item.split(":")
                bus_ip = value[0]
                bus_port = value[1]
                curkey = self.buss + "_task_" + bus_ip + "_" + bus_port
                self.client.execute_command("del",  curkey)
            for item in self.dst:
                value = item.split(":")
                dst_ip = value[0]
                dst_port = value[1]
                curkey = self.buss + "_online_" + dst_ip + "_" + dst_port
                self.client.execute_command("del", curkey)
            keylist = ['target', 'source', 'user', 'target_size', 'bus',
                    'source_type', 'transfer_type', 'charset', 'target_type']
            for item in keylist:
                curkey = self.buss + "_" + item
                self.client.execute_command("del", curkey)
            self.client.execute_command("srem", "all_bis", self.buss)
        except Expetion, ex:
            _logger.error("delete buss fail");
            return "fail"
        return "ok"
    def init_buss(self):
        if not self.get_type():
            return False
        if not self.get_target_count():
            return False
        if not self.get_source():
            return False
        if not self.get_target():
            return False
        if not self.get_bus():
            return False
        if not self.get_type():
            return False
        if not self.get_task():
            return False
        if not self.get_online():
            return False
        if not self.get_user_info():
            return False
        if not self.get_charset():
            return False
        if not self.get_transfer_type():
            return False
        return True
    def get_target(self):
        self.dst = []
        self.dst_score = []
        key = self.buss + "_target"
        try:
            dst_list = self.client.execute_command("zrangebyscore", key, "-inf",
                    "+inf", "withscores")
            list_len = len(dst_list) / 2
            for i in range(0, list_len):
                self.dst.append(dst_list[i*2])
                self.dst_score.append(dst_list[i*2+1])
        except Exception, ex:
            _logger.error("get target fail,exception: %s" % ex)
            return False
        return True
    def get_source(self):
        key = self.buss + "_source"
        try:
            self.source = self.client.smembers(key)
        except Exception, ex:
            _logger.error("get source fail, except:%s" % ex);
            return False
        self.source = list(self.source)
        self.source.sort()
        return True
    def get_target_status(self):
        dst_list = []
        ct = 0
        for item in self.dst:
            curmap = {}
            value = item.split(":")
            dst_ip = value[0]
            dst_port = value[1]
            curmap["ip"] = dst_ip
            curmap["port"] = dst_port
            curmap["partnum"] = self.dst_score[ct]
            if self.online.has_key(item):
                curmap["status"] = self.online[item]
            else:
                curmap["status"] = "unknown"
            dst_list.append(curmap)
            ct += 1
        return dst_list
    def add_task(self, cur_src, cur_bus):
        src_value = cur_src.split(":")
        srcip = src_value[0]
        srcport = src_value[1]
        if not self.delete_src_from_task(srcip, srcport):
            _logger.error("delete src %s %s fail" % (srcip, srcport))
            return False
        if cur_bus == "free":
            return True
        bus_value = cur_bus.split(":")
        busip = bus_value[0]
        busport = bus_value[1]
        key = "dev_yf_task_" + busip + "_" + busport
        try:
            r = self.client.execute_command("sadd", key, cur_src)
        except Exception, ex:
            _logger.error("add task %s %s fail, exception:%s" % (key, cur_src, ex))
            return False
        if r != 1:
            _logger.error("add task %s %s fail" % (key, cur_src))
            return False
        return True
    
    def get_src_bus_map(self):
        src_bus_map = {}
        for item in self.bus:
            ret = self.task[item]
            for src_item in ret:
                src_bus_map[src_item] = item
        return src_bus_map

    def get_source_task(self):
        src_list = []
        src_bus_map = self.get_src_bus_map()
        for item in self.source:
            src_map = {}
            value = item.split(":")
            src_ip = value[0]
            src_port = value[1]
            src_map["ip"] = src_ip
            src_map["port"] = src_port
            if src_bus_map.has_key(item):
                src_map["task"] = src_bus_map[item]
            else:
                src_map["task"] = "free"
            src_list.append(src_map)
        return src_list
    def get_type(self):
        key_src = self.buss + "_source_type"
        key_dst = self.buss + "_target_type"
        try:
            self.src_type = self.client.get(key_src)
            self.dst_type = self.client.get(key_dst)
        except Exception, ex:
            _logger.error("get source type fail, exception:%s" % ex)
            return False
        return True
    def get_bus_monitor(self, busip, busport):
        key = self.buss + "_mon_" + busip + "_" + busport;
        ret = None
        try:
            ret = self.client.execute_command("get", key)
        except Exception, ex:
            _logger.error("get bus %s %s monitor except:%s", busip, busport, ex);
            return "unknown"
        if ret is None:
            ret = "no"
        return ret
    def get_bus_status(self):
        bus_info = []
        for item in self.bus:
            curmap = {}
            value = item.split(":")
            ip = value[0]
            port = value[1]
            curmap["ip"] = ip
            curmap["port"] = port
            curmap["bus_item"] = ip + ":" + port
            infomap = {}
            if buss.get_bus_info(ip, port, infomap):
                curmap["bus_info"] = infomap["bus_status"]
                curmap["if_can_delete"] = "no"
            else:
                curmap["bus_info"] = "bus_stop"
                curmap["if_can_delete"] = "yes"
            ret = self.get_bus_monitor(ip, port)
            curmap["bus_mon"] = ret
            bus_info.append(curmap)
        return bus_info
    def set_bus_monitor(self, busip, busport, monvalue):
        key = self.buss + "_mon_" + busip + "_" + busport
        try:
           self.client.execute_command("set", key, monvalue)
        except Exception, ex:
            _logger.error("set bus %s %s monitor except:%s", busip, busport,
                    ex)
            return "fail"
        return "ok"
    def get_bus(self):
        key = self.buss + "_bus"
        try:
            self.bus = self.client.smembers(key)
        except Exception, ex:
            _logger.error("get bus fail, except:%s" % ex)
            return False
        return True

    def get_task(self):
        if self.bus is None:
            _logger.error("bus is empty");
            return False
        self.task = {}
        for item in self.bus:
            value = item.split(":")
            bus_ip = value[0]
            bus_port = value[1]
            key = self.buss + "_task_" + bus_ip + "_" + bus_port
            try:
                curtask = self.client.smembers(key)
                self.task[item] = curtask
            except Exception, ex:
                _logger.error("get bus task fail, except:%s" % ex)
                return False
        return True
    def get_online(self):
        if self.dst is None:
            _logger.error("target is empty")
            return False
        self.online = {}
        for item in self.dst:
            value = item.split(":")
            dst_ip = value[0]
            dst_port = value[1]
            key = self.buss + "_online_" + dst_ip + "_" + dst_port
            try:
                curonline = self.client.execute_command("get", key)
                self.online[item] = curonline
            except Exception, ex:
                _logger.error("get bus online fail, except:%s" % ex)
                return False
        return True
    def get_pos(self, src_port, dst_port):
        if self.dst_type == "redis":
            return self.get_redis_pos(src_port, dst_port)
        elif self.dst_type == "rediscounter":
            return self.get_rediscounter_pos(src_port, dst_port)

    def get_rediscounter_pos(self, option_src_port, option_dst_port):
        self.pos_list = []
        for item in self.dst:
            dst_item = item.split(":")
            dst_ip = dst_item[0]
            dst_port = dst_item[1]
            if option_dst_port != '0' and option_dst_port != dst_port:
                continue
            r = redis.Redis(host = dst_ip, port = string.atoi(dst_port), db = 0)
            for src in self.source:
                src_item = src.split(":")
                src_ip = src_item[0]
                src_port = src_item[1]
                if option_src_port != '0' and option_src_port != src_port:
                    continue
                key1 = self.mbuss + "_fseq_" + src_port
                key2 = self.mbuss + "_foff_" + src_port
                key3 = self.mbuss + "_row_" + src_port
                key4 = src_port + "_tm"
                v1 = None
                v2 = None
                v3 = None
                v4 = None
                try:
                    v1 = r.get(key1)
                    v2 = r.get(key2)
                    v3 = r.get(key3)
                    v4 = r.get(key4)
                except Exception, ex:
                    _logger.error("get rediscounter pos exception:%s" % ex)
                    return False
                if v1 is None or v2 is None or v3 is None:
                    _logger.error("key1=%s, key2=%s, key3=%s, key4=%s is not exist" %
                            (key1, key2, key3, key4))
                cur_pos = {}
                cur_pos["src_ip_port"] = src_ip + ":" + src_port
                cur_pos["dst_ip_port"] = dst_ip + ":" + dst_port
                cur_pos["pos_value"] = "%s:%s:%s" % (v1, v2, v3)
                if v4 is None:
                    cur_pos["delay"] = None
                else:
                    cur_pos["delay"] = int(time.time()) - string.atoi(v4)
                self.pos_list.append(cur_pos)
        return True

    def get_redis_pos(self, option_src_port, option_dst_port):
        self.pos_list = []
        for item in self.dst:
            dst_item = item.split(":")
            dst_ip = dst_item[0]
            dst_port = dst_item[1]
            if option_dst_port != '0' and option_dst_port != dst_port:
                continue
            print dst_port
            r = redis.Redis(host = dst_ip, port = string.atoi(dst_port), db = 0)
            for src in self.source:
                src_item = src.split(":")
                src_ip = src_item[0]
                src_port = src_item[1]
                if option_src_port != '0' and option_src_port != src_port:
                    continue
                key1 = self.mbuss + "_pos_" + src_port
                key4 = src_port + "_tm"
                v1 = None
                v4 = None
                try:
                    v1 = r.get(key1)
                    v4 = r.get(key4)
                except Exception, ex:
                    _logger.error("get redis pos exception:%s" % ex)
                    return False
                if v1 is None:
                    _logger.error("key=%s, is not exist" % key1)
                cur_pos = {}
                cur_pos["src_ip_port"] = src_ip + ":" + src_port
                cur_pos["dst_ip_port"] = dst_ip + ":" + dst_port
                cur_pos["pos_value"] = v1
                if v4 is None:
                    cur_pos["delay"] = None
                else:
                    cur_pos["delay"] = int(time.time()) - string.atoi(v4)
                self.pos_list.append(cur_pos)
        return True
    def set_redis_pos(self, src_ip_port, dst_ip_port, pos_value):
        dst_value = dst_ip_port.split(":")
        dstip = dst_value[0]
        dstport = dst_value[1]
        src_value = src_ip_port.split(":")
        srcip = src_value[0]
        srcport = src_value[1]
        key = self.mbuss + "_pos_" + srcport
        v1 = None
        r = redis.Redis(host = dstip, port = string.atoi(dstport), db=0)
        try:
            v1 = r.execute_command("set", key, pos_value)
        except Exception, ex:
            _logger.error("set redis pos exception:%s" % ex)
            return "fail"
        if v1 != "OK":
            _logger.error("set redis pos fail")
            return "fail"
        return "ok"
    def set_rediscounter_pos(self, src_ip_port, dst_ip_port, pos_value):
        dst_value = dst_ip_port.split(":")
        dstip = dst_value[0]
        dstport = dst_value[1]
        src_value = src_ip_port.split(":")
        srcip = src_value[0]
        srcport = src_value[1]
        value_list = pos_value.split(":")
        fseq = value_list[0]
        foff = value_list[1]
        rownum = value_list[2]
        key1 = self.mbuss + "_fseq_" + srcport
        key2 = self.mbuss + "_foff_" + srcport
        key3 = self.mbuss + "_row_" + srcport
        v1 = None
        v2 = None
        v3 = None
        r = redis.Redis(host = dstip, port = string.atoi(dstport), db=0)
        try:
            v1 = r.execute_command("set", key1, fseq)
            v2 = r.execute_command("set", key2, foff)
            v3 = r.execute_command("set", key3, rownum)
        except Exception, ex:
            _logger.error("set rediscounter pos exception:%s" % ex)
            return "fail"
        if v1 != "OK" or v2 != "OK" or v3 != "OK":
            _logger.error("set rediscounter pos fail")
            return "fail"
        return "ok"
    def set_pos(self, src_ip_port, dst_ip_port, pos_value):
        if not check_pos(pos_value):
            _logger.error("%s is invalid" % pos_value)
            return "pos %s is invalid" % pos_value
        if self.dst_type == "redis":
            return self.set_redis_pos(src_ip_port, dst_ip_port, pos_value)
        elif self.dst_type == "rediscounter":
            return self.set_rediscounter_pos(src_ip_port, dst_ip_port, pos_value)
        return "fail"
    def delete_src_from_task(self, src_ip, src_port):
        src_bus_map = self.get_src_bus_map()
        item = src_ip + ":" + src_port
        if src_bus_map.has_key(item):
            curbus = src_bus_map[item]
            value = curbus.split(":")
            busip = value[0]
            busport = value[1]
            key = self.buss + "_task_" + busip + "_" + busport
            try:
                self.client.srem(key, item)
            except Exception, ex:
                _logger.error("delete src %s %s from task %s fail" % (src_ip,
                    src_port, key))
                return False
        return True
    def delete_src(self, src_ip, src_port):
        ret = self.delete_src_from_task(src_ip, src_port)
        if not ret:
            _logger.error("delete %s %s fail" % (src_ip, src_port))
            return False
        item = src_ip + ":" + src_port
        key = self.buss + "_source"
        try:
            self.client.srem(key, item)
        except Exception, ex:
            _logger.error("delete %s %s source fail")
            return False
        return True
    def add_src(self, srcip, srcport):
        key = self.buss + "_source"
        value = srcip + ":" + srcport
        ret = "fail"
        try:
            ret = self.client.sadd(key, value)
        except Exception, ex:
            _logger.error("add src %s %s fail" % (srcip, srcport))
            return "fail"
        if ret != 1:
            _logger.error("add src %s %s exist" % (srcip, srcport))
            return "exist"
        return "ok"
    def set_dst_ct(self, target_count):
        key = self.buss + "_target_size"
        value = target_count
        try:
            self.client.set(key, value)
        except:
            _logger.error("set dst ct %s fail" % target_count)
            return "fail"
        return "ok"
    def add_dst(self, dstip, dstport, online, partnum):
        online_key = self.buss + "_online_" + dstip + "_" + dstport
        dst_key = self.buss + "_target"
        member = dstip + ":" + dstport
        ret = 0
        try:
            ret = self.client.execute_command("zadd", dst_key, partnum, member)
            if ret != 1:
                return "exist"
            self.client.execute_command("set", online_key, online)
        except Exception, ex:
            _logger.error("add dst fail:%s %s" % (dstip, dstport))
            return "fail"
        return "ok"
    def del_dst(self, dstip, dstport):
        online_key = self.buss + "_online_" + dstip + "_" + dstport
        dst_key = self.buss + "_target"
        member = dstip + ":" + dstport
        try:
            self.client.execute_command("zrem", dst_key, member)
            self.client.execute_command("del", online_key)
        except Exception, ex:
            _logger.error("del dst %s %s fail" % (dstip, dstport))
            return "fail"
        return "ok"
    def modify_dst(self, dstip, dstport, online, partnum):
        if self.del_dst(dstip, dstport) != "ok":
            _logger.error("del %s %s fail" % (dstip, dstport))
            return "fail"
        if self.add_dst(dstip, dstport, online, partnum) != "ok":
            _logger.error("add %s %s fail" % (dstip, dstport))
            return "fail"
        return "ok"
    def add_bus(self, ip, port):
        key = self.buss + "_bus"
        value = ip + ":" + port
        _logger.error("add bus %s %s fail", ip, port)
        ret = 0
        try:
            ret = self.client.execute_command("sadd", key, value)
        except Exception, ex:
            _logger.error("add bus %s %s fail", ip, port)
            return "fail"
        if ret != 1:
            _logger.error("bus %s %s exist", ip, port)
            return "exist"
        return "ok"
    def del_bus(self, ip, port):
        key = self.buss + "_bus"
        member = ip + ":" + port
        try:
            self.client.execute_command("srem", key, member)
        except Exception, ex:
            _logger.error("del  bus %s %s fail", ip, port)
            return "fail"
        return "ok"
    def update_user(self, user, password):
        key = self.buss + "_user"
        value = user + ":" + password
        try:
            self.client.execute_command("set", key, value)
        except Exception, ex:
            _logger.error("set user info %s fail", value)
            return "fail"
        return "ok"
    def update_transfer_type(self, transfer_type):
        key = self.buss + "_transfer_type"
        try:
            self.client.execute_command("set", key, transfer_type)
        except Exception, ex:
            _logger.error("set transfer type %s fail", transfer_type)
            return "fail"
        return "ok"
    def update_charset(self, charset):
        key = self.buss + "_charset"
        try:
            self.client.execute_command("set", key, charset)
        except Exception, ex:
            _logger.error("set charset %s fail", charset)
            return "fail"
        return "ok"
    def update_storage_type(self, src_type, dst_type):
        key1 = self.buss + "_source_type"
        key2 = self.buss + "_target_type"
        try:
            self.client.execute_command("set", key1, src_type)
            self.client.execute_command("set", key2, dst_type)
        except Exception, ex:
            _logger.error("set storage type fail")
            return "fail"
        return "ok"
    def start_bus(self, bus_ip, bus_port):
        r = redis.Redis(host=bus_ip, port=string.atoi(bus_port))
        ret = None
        try:
            ret = r.execute_command("start", "trans")
        except Exception, ex:
            _logger.error("bus %s % start trans fail, error:%s", bus_ip,
                    bus_port, ex)
            return "bus %s %s start trans fail, error:%s" % (bus_ip, bus_port, ex)
        return "ok"
    def stop_bus(self, bus_ip, bus_port):
        r = redis.Redis(host=bus_ip, port=string.atoi(bus_port))
        ret = None
        try:
            ret = r.execute_command("stop", "trans")
        except Exception, ex:
            _logger.error("bus %s % stop trans fail, error:%s", bus_ip,
                    bus_port, ex)
            return "bus %s %s stop trans fail, error:%s" % (bus_ip, bus_port, ex)
        return "ok"
    def bus_info(self, bus_ip, bus_port):
        r = redis.Redis(host=bus_ip, port=string.atoi(bus_port))
        ret = None
        try:
            ret = r.execute_command("info")
        except Exception, ex:
            _logger.error("bus %s % info fail, error:%s", bus_ip,
                    bus_port, ex)
            return "bus %s %s info fail, error:%s" % (bus_ip, bus_port, ex)
        ret = ret.replace("\r", "")
        return ret
    def get_alert_data(self, bus_ip, bus_port):
        key = self.buss + "_alert_" + bus_ip + "_" + bus_port
        alert_data = {}
        try:
            ret = self.client.execute_command("hgetall", key)
        except Exception, ex:
            _logger.error("get bus %s %s alert data fail", bus_ip, bus_port, ex)
            return None
        dlen = len(ret) / 2
        for idx in xrange(dlen):
            hkey = ret[idx * 2]
            hval = ret[idx *2 + 1]
            alert_data[hkey] = string.atoi(hval)
        return alert_data
    def set_alert_data(self, bus_ip, bus_port, alert_data):
        key = self.buss + "_alert_" + bus_ip + "_" + bus_port
        try:
            self.client.execute_command("del", key)
            for k, v in alert_data.iteritems():
                self.client.execute_command("hset", key, k, v)
        except Exception, ex:
            _logger.error("set bus %s %s alert data fail", bus_ip, bus_port, ex)
            return "fail"
        return "succ"

if __name__ == "__main__":
    mybuss = buss("10.73.11.21", 8866, "dev_yf")
    ret = mybuss.init_buss()
    print ret
    ret = mybuss.set_pos("10.73.11.21:5008", "10.73.11.21:8000", "1:107:0")
    print ret
    '''
    if  ret == "bus_stop":
        myret = mybuss.delete_buss()
        print myret
    all_bis = buss.get_all_bis("10.73.11.21", 8866)
    print all_bis
    mybuss = buss("10.73.11.21", 8866, "dev_yf")
    if not mybuss.get_type():
        print "get type fail"
        sys.exit(2)
    print "type=%s" % mybuss.dst_type
    if mybuss.dst_type == "rediscounter":
        if mybuss.get_rediscounter_pos():
            print mybuss.pos_list
        else:
            print "get rediscounter pos fail"
    else:
        if mybuss.get_redis_pos():
            print mybuss.pos_list
        else:
            print "get redis pos fail"
    '''


