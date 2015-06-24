#!/usr/bin/env python26
from django.http import Http404, HttpResponse
from django.template import Template, Context
from django.template.loader import get_template
from django.shortcuts import render_to_response
from django.template import RequestContext
from django.utils.datastructures import SortedDict
import logging
import traceback
import urllib2
import redis
import datetime
from buss import buss
import json
import pdb
import salt.client
from django.views.decorators.csrf import csrf_exempt
logger = logging.getLogger('django')
##set your redis ip/port
redis_ip="127.0.0.1"
redis_port=8866

def index(request):
    now = datetime.datetime.now()
    return render_to_response('business/index.html', {'current_date': now})
def get_bus_business(request):
    all_bis = buss.get_all_bis(redis_ip, redis_port)
    return_dir = {'business_result_data': all_bis}  
    return render_to_response("business/index.html",
            return_dir,
            context_instance = RequestContext(request))
def add_bus_business(request):
    new_bis = None
    if request.GET:
        new_bis = request.GET['business'].strip()
    if new_bis is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    if new_bis.find("_") == -1:
        logger.error("new bis %s name invalid", new_bis)
        return HttpResponse("invalid")
    ret = buss.add_bis(redis_ip, redis_port, new_bis)
    return HttpResponse(ret) 
def delete_bus_business(request):
    bis = None
    if request.GET:
        bis = request.GET['business'].strip()
    if bis is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, bis)
    if not mybuss.init_buss():
        logger.error("init buss:%s fail" % (bis))
        return HttpResponse("fail")
    ret = mybuss.is_buss_run()
    if ret == "bus_run":
        return HttpResponse("run")
    ret = mybuss.delete_buss()
    return HttpResponse(ret)
def one_business(request, business):
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init buss:%s fail" % (business))
        return
    src_list = mybuss.get_source_task()
    dst_list = mybuss.get_target_status()
    bus_list = mybuss.get_bus_status()
    user_data = {"username" : mybuss.username, "password" : mybuss.password}
    all_pos_list = []
    all_schema = mybuss.get_schema()
    return_dir = {'business_src_data':src_list,
            'business_target_data':dst_list,
            'business_bus_data': bus_list,
            "user_data":user_data,
            "bus_business":business,
            "transfer_type": mybuss.transfer_type,
            "all_pos_list":all_pos_list,
            "charset_type": mybuss.charset,
            "target_count":mybuss.target_count,
            "source_type": mybuss.src_type,
            "target_type": mybuss.dst_type,
            "all_schema":mybuss.schema,
            "snap_check": mybuss.snap_check,
            "hbase_thread_num" : mybuss.hbase_full_thread_num}  
    return render_to_response("business/one_business.html", return_dir,
            context_instance=RequestContext(request))

def show_pos(request,business): 
    old_business=business
    src = '0'
    dist = '0'
    if request.GET:
        dist = request.GET['dist'].strip()
        src = request.GET['src'].strip()
    mybuss = buss(redis_ip, redis_port, old_business)
    if not mybuss.init_buss():
        logger.error("init buss fail")
        return
    src_result_list = mybuss.get_source_task()
    dst_target_list = mybuss.get_target_status()
    ret = mybuss.get_pos(src, dist)
    return_dir = {'business_src_data' : src_result_list,
            'business_target_data' : dst_target_list,
            "bus_business" : mybuss.mbuss,
            "old_business" : mybuss.buss,
            "all_pos_list" : mybuss.pos_list,
            "dist" : dist,
            "src" : src} 
    return render_to_response("business/pos.html", 
            return_dir, 
            context_instance=RequestContext(request))
def delete_src(request):
    ip = None
    port = None
    business = None
    if request.GET:
        ip = request.GET['ip'].strip()
        port = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if ip is None or port is None or business is None:
        logger.error("parm is None, invalid")
        return HttpResponse("parm invalid")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init buss %s fail" % (business))
        return HttpResponse("init buss fail")
    if not mybuss.delete_src(ip, port):
        logger.error("delete src %s %s fail" % (ip, port))
        return HttpResponse("delete src %s %s fail" % (ip, port))
    return HttpResponse("ok")
#add up source
def add_src(request):
    ip = None
    start_port = None
    end_port = None
    business = None
    if request.GET:
        ip = request.GET['ip'].strip();
        start_port = request.GET['start_port'].strip()
        end_port = request.GET['end_port'].strip()
        business = request.GET['business'].strip()
    if ip is None or start_port is None or end_port is None or business is None:
        logger.error("parm is None, invalid")
        return HttpResponse("parm invalid")
    start_port_num = int(start_port)
    end_port_num = int(end_port)
    if end_port_num != 0 and end_port_num < start_port_num:
        return HttpResponse("start port:%d>end_port:%d" % (start_port_num, end_port_num))
    mybuss = buss(redis_ip, redis_port, business)
    if end_port_num == 0:
        ret = mybuss.add_src(ip, start_port)
        return HttpResponse(ret)
    for port in xrange(start_port_num, end_port_num + 1):
        ret = mybuss.add_src(ip, str(port))
        if ret != "ok":
            return HttpResponse(ret)
    return HttpResponse("ok")

#modify source bus configuration
def add_task_src(request):
    if request.GET:
        cur_src = request.GET['src'].strip()
        cur_bus = request.GET['bus'].strip()
        cur_buss = request.GET['business'].strip()
    mybuss = buss(redis_ip, redis_port, cur_buss)
    if not mybuss.init_buss():
        logger.error("init buss fail")
        return HttpResponse("init buss fail")
    ret = mybuss.add_task(cur_src, cur_bus)
    if  not ret:
        logger.error("add task fail")
        return HttpResponse("add task fail")
    return HttpResponse("ok")
def set_target_count(request):
    business = None
    target_count = None
    if request.GET:
       business = request.GET['business'].strip()
       target_count = request.GET['target_count'].strip()
    if business is None or target_count is None:
        return HttpResponse("fail")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.set_dst_ct(target_count)
    return HttpResponse(ret)
def delete_target(request):
    ip = None
    port = None
    business = None
    if request.GET:
        ip = request.GET['ip'].strip()
        port = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if ip is None or port is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.del_dst(ip, port)
    return HttpResponse(ret)
def add_target(request):
    ip = None
    start_port = None
    end_port = None
    part_num = None
    online = None
    business = None
    if request.GET:
        ip = request.GET['ip'].strip()
        start_port = request.GET['start_port'].strip()
        end_port = request.GET['end_port'].strip()
        part_num = int(request.GET['part_num'].strip())
        online = request.GET['online'].strip()
        business = request.GET['business'].strip()
    if ip is None or start_port is None or end_port is None or part_num is None or online is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invliad parm")
    start_port_num = int(start_port)
    end_port_num = int(end_port)
    part_num_num = int(part_num)
    if end_port_num != 0 and start_port_num > end_port_num:
        return HttpResponse("start port:%s>end port:%s" % (start_port, end_port))
    mybuss = buss(redis_ip, redis_port, business)
    if end_port_num == 0:
        ret = mybuss.add_dst(ip, start_port, online, str(part_num_num))
        return HttpResponse(ret)
    for port in xrange(start_port_num, end_port_num + 1):
        ret = mybuss.add_dst(ip, str(port), online, str(part_num_num))
        if ret != "ok":
            return HttpResponse(ret)
        part_num_num += 1
    return HttpResponse("ok")
        
def update_target(request):
    ip = None
    port = None
    partnum = None
    online = None
    business = None
    if request.GET:
        ip = request.GET['ip'].strip()
        port = request.GET['port'].strip()
        partnum = request.GET['partnum'].strip()
        online = request.GET['online'].strip()
        business = request.GET['business'].strip()
    if ip is None or port is None or partnum is None or online is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.modify_dst(ip, port, online, partnum)
    return HttpResponse(ret)
def add_bus(request):
    ip = None
    port = None
    business = None
    if request.GET:
        ip = request.GET['ip'].strip()
        port = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if ip is None or port is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.add_bus(ip, port)
    return HttpResponse(ret)
def delete_bus(request):
    ip = None
    port = None
    business = None
    if request.GET:
        ip = request.GET['ip'].strip()
        port = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if ip is None or port is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.del_bus(ip, port)
    return HttpResponse(ret)
def modify_bus_user(request):
    user = None
    password = None
    business = None
    if request.GET:
        user = request.GET['user'].strip()
        password = request.GET['pass'].strip()
        business = request.GET['business'].strip()
    if user is None or password is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.update_user(user, password)
    return HttpResponse(ret)
    
def modify_transfer_type(request):
    transfer_type = None
    business = None
    if request.GET:
        transfer_type = request.GET['transfer_type'].strip()
        business = request.GET['business'].strip()
    if transfer_type is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    if transfer_type != "incr" and transfer_type != "full":
        logger.error("transfer_type %s invalid", transfer_type);
        return HttpResponse("transfer_type %s invalid" % transfer_type)
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.update_transfer_type(transfer_type)
    return HttpResponse(ret)
charset_list = ["utf8", "gbk", "latin1"]
def modify_charset_type(request):
    charset_type = None
    business = None
    if request.GET:
        charset_type = request.GET['charset_type'].strip()
        business = request.GET['business'].strip()
    if charset_type is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    if charset_type not in charset_list:
        logger.error("charset %s invalid", charset_type)
        return HttpResponse("charset %s invalid" % charset_type)
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.update_charset(charset_type)
    return HttpResponse(ret)
storage_list = ["mysql", "redis", "rediscounter", "hbase"]
def modify_source_target_type(request):
    source_type = None
    target_type = None
    business = None
    if request.GET:
        source_type = request.GET['source_type'].strip()
        target_type = request.GET['target_type'].strip()
        business = request.GET['business'].strip()
    if source_type is None or target_type is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    if source_type not in storage_list or target_type not in storage_list:
        logger.error("invalid type")
        return HttpResponse("invalid type")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.update_storage_type(source_type, target_type)
    return HttpResponse(ret)
def view_log(request):
    busip = None
    busport = None
    business = None
    if request.GET:
        busip = request.GET['ip'].strip()
        busport = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if busip is None or busport is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    local = salt.client.LocalClient()
    logfile = "/data1/databus%s/bus%s.log" % (busport, busport)
    view_cmd = "tail -n10 %s" % (logfile)
    ret = local.cmd(busip, "cmd.run", [view_cmd])
    return HttpResponse(ret[busip])
def bus_start(request):
    busip = None
    busport = None
    business = None
    if request.GET:
        busip = request.GET['ip'].strip()
        busport = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if busip is None or busport is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    local = salt.client.LocalClient()
    start_cmd = "/usr/local/databus/bus -x %s -p %s -m %s:%d" % (business, busport, redis_ip, redis_port)
    ret = local.cmd(busip, "cmd.run_all", [start_cmd]);
    reply_info = ret[busip]
    status_code = reply_info["retcode"]
    stdout_info = reply_info["stdout"]
    replystr = "ok"
    if status_code != 0:
        if stdout_info != "":
            replystr = stdout_info
        else:
            replystr = "fail"
    return HttpResponse(replystr)
def bus_stop(request):
    busip = None
    busport = None
    business = None
    if request.GET:
        busip = request.GET['ip'].strip()
        busport = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if busip is None or busport is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    ret = buss.stop_bus(business, busip, busport)
    return HttpResponse(ret)
def start_tran(request):
    busip = None
    busport = None
    business = None
    if request.GET:
        busip = request.GET['ip'].strip()
        busport = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if busip is None or busport is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.start_tran(busip, busport)
    return HttpResponse(ret)
def stop_tran(request):
    busip = None
    busport = None
    business = None
    if request.GET:
        busip = request.GET['ip'].strip()
        busport = request.GET['port'].strip()
        business = request.GET['business'].strip()
    if busip is None or busport is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.stop_tran(busip, busport)
    return HttpResponse(ret)
def bus_info(request):
    busip = None
    busport = None
    business = None
    cmd = None
    if request.GET:
        busip = request.GET['ip'].strip()
        busport = request.GET['port'].strip()
        business = request.GET['business'].strip()
        cmd = request.GET['cmd'].strip()
    if busip is None or busport is None or business is None or cmd is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.bus_info(busip, busport, cmd)
    return HttpResponse(ret)

def modify_pos(request):
    src_ip_port = None
    dst_ip_port = None
    pos_value = None
    business = None
    if request.GET:
        src_ip_port = request.GET['src_ip_port'].strip()
        dst_ip_port = request.GET['dst_ip_port'].strip()
        pos_value = request.GET['pos_value'].strip()
        business = request.GET['business'].strip()
    if src_ip_port is None or dst_ip_port is None or pos_value is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init buss fail")
        return HttpResponse("fail")
    ret = mybuss.set_pos(src_ip_port, dst_ip_port, pos_value)
    return HttpResponse(ret)
def modify_bus_mon(request):
    bus_ip = None
    bus_port = None
    business = None
    monvalue = None
    if request.GET:
        bus_ip = request.GET['bus_ip'].strip()
        bus_port = request.GET['bus_port'].strip()
        business = request.GET['business'].strip()
        monvalue = request.GET['monvalue'].strip()
    if bus_ip is None or bus_port is None or business is None or monvalue is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    ret = mybuss.set_bus_monitor(bus_ip, bus_port, monvalue)
    return HttpResponse(ret)
def modify_all_pos(request):
    src_ip = None
    src_port = None
    pos_value = None
    business = None
    if request.GET:
        src_ip = request.GET['src_ip'].strip()
        src_port = request.GET['src_port'].strip()
        pos_value = request.GET['pos_value'].strip()
        business = request.GET['business'].strip()
    if src_ip is None or src_port is None or pos_value is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init buss fail")
        return HttpResponse("fail")
    ret = mybuss.set_all_pos(src_ip, src_port, pos_value)
    return HttpResponse(ret)
@csrf_exempt
def add_schema(request):
    business = None
    tbid = None
    dbname = None
    tbname = None
    column = None
    stat = None
    if request.POST:
        tbid = request.POST.get("tbid").strip()
        dbname = request.POST.get("dbname").strip()
        tbname = request.POST.get("tbname").strip()
        column = request.POST.get("column").strip()
        stat = request.POST.get("stat").strip()
        business = request.POST.get("business").strip()
    if tbid is None or dbname is None or business is None or tbname is None or column is None or stat is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init busss fail")
        return HttpResponse("fail")
    ret = mybuss.add_schema(tbid, dbname, tbname, stat, column);
    return HttpResponse(json.dumps({"result": ret}), mimetype='application/json')
@csrf_exempt
def update_schema(request):
    business = None
    tbid = None
    dbname = None
    tbname = None
    column = None
    stat = None
    if request.POST:
        tbid = request.POST.get("tbid").strip()
        dbname = request.POST.get("dbname").strip()
        tbname = request.POST.get("tbname").strip()
        column = request.POST.get("column").strip()
        stat = request.POST.get("stat").strip()
        business = request.POST.get("business").strip()
    if tbid is None or dbname is None or business is None or tbname is None or column is None or stat is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init busss fail")
        return HttpResponse("fail")
    ret = mybuss.update_schema(tbid, dbname, tbname, stat, column);
    logger.error(ret);
    return HttpResponse(json.dumps({"result": ret}), mimetype='application/json')
def remove_schema(request):
    business = None
    tbid = None
    if request.GET:
        tbid = request.GET['tbid'].strip()
        business = request.GET['business'].strip()
    if tbid is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init busss fail")
        return HttpResponse("fail")
    ret = mybuss.remove_schema(tbid);
    return HttpResponse(ret)
def modify_check_snapshot(request):
    business = None
    snap_check = None
    if request.GET:
        snap_check = request.GET['snap_check'].strip()
        business = request.GET['business'].strip()
    if snap_check is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init busss fail")
        return HttpResponse("fail")
    ret = mybuss.modify_check_snapshot(snap_check);
    return HttpResponse(ret)
def modify_hbase_full_thread_num(request):
    business = None
    thread_num = None
    if request.GET:
        thread_num = request.GET['thread_num'].strip()
        business = request.GET['business'].strip()
    if  thread_num is None or business is None:
        logger.error("invalid parm")
        return HttpResponse("invalid parm")
    mybuss = buss(redis_ip, redis_port, business)
    if not mybuss.init_buss():
        logger.error("init busss fail")
        return HttpResponse("fail")
    ret = mybuss.modify_hbase_full_thread_num(thread_num);
    return HttpResponse(ret)
    
        

