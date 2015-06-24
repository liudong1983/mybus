/***************************************************************************
 *
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 *
 **************************************************************************/

/**
 * @file redis_full_sender.cc
 * @author zhangbo6(zhangbo6@staff.sina.com.cn)
 * @date 2014/12/26 10:25:08
 * @version 1.0
 * @brief
 *
 **/
#include <unistd.h>
#include <math.h> //for abs()
#include "hbase_full_sender.h"
#include "bus_hbase_util.h"

namespace bus {
hbase_full_pool_t::~hbase_full_pool_t()
{
    while(!_queue.isEmpty())
    {
        void *data = _queue.simple_pop();
        row_t *row = static_cast<row_t*>(data);
        if (row != NULL) delete row;
    }
    pthread_mutex_destroy(&_cmutex);
}

int hbase_full_pool_t::init_connection(bus_hbase_t* conn, uint32_t statid,
        data_source_t **curdst, uint32_t &dst_partnum, bool need_connect)
{ 
    bus_stat_t& stat = g_bus.get_stat();
    bus_config_t& config = g_bus.get_config();
    pthread_t pid = pthread_self();
    std::vector<data_source_t*> &source = config.get_source();
    std::vector<data_source_t*> &dst = config.get_dst();
    uint32_t dstsize = dst.size();
    data_source_t* cursource = source[_src_partnum];
    const int src_port = cursource->get_port();
    char buffer[128];
    uint32_t dstnum = dst_partnum;

    if (stat.get_thrift_flag(statid) && dstnum != (statid % dstsize)) {
        g_logger.notice("%lx try connect default thriftserver [%d]", pid, dstnum);
        stat.reset_thrift_flag(statid);
        dstnum = statid % dstsize;
        need_connect = true;
    }

    if (!need_connect) return 0;
    while (!_stop_flag)
    {
        //info thriftserver
        *curdst = dst[dstnum];
        char *curip = const_cast<char*>((*curdst)->get_ip());
        int curport = (*curdst)->get_port();
        //设置ip+port
        g_logger.notice("try to connect with next thriftserver [%d] [%s:%d]",
                dstnum, curip, curport);

        snprintf(buffer, sizeof(buffer), "%d.%d.%lx", src_port, curport, pid);
        stat.set_sender_thread_name(statid, buffer);
        conn->set_thrift_ip_port(curip, curport);
        if (conn->connect()) {
            stat.set_consumer_thread_state(statid, THREAD_FAIL);
            g_logger.warn("init connection [%s:%d] fail",
                curip, curport);
            dstnum = (dstnum + 1) % dstsize;
            sleep(2);
            continue;
        } else {
            g_logger.notice("connect with thriftserver [%s:%d] success", curip, curport);
            stat.set_consumer_thread_state(statid, THREAD_RUNNING);
            dst_partnum = dstnum;
            break;
        }
    }

    if (_stop_flag) return -1;
    return 0;
}

uint32_t hbase_full_pool_t::_get_data(std::vector<void*> &batch_vector, MAP2VECTOR &cmd2table)
{
    int32_t rtn = _queue.pop_batch(batch_vector);
    uint32_t batchsize = batch_vector.size();
    if (-1 == rtn) {
        g_logger.notice("[%s:%d][sender=%lx], recv data NULL", 
                _ip, _port, pthread_self());
        //通知同一个sender中其它thread。
        _queue.push(NULL);
        if (0 == batchsize) return 0;
    }

    //将batch_vector中的hbase_cmd_t按table分组
    std::string table_name;
    for (std::vector<void*>::iterator it = batch_vector.begin();
            it != batch_vector.end();
            ++it) {
        row_t* hrow = static_cast<row_t*>(*it);
        if (NULL == hrow) { g_logger.debug("%lx hcmd NULL", pthread_self()); break;}
        table_name = hrow->get_cmd()->get_hbase_table();
        //g_logger.debug("row:%s %d", hcmd->get_hbase_row(), hcmd->get_hbase_cmd());
        int count = cmd2table.count(table_name);
        if (count == 0) {
            std::vector<row_t*> tmpvec;
            tmpvec.reserve(HBASE_BATCH_COUNT);
            cmd2table[table_name] = tmpvec;
        }
        cmd2table[table_name].push_back(hrow);
    }

    return batchsize;
}

int hbase_full_pool_t::_send_data(bus_hbase_t* conn, MAP2VECTOR &cmd2table, 
        int statid, data_source_t *curdst, uint32_t &processnum)
{
    bus_stat_t& stat = g_bus.get_stat();
    //循环将分组的数据发送出去
    for (MAP2VECTOR::iterator itmap = cmd2table.begin();
            itmap != cmd2table.end();
            ++itmap) {
        //g_logger.debug("%d send %s, count %d", pthread_self(), (itmap->first).c_str(), (itmap->second).size());
        uint32_t sz = itmap->second.size();
        if (0 == sz) continue;
        int batch_ret = conn->send_to_hbase(itmap->first, itmap->second, processnum);
        if (batch_ret) return batch_ret;
        //发送成功后，更新计数
        if ((itmap->second)[0]->get_src_schema()->get_record_flag())
            stat.incr_consumer_succ_count(statid, sz);
        (itmap->second).clear();
    }

    return 0;
}

void hbase_full_pool_t::process()
{
    bus_stat_t& stat = g_bus.get_stat();
    pthread_t pid = pthread_self();
    bus_config_t& config = g_bus.get_config();
    std::vector<data_source_t*> &source = config.get_source();
    std::vector<data_source_t*> &dst = config.get_dst();
    uint32_t dstsize = dst.size();
    std::vector<void*> batch_vector;
    batch_vector.reserve(HBASE_BATCH_COUNT);
    MAP2VECTOR cmd2table;
    bool need_connect = true;
    uint32_t processnum = 0; //用于记录send的位置

    char buffer[128];
    snprintf(buffer, sizeof(buffer), "%d.%d.%lx", 
            (source[_src_partnum])->get_port(),
            _port,
            pid);
    uint32_t statid = stat.get_sender_thread_id(buffer);

    g_logger.debug("down worker %lx start %d", pid, statid);
    //hbase full sender的多线程，再断线重连后，可能会使用不同的下游
    //因此这里使用curdst来指向当前使用的下游，这样每个线程可以不一样
    uint32_t dstnum = statid % dstsize;
    data_source_t *curdst = dst[dstnum];

    bus_hbase_t* conn = new (std::nothrow)bus_hbase_t(&_stop_flag);
    if (NULL == conn) oom_error();
    add_conn(conn);
    while (!_stop_flag)
    {
        if (init_connection(conn, statid, &curdst, dstnum, need_connect)) {
            //_stop_flag
            goto hbase_full_sender_stop;
        }

        //从队列中获取row, 并按照table存入到cmd2table
        //g_logger.debug("%ld batch_vecotr.size() %d", pthread_self(), batch_vector.size());
        if (batch_vector.empty() && !_get_data(batch_vector, cmd2table))
            goto hbase_full_sender_stop;

        //g_logger.debug("batch_vector:%d", batch_vector.size());
        int32_t rtn = _send_data(conn, cmd2table, statid, curdst, processnum);
        if (0 == rtn) {
            need_connect = false;
        } else if (rtn == -1) {
            g_logger.warn("full send data to [%s:%d] fatal error, stop", 
                    curdst->get_ip(), curdst->get_port());
            stat.set_consumer_thread_state(statid, THREAD_FAIL);
            goto hbase_full_sender_stop;
        } else {
            g_logger.warn("full send data to [%s:%d] failed, need reconnet",
                    curdst->get_ip(), curdst->get_port());
            need_connect = true; 
            continue;
        }

        //释放row
        clear_batch_vector(batch_vector);
    } //while
hbase_full_sender_stop:
    clear_batch_vector(batch_vector);
    add_exit_thread(pid);
    if (NULL != conn) {
        conn->disconnect(); //close on NON_OPEN xxx
        delete conn;
        conn = NULL;
    }
    if (stat.get_consumer_thread_state(statid) == THREAD_FAIL)
    {
        stat.set_consumer_thread_state(statid, THREAD_EXIT_WITH_FAIL);
    } else {
        stat.set_consumer_thread_state(statid, THREAD_EXIT);
    }
    g_logger.notice("[%s:%d][sender=%lx] exit succ",
        curdst->get_ip(), curdst->get_port(), pid);
}

/*
void clear_batch_map(MAP2VECTOR &map)
{
    for (MAP2VECTOR::iterator it = map.begin();
            it != map.end();
            ++it) {
        clear_batch_row(it->second);
    }
}
*/
} //namespace bus

