/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file mysql_full_extract.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/08 14:34:30
 * @version $Revision$
 * @brief 
 *  
 **/
#include "mysql_full_extract.h"
#include "bus_wrap.h"
#include "bus_hbase_util.h"

namespace bus {
bool mysql_full_extract_t::init()
{
    //获取全量抽取位置
    bus_config_t &config = g_bus.get_config();
    std::vector<data_source_t*> &src_source = config.get_source();
    std::vector<data_source_t*> &dst_source = config.get_dst();
    source_t dst_type = config.get_dstds_type();

    /* 获取所有数据源的位置信息 */
    for (std::vector<data_source_t*>::iterator it = src_source.begin();
            it != src_source.end();
            ++it)
    {
        data_source_t *ds = *it;
        const char *ip = ds->get_ip();
        const int port = ds->get_port();
        const char *user_name = config.get_user_name();
        const char *user_pwd = config.get_user_pwd();
        const char *charset = config.get_charset();

        bus_mysql_t conn(ip, port, user_name, user_pwd);
        if (!conn.init(charset))
        {
            g_logger.error("init %s:%d mysql connection fail",
                    ip, port);
            return false;
        }
        if (!conn.get_full_transfer_position(*ds))
        {
            g_logger.error("get %s:%d, mysql position fail",
                    ip, port);
            return false;
        }
    }

    if (dst_type == HBASE_DS) {
        bus_config_t &config = g_bus.get_config();
        std::map<std::string, std::string> posmap;
        if (!config.get_position_map(posmap)) {
            g_logger.debug("get position map fail");
            return false;
        }
        
        //对于HBASE_DS来说，下游只有一个HBase，那么只要通过任一thriftserver设置成功后，
        //就可以break；但是通过每个thriftserver设置一次，这样不影响hbase中的结果，但是可以
        //用来在start trans时保证每个thriftserver都是OK的。
        bool set_pos = false;
        for (std::vector<data_source_t*>::iterator it = dst_source.begin();
            it != dst_source.end(); ++it)
        {   
            data_source_t *ds = *it;
            const char *ip = ds->get_ip();
            const int port = ds->get_port();
            if (!set_hbase_position(ip, port, posmap)) {
                g_logger.debug("set position to hbase by [%s:%d] succcess", ip, port);
                set_pos = true;
                continue;
            } else {
                g_logger.error("can not set position to hbase thriftserver [%s:%d]", ip, port);
                return false;
            }
        }
    }
    
    if (dst_type == REDIS_DS || dst_type == REDIS_COUNTER_DS)
    {
        /* 对每个下游设定同步点的信息 */
        for (std::vector<data_source_t*>::iterator it = dst_source.begin();
                it != dst_source.end();
                ++it)
        {
            data_source_t *ds = *it;
            const char *ip = ds->get_ip();
            const int port = ds->get_port();

            bus_master_t rconn(ip, port);
            if (!rconn.init())
            {
                g_logger.error("init %s:%d redis connection fail",
                        ip, port);
                return false;
            }
            if (dst_type == REDIS_DS) {
                if (!rconn.set_redis_full_position()) {
                    g_logger.error("set %s:%d redis position fail", ip, port);
                    return false;
                }
            } else if (dst_type == REDIS_COUNTER_DS) {
                if (!rconn.set_rediscounter_full_position()) {
                    g_logger.error("set %s:%d redis position fail", ip, port);
                    return false;
                }
            }
        }
    }

    if(!thread_pool_t::init())
    {
        g_logger.error("init threadpool fail");
        return false;
    }

    return true;
}

bool mysql_full_extract_t::work(void *item, uint32_t statid)
{
    bus_job_t *curjob = static_cast<bus_job_t*>(item);
    //获取ip/port
    data_source_t *source = curjob->get_data_source();
    const char *ip = source->get_ip();
    const int port = source->get_port();
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "%d", port);
    //获取user/pwd
    bus_config_t &config = g_bus.get_config();
    const char *user_name = config.get_user_name();
    const char *user_pwd = config.get_user_pwd();
    const char *charset = config.get_charset();
    bus_stat_t &stat = g_bus.get_stat();
    stat.set_producer_thread_source(statid, buffer);
    //初始化mysql链接
    bus_mysql_t conn(ip, port, user_name, user_pwd);
    //开始进行全量抽取
    if (!conn.get_full_data(*curjob, _stop_flag, charset, statid))
    {
        g_logger.error("[extractor=%s:%d] datasource fail", ip, port);
        stat.set_producer_thread_state(statid, THREAD_FAIL);
        delete curjob;
        return false;
    }

    //检查mysql位置
    while(!_stop_flag && !conn.check_full_position(*source))
    {
        g_logger.error("[extractor=%s:%d] check position fail", ip, port);
        stat.set_producer_thread_state(statid, THREAD_FAIL);
        sleep(SLEEP_TIME);
        conn.init(charset);
        continue;
    }
    stat.set_producer_thread_state(statid, THREAD_RUNNING);
    delete curjob;
    return true;
}

void mysql_full_extract_t::process()
{
    pthread_t tid = pthread_self();
    g_logger.notice("[extractor=%lx] start", tid);
    bus_stat_t &stat = g_bus.get_stat();
    uint32_t i = 0;
    uint32_t statid = stat.get_extract_thread_id("unknown");

    while (!_stop_flag)
    {
        void *data = _queue.pop();
        /* 处理当前job */
        if (data == NULL)
        {
            g_logger.notice("[extractor=%lx] recv NULL signal", tid);
            g_bus.set_transfer_stat(FULL_TRAN_WAIT_PRODUCER_EXIT);
            this->add(NULL);
            goto thread_exit;
        } else {
            g_logger.notice("[extractor=%lx] begin %d job", tid, i++);
            if(!work(data, statid))
            {
                goto thread_exit;
            }
        }
    }
thread_exit:
    this->add_exit_thread(tid);
    if (stat.get_producer_thread_state(statid) == THREAD_FAIL) {
        stat.set_producer_thread_state(statid, THREAD_EXIT_WITH_FAIL);
    } else {
        stat.set_producer_thread_state(statid, THREAD_EXIT);
    }
    mysql_thread_end();
    g_logger.notice("[extractor=%lx] exit succ", tid);
}

mysql_full_extract_t::~mysql_full_extract_t()
{
    while(!_queue.isEmpty())
    {
        void *data = _queue.simple_pop();
        bus_job_t *job = static_cast<bus_job_t*>(data);
        if (job != NULL) delete job;
    }
}


}//namespace bus

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
