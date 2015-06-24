/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file mysql_incr_extract.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/23 12:05:07
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_event.h"
#include "bus_engine.h"
#include "bus_config.h"
#include "bus_wrap.h"
#include "mysql_incr_extract.h"

namespace bus {
bool mysql_incr_extract_t::init()
{
    bus_config_t &config = g_bus.get_config();
    if (!config.init_charset_table())
    {
        g_logger.error("init charset table fail");
        return false;
    }
    if (!thread_pool_t::init())
    {
        g_logger.error("init thread pool fail");
        return false;
    }
    return true;
}
void mysql_incr_extract_t::shutdown()
{
    thread_pool_t::shutdown();
    clean_conn();
}
void mysql_incr_extract_t::process()
{
    pthread_t tid = pthread_self();
    g_logger.notice("[extractor=%lx] begin start", tid);
    bus_stat_t& stat = g_bus.get_stat();
    /* 生成生产者id */
    uint32_t statid = stat.get_extract_thread_id("unknown");
    /* 生成mysql链接对象,并注册到线程池，用于stop */
    bus_mysql_t *conn = new(std::nothrow)bus_mysql_t();
    if (conn == NULL) oom_error();
    this->add_conn(conn);
    
    void *data = _queue.pop();
    assert(data != NULL);
    bus_job_t *curjob = static_cast<bus_job_t*>(data);
    uint32_t src_partnum = curjob->get_ds_index();
    data_source_t *source = curjob->get_data_source();
    const char *ip = source->get_ip();
    const int port = source->get_port();
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "%d", port);
    stat.set_producer_thread_source(statid, buffer);
    
    bus_config_t &config = g_bus.get_config();
    const char *user_name = config.get_user_name();
    const char *user_pwd = config.get_user_pwd();
    const int server_id = config.get_server_id();
    const char *charset = config.get_charset();

    bus_engine_t &engine = g_bus.get_engine();
    int ret = -1;
    while (!_stop_flag)
    {
        /* 连接到mysql */
        if (!conn->init(ip, port, user_name, user_pwd, charset))
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("init mysql connection [%s:%d] fail, retry", ip, port);
            sleep(SLEEP_TIME);
            continue;
        }
        /* 如果主线程已经摧毁了当前的connection, 则返回false */
        if(!conn->set_block_mode())
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("[%s:%d] set block mode fail", ip, port);
            goto thread_exit;
        }
        
        /* 如果所有的线程退出，主线程才会去清理engine */
        if (!engine.is_clean_source(src_partnum))
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("source [%s:%d][%u] is not clean, retry",
                    ip, port, src_partnum);
            sleep(SLEEP_TIME);
            continue;
        }
        /* 从下游获取最新的同步位置 */
        if (!config.update_source_position(src_partnum))
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("get source [%s:%d] position fail, retry", ip, port);
            sleep(SLEEP_TIME);
            continue;
        }
        mysql_position_t &srcpos = source->get_position();
        const char *filename = srcpos.binlog_file;
        const long filepos = srcpos.binlog_offset;
        const long rowoffset = srcpos.rowindex;
        const long cmdoffset = srcpos.cmdindex;
        bus_repl_t repl(statid, filename, filepos, rowoffset, cmdoffset,
                conn->get_fd(), source, src_partnum, server_id);
        stat.set_producer_thread_state(statid, THREAD_RUNNING);
        ret = repl.start_repl(_stop_flag);
        if (ret == -1) {
            g_logger.error("[%s:%d] repl fatal error, exit", ip, port);
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            goto thread_exit;
        } else if (ret == 1) {
            g_logger.notice("[extractor=%lx][%s:%d] stop", tid, ip, port);
            goto thread_exit;
        } else if (ret == 2) {
            if (!_stop_flag) {
                g_logger.error("[%s:%d] repl fail, retry", ip, port);
                stat.set_producer_thread_state(statid, THREAD_FAIL);
                sleep(SLEEP_TIME);
            } else {
                g_logger.notice("[extractor=%lx][%s:%d] stop", tid, ip, port);
                goto thread_exit;
            }
        }
    }//while
thread_exit:
    this->add_exit_thread(tid);
    if (stat.get_producer_thread_state(statid) == THREAD_FAIL) {
        stat.set_producer_thread_state(statid, THREAD_EXIT_WITH_FAIL);
    } else {
        stat.set_producer_thread_state(statid, THREAD_EXIT);
    }
    delete curjob;
    mysql_thread_end();
    g_logger.notice("[extractor=%lx][%s:%d] exit succ", tid, ip, port);
}

mysql_incr_extract_t::~mysql_incr_extract_t()
{
    clean_conn();
    while(!_queue.isEmpty())
    {
        void *data = _queue.simple_pop();
        bus_job_t *job = static_cast<bus_job_t*>(data);
        if (job != NULL) delete job;
    }
    pthread_mutex_destroy(&_conn_mutex);
    
}

}//namespace bus
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
