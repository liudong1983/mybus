/***************************************************************************
 *
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 *
 **************************************************************************/

/**
 * @file redis_increment_sender.cc
 * @author haoning(haoning@staff.sina.com.cn)
 * @date 2014/05/20 15:11:18
 * @version 1.0
 * @brief
 *
 **/
#include "redis_increment_sender.h"

namespace bus {
void redis_increment_pool_t::process()
{
    int ret;
    pthread_t pid = pthread_self();
    bus_config_t& config = g_bus.get_config();

    std::vector<data_source_t*> &src_source = config.get_source();
    data_source_t *cursource = src_source[_src_partnum];
    const int src_port = cursource->get_port();

    bus_stat_t& stat = g_bus.get_stat();
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "%d.%d", src_port, _port);
    uint32_t statid = stat.get_sender_thread_id(buffer);

    bus_redis_t conn(_ip, _port, &_stop_flag);
    if(!conn.init_connection())
    {
        g_logger.notice("connect to [%s:%d] fail", _ip, _port);
        stat.set_consumer_thread_state(statid, THREAD_FAIL);
        goto redis_increment_sender_stop;
    }
    stat.set_consumer_thread_state(statid, THREAD_RUNNING);
    
    while (!_stop_flag)
    {
        void* pre_data = _queue.pop();
        if (NULL == pre_data)
        {
            g_logger.notice("[%s:%d][sender=%lx] recv NULL data", _ip, _port, pid);
            goto redis_increment_sender_stop;
        }
        row_t* data = static_cast<row_t*>(pre_data);
        if (!check_redis_row(data)) {
            std::string info;
            data->print(info);
            g_logger.error("[%s:%d][src=%ld] check redis row fail, exit"
                    "row=%s", _ip, _port, 
                    data->get_src_partnum(), info.c_str());
            stat.set_consumer_thread_state(statid, THREAD_FAIL);
            delete data;
            goto redis_increment_sender_stop;
        }
        
        ret = conn.execute_batch_incr(data, statid);
        if (ret == -1)
        {
            stat.set_consumer_thread_state(statid, THREAD_FAIL);
            delete data;
            goto  redis_increment_sender_stop;
        }
        if (data->get_src_schema()->get_record_flag())
            stat.incr_consumer_succ_count(statid, 1);
        delete data;
    }
redis_increment_sender_stop:
    add_exit_thread(pid);
    if (stat.get_consumer_thread_state(statid) == THREAD_FAIL) {
        stat.set_consumer_thread_state(statid, THREAD_EXIT_WITH_FAIL);
    } else {
        stat.set_consumer_thread_state(statid, THREAD_EXIT);
    }
    g_logger.notice("[sender=%lx] exit succ", pid);
}
} //namespace bus

