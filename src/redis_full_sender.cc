/***************************************************************************
 *
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 *
 **************************************************************************/

/**
 * @file redis_full_sender.cc
 * @author haoning(haoning@staff.sina.com.cn)
 * @date 2014/05/15 23:11:18
 * @version 1.0
 * @brief
 *
 **/
#include "redis_full_sender.h"

namespace bus {
redis_full_pool_t::~redis_full_pool_t()
{
    while(!_queue.isEmpty())
    {
        void *data = _queue.simple_pop();
        row_t *row = static_cast<row_t*>(data);
        if (row != NULL) delete row;
    }
}

void redis_full_pool_t::process()
{
    bus_stat_t& stat = g_bus.get_stat();
    bus_config_t& config = g_bus.get_config();
    std::vector<data_source_t*> &source = config.get_source();
    data_source_t* cursource = source[_src_partnum];
    const int src_port = cursource->get_port();
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "%d.%d", src_port, _port);
    uint32_t statid = stat.get_sender_thread_id(buffer);
    pthread_t pid = pthread_self();
    bus_redis_t conn(_ip, _port, &_stop_flag);
    // end_flag is useless? maybe can delete
    bool end_flag = false;

    std::vector<row_t*> batch_vector;
    std::vector<row_t*> batch_error;
    batch_vector.reserve(BATCH_COUNT);
    batch_error.reserve(BATCH_COUNT);

    if (!conn.init_connection())
    {
        stat.set_consumer_thread_state(statid, THREAD_FAIL);
        g_logger.error("init connection [%s:%d] fail",
                _ip, _port);
        goto redis_full_sender_stop;
    }

    while (!_stop_flag)
    {
        assert(batch_vector.empty() && batch_error.empty());
        for (uint32_t i = 0; i < BATCH_COUNT; ++i)
        {
            void* pre_data = _queue.pop();
            if (NULL == pre_data)
            {
                g_logger.notice("[%s:%d][sender=%lx], recv data NULL", 
                        _ip, _port, pid);
                end_flag = true;
                break;
            }
            row_t* data = static_cast<row_t*>(pre_data);
            if (!check_redis_row(data)) {
                std::string info;
                data->print(info);
                g_logger.error("[%s:%d][src=%ld] check redis row fail, exit, row=%s",
                        _ip, _port, data->get_src_partnum(), info.c_str());
                stat.set_consumer_thread_state(statid, THREAD_FAIL);
                delete data;
                goto redis_full_sender_stop;
            }
            batch_vector.push_back(data);
        }
        
        int batch_ret = conn.execute_batch(batch_vector, batch_error, statid);
        if (batch_ret == -1)
        {
            g_logger.error("[%s:%d] [sender=%lx] execute batch fatal error, exit",
                    _ip, _port, pid);
            stat.set_consumer_thread_state(statid, THREAD_FAIL);
            goto redis_full_sender_stop;
        }
        if (end_flag) break;
    } //while
redis_full_sender_stop:
    clear_batch_row(batch_vector);
    clear_batch_row(batch_error);
    add_exit_thread(pid);
    if (stat.get_consumer_thread_state(statid) == THREAD_FAIL)
    {
        stat.set_consumer_thread_state(statid, THREAD_EXIT_WITH_FAIL);
    } else {
        stat.set_consumer_thread_state(statid, THREAD_EXIT);
    }
    g_logger.notice("[%s:%d][sender=%lx] exit succ", _ip, _port, pid);
}
} //namespace bus

