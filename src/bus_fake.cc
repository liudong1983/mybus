/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_fake.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/24 18:19:32
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_fake.h"
#include "bus_wrap.h"
namespace bus {
fake_pool_t::~fake_pool_t()
{
    while(!_queue.isEmpty())
    {
        void *data = _queue.simple_pop();
        row_t *row = static_cast<row_t*>(data);
        if (row != NULL) delete row;
    }
}

void fake_pool_t::process()
{
    bus_stat_t& stat = g_bus.get_stat();
    char buffer[128];
    snprintf(buffer, sizeof(buffer), "%s.%d", _ip, _port);
    uint32_t statid = stat.get_sender_thread_id(buffer);
    pthread_t pid = pthread_self();

    while(!_stop_flag)
    {
        void* pre_data = _queue.pop();
        if (NULL == pre_data)
        {
            g_logger.notice("recv data NULL,[worker=%lu], will exit", pid);
            break;
        }
        row_t *data = static_cast<row_t*>(pre_data);
        delete data;
        data = NULL;
    }
    add_exit_thread(pid);
    if (stat.get_consumer_thread_state(statid) == THREAD_FAIL)
    {
        stat.set_consumer_thread_state(statid, THREAD_EXIT_WITH_FAIL);
    } else {
        stat.set_consumer_thread_state(statid, THREAD_EXIT);
    }
    g_logger.notice("[fake sender=%lu] exit succ", pid);
}

} //namespace bus
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
