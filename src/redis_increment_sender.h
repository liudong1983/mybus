/***************************************************************************
 *
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 *
 **************************************************************************/



/**
 * @file redis_increment_sender.h
 * @author haoning(haoning@staff.sina.com.cn)
 * @date 2014/05/15 19:48:16
 * @version 1.0
 * @brief
 *
 **/

#ifndef  __REDIS_INCREMENT_SENDER_H_
#define  __REDIS_INCREMENT_SENDER_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <list>

#include "hiredis.h"
#include "bus_util.h"
#include "threadpool.h"
#include "bus_config.h"
#include "bus_wrap.h"
#include "bus_redis_util.h"


namespace bus{

class redis_increment_pool_t : public thread_pool_t
{
    public:
        redis_increment_pool_t(uint32_t worker_num,
                uint32_t queue_size,
                const char* ip,
                const int port,
                uint32_t src_partnum):
            thread_pool_t(worker_num, queue_size),
            _port(port),
            _src_partnum(src_partnum)
        {
            snprintf(_ip, sizeof(_ip), ip);
        }

        virtual ~redis_increment_pool_t()
        {
            while(!_queue.isEmpty())
            {
                void *data = _queue.simple_pop();
                row_t *row = static_cast<row_t*>(data);
                if (row != NULL) delete row;
            }
        }
        virtual void process();
    private:
        DISALLOW_COPY_AND_ASSIGN(redis_increment_pool_t);
    private:
        char _ip[128];
        int _port;
        uint32_t _src_partnum;
};


} //namespace bus


#endif  //__REDIS_INCREMENT_SENDER_H_
