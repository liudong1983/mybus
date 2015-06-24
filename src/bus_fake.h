/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_fake.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/24 18:11:54
 * @version $Revision$
 * @brief 
 *  
 **/
#ifndef  __BUS_FAKE_H_
#define  __BUS_FAKE_H_
#include "threadpool.h"
#include "bus_util.h"

namespace bus {
class fake_pool_t : public thread_pool_t
{
    public:
        fake_pool_t(uint32_t worker_num,
                uint32_t queue_size,
                const char *ip, const int port,
                uint32_t src_partnum):
            thread_pool_t(worker_num, queue_size), _port(port),
            _src_partnum(src_partnum)
        {
            snprintf(_ip, sizeof(_ip), "%s", ip);
        }

        virtual ~fake_pool_t();
        virtual void process();
    private:
        DISALLOW_COPY_AND_ASSIGN(fake_pool_t);
        char _ip[128];
        int _port;
        uint32_t _src_partnum;
};
}//namespace bus
#endif  //__BUS_FAKE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
