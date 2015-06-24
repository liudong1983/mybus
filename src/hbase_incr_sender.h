/***************************************************************************
 *
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 *
 **************************************************************************/



/**
 * @file hbase_incr_sender.h
 * @author zhangbo6(zhangbo6@staff.sina.com.cn)
 * @date 2014/12/24 12:23:56
 * @version $Revision$
 * @brief
 *
 **/

#ifndef  __HBASE_INCR_SENDER_H_
#define  __HBASE_INCR_SENDER_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <list>
#include <vector>
#include <map>

#include "bus_row.h"
#include "bus_util.h"
#include "threadpool.h"
#include "bus_config.h"
#include "bus_wrap.h"
#include "bus_hbase_util.h"

namespace bus{

class hbase_incr_pool_t : public thread_pool_t
{
    public:
        hbase_incr_pool_t(uint32_t worker_num, 
                            uint32_t queue_size, 
                            const char* ip,
                            const int port,
                            uint32_t src_partnum,
                            uint32_t dst_partnum):
            thread_pool_t(worker_num, queue_size),
            _port(port),
            _src_partnum(src_partnum),
            _dst_partnum(dst_partnum),
            _conn(NULL)
             {
                 snprintf(_ip, sizeof(_ip), ip);
             }

        virtual ~hbase_incr_pool_t();
        virtual void process();
        int init_connection(bus_hbase_t *conn, uint32_t statid, uint32_t &dstnum, bool);

        void shutdown()
        {
            if (_conn != NULL) {
                _conn->disconnect();
            }
            thread_pool_t::shutdown();
        }

    private:
        DISALLOW_COPY_AND_ASSIGN(hbase_incr_pool_t);
        uint32_t _get_data(std::vector<void*> &batch_vector, MAP2VECTOR &cmd2table, std::string &posvalue);
        int _send_data(bus_hbase_t* conn, MAP2VECTOR &cmd2table, int statid, uint32_t &processnum);
        char _ip[128];
        int _port;
        uint32_t _src_partnum;
        uint32_t _dst_partnum;
        //incr使用单线程，区别于full
        bus_hbase_t* _conn;
};


} //namespace bus


#endif  //__HBASE_INCR_SENDER_H_
