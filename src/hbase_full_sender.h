/***************************************************************************
 *
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 *
 **************************************************************************/



/**
 * @file hbase_full_sender.h
 * @author zhangbo6(zhangbo6@staff.sina.com.cn)
 * @date 2014/12/24 12:23:56
 * @version $Revision$
 * @brief
 *
 **/

#ifndef  __HBASE_FULL_SENDER_H_
#define  __HBASE_FULL_SENDER_H_

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
//typedef std::map<std::string, std::vector<row_t*> > MAP2VECTOR;
//void clear_batch_map(MAP2VECTOR &map);

class hbase_full_pool_t : public thread_pool_t
{
    public:
        hbase_full_pool_t(uint32_t worker_num, 
                            uint32_t queue_size, 
                            const char* ip,
                            const int port,
                            uint32_t src_partnum,
                            uint32_t dst_partnum):
            thread_pool_t(worker_num, queue_size),
            _port(port),
            _src_partnum(src_partnum),
            _dst_partnum(dst_partnum)
             {
                 snprintf(_ip, sizeof(_ip), ip);
                 pthread_mutex_init(&_cmutex, NULL);
             }

        virtual ~hbase_full_pool_t();
        virtual void process();
        int init_connection(bus_hbase_t* conn, uint32_t statid,
                data_source_t** curdst, uint32_t &dstnum, bool);
        
        void shutdown()
        {
            close_conn();
            thread_pool_t::shutdown();
        }

        void add_conn(bus_hbase_t *conn)
        {
            lock_t mylock(&_cmutex);
            _conn_vec.push_back(conn);
        }
        void close_conn()
        {
            lock_t mylock(&_cmutex);
            std::size_t sz = _conn_vec.size();
            for (std::size_t i = 0; i < sz; ++i)
            {
                if (_conn_vec[i] != NULL)
                {
                    g_logger.debug("close_conn");
                    _conn_vec[i]->disconnect();
                    _conn_vec[i] = NULL;
                }
            }
            _conn_vec.clear();
        }

    private:
        DISALLOW_COPY_AND_ASSIGN(hbase_full_pool_t);
        uint32_t _get_data(std::vector<void*> &batch_vector, MAP2VECTOR &cmd2table);
        int _send_data(bus_hbase_t* conn, MAP2VECTOR &cmd2table, int statid, 
                data_source_t* curdst, uint32_t &processnum);
        char _ip[128];
        int _port;
        uint32_t _src_partnum;
        uint32_t _dst_partnum;
        //_cmutex 用于 互斥操作 _conn_vec
        pthread_mutex_t _cmutex;
        std::vector<bus_hbase_t*> _conn_vec;
};

} //namespace bus


#endif  //__HBASE_FULL_SENDER_H_
