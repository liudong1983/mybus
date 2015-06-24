/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file mysql_incr_extract.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/23 11:42:20
 * @version $Revision$
 * @brief 
 *  增量抽取mysql binlog 事件
 **/
#ifndef  __MYSQL_INCR_EXTRACT_H_
#define  __MYSQL_INCR_EXTRACT_H_
#include "threadpool.h"
namespace bus {
class mysql_incr_extract_t : public thread_pool_t
{
    public:
        mysql_incr_extract_t(uint32_t worker_num,
        uint32_t queue_size):
            thread_pool_t(worker_num, queue_size)
        {
            pthread_mutex_init(&_conn_mutex, NULL);
        }
        virtual void shutdown();
        virtual ~mysql_incr_extract_t();
        virtual void process();
        virtual bool init();
        void add_conn(bus_mysql_t *conn)
        {
            lock_t mylock(&_conn_mutex);
            _conn_vec.push_back(conn);
        }
        void clean_conn()
        {
            lock_t mylock(&_conn_mutex);
            std::size_t sz = _conn_vec.size();
            for (std::size_t i = 0; i < sz; ++i)
            {
                if (_conn_vec[i] != NULL)
                {
                    delete _conn_vec[i];
                    _conn_vec[i] = NULL;
                }
            }
            _conn_vec.clear();
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(mysql_incr_extract_t);
    private:
        std::vector<bus_mysql_t*> _conn_vec;
        pthread_mutex_t _conn_mutex;
};

} //namespace bus
#endif  //__MYSQL_INCR_EXTRACT_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
