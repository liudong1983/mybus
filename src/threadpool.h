/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file threadpool.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/03/20 19:48:16
 * @version $Revision$
 * @brief 
 *  
 **/



#ifndef  __THREADPOOL_H_
#define  __THREADPOOL_H_


#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<pthread.h>
#include<list>
#include "hiredis.h"
#include "bus_util.h"
#include "bus_row.h"

namespace bus{

class queue_t{
    public:
        explicit queue_t(uint32_t size):_maxsize(size), 
        _stop_flag(false), _size(0)
        {
            pthread_mutex_init(&_mutex, NULL);
            pthread_cond_init(&_empty, NULL);
            pthread_cond_init(&_full, NULL);
        }
        ~queue_t()
        {
            pthread_mutex_destroy(&_mutex);
            pthread_cond_destroy(&_empty);
            pthread_cond_destroy(&_full);
        }
        bool push(void *item);
        void* pop();
        //for hbase sender
        int32_t pop_batch(std::vector<void*> &rtn);
        void* simple_pop()
        {
            void* item = _dlist.front();
            _dlist.pop_front();
            --_size;
            return item;
        }
        void simple_add(void *item)
        {
            _dlist.push_back(item);
            ++_size;
        }

        bool isEmpty() const
        {
           return _size == 0 ? true : false;
        }

        bool isFull() const
        {
            return _size >= _maxsize ? true : false;
        }
        void stop()
        {
            _stop_flag = true;
            pthread_cond_broadcast(&_empty);
            pthread_cond_broadcast(&_full);
        }
        uint32_t get_queue_size()
        {
            return _size;
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(queue_t);
        uint32_t _maxsize;
        std::list<void*> _dlist;
        pthread_mutex_t _mutex;
        pthread_cond_t _empty;
        pthread_cond_t _full;
        bool _stop_flag;
        uint32_t _size;
};

class thread_pool_t
{
    public:
        thread_pool_t(uint32_t worker_num, uint32_t queue_size):
            _queue(queue_size),
            _worker_num(worker_num),
            _stop_flag(false)
        {
            pthread_mutex_init(&_mutex, NULL);
        }
        void set_thread_num(const uint32_t worker_num)
        {
            _worker_num = worker_num;
        }
        virtual bool init();
        bool add(void *item)
        {
            return _queue.push(item);
        }
        void simple_add(void *item)
        {
            _queue.simple_add(item);
        }
        bool get_stat()
        {
            lock_t lock(&_mutex);
            return _end_workers.size() == _worker_num ? true : false;
        }
        void add_exit_thread(pthread_t tid)
        {
           lock_t lock(&_mutex);
           _end_workers.push_back(tid);
        }
        virtual void shutdown()
        {
            _stop_flag = true;
            _queue.stop();
        }
        uint32_t get_task_size()
        {
            return _queue.get_queue_size();
        }
        virtual void process() = 0;
        virtual ~thread_pool_t();
        uint32_t get_worker_num()
        {
            return _worker_num;
        }
    protected:
        //_queue中保存的对象，在thread_pool_t子类的析构函数中释放
        queue_t _queue;
        std::vector<pthread_t> _end_workers;
        uint32_t _worker_num;
        bool _stop_flag;
        pthread_mutex_t _mutex;
    private:
        DISALLOW_COPY_AND_ASSIGN(thread_pool_t);
};
} //namespace bus

#endif  //__THREADPOOL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
