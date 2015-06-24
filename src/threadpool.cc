/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
/**
 * @file threadpool.cpp
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/03/23 09:36:06
 * @version $Revision$
 * @brief 定义线程池模型
 *  
 **/
#include "threadpool.h"
#include "bus_wrap.h"

namespace bus{

static void* callback(void *arg)
{
    thread_pool_t* pool = static_cast<thread_pool_t*>(arg);
    pool->process();
    return NULL;
}

bool queue_t::push(void *item)
{
   lock_t lock(&_mutex);

   while (isFull() && !_stop_flag)
   {
       pthread_cond_wait(&_full, &_mutex);
   }

   if (_stop_flag) return false;

   if (isEmpty())
   {
        _dlist.push_back(item);
        pthread_cond_broadcast(&_empty);
   } else
   {
       _dlist.push_back(item);
   }
   ++_size;

   return true;
}

void* queue_t::pop()
{
    lock_t lock(&_mutex);
    while (isEmpty() && !_stop_flag)
    {
        pthread_cond_wait(&_empty, &_mutex);
    }
    if (_stop_flag) return NULL;
    void *item = NULL;
    if (isFull())
    {
        item = _dlist.front();
        _dlist.pop_front();
        pthread_cond_broadcast(&_full);
    } else
    {
        item = _dlist.front();
        _dlist.pop_front();
    }
    --_size;

    return item;
}

//返回值：-1表示stop信号_stop_flag或者NULL，其它表示不stop
//请通过rtn.size()获取pop的数目
int32_t queue_t::pop_batch(std::vector<void*> &rtn)
{
    lock_t lock(&_mutex);
    while(isEmpty() && !_stop_flag)
    {
        pthread_cond_wait(&_empty, &_mutex);
    }
    if (_stop_flag) return -1;
    
    uint32_t batchsize = (_size > HBASE_BATCH_COUNT) ? HBASE_BATCH_COUNT : _size;

    void *item = NULL;
    uint32_t i = 0;
    for (; i < batchsize; ++i)
    {
        item = _dlist.front();
        _dlist.pop_front();
        if (NULL == item) { ++i; break;} //bug resolved
        rtn.push_back(item);
    }

    if (isFull()) 
        pthread_cond_broadcast(&_full);
    _size = _size - i;

    return (NULL == item) ? -1 : i;
}
/*********************************************************/
bool thread_pool_t::init()
{
    for (uint32_t i = 0; i < _worker_num; ++i)
    {
        pthread_t tid;
        int ret = pthread_create(&tid, NULL, callback, (void*)this);
        if (ret)
        {
            g_logger.error("create thread fail");
            return  false;
        }
    }
    return true;
}

thread_pool_t::~thread_pool_t()
{
    for (std::vector<pthread_t>::iterator it = _end_workers.begin();
            it != _end_workers.end();
            ++it)
        {
            pthread_join(*it, NULL);
        }
    pthread_mutex_destroy(&_mutex);
}
}//namespace bus

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
