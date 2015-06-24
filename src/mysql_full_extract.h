/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file mysql_full_extract.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/08 14:22:22
 * @version $Revision$
 * @brief 
 * 全量抽取mysql数据 
 **/

#ifndef  __MYSQL_FULL_EXTRACT_H_
#define  __MYSQL_FULL_EXTRACT_H_
#include "bus_util.h"
#include "bus_wrap.h"
#include "bus_engine.h"
#include "threadpool.h"
namespace bus {
class mysql_full_extract_t : public thread_pool_t
{
    public:
        mysql_full_extract_t(uint32_t worker_num, uint32_t queue_size):
            thread_pool_t(worker_num, queue_size){}
        virtual ~mysql_full_extract_t();
        bool work(void *item, uint32_t statid);
        virtual void process();
        virtual bool init();
    private:
        DISALLOW_COPY_AND_ASSIGN(mysql_full_extract_t);
};

}//namespace bus



#endif  //__MYSQL_FULL_EXTRACT_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
