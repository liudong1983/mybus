/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_stat.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/05 22:32:18
 * @version $Revision$
 * @brief 
 *  
 **/



#ifndef  __BUS_STAT_H_
#define  __BUS_STAT_H_
#include <pthread.h>
#include <vector>
#include "bus_util.h"
#include "bus_position.h"
namespace bus {
class stat_item_t
{
    public:
        stat_item_t():succ_count(0), error_count(0) {}
        uint64_t succ_count;
        uint64_t error_count;
};
class bus_stat_t
{
    public:
        bus_stat_t()
        {
            pthread_mutex_init(&_pmutex, NULL);
            pthread_mutex_init(&_smutex, NULL);
        }

        ~bus_stat_t();
        uint32_t get_sender_thread_id(char*);
        void set_sender_thread_name(int statid, char *name);
        uint32_t get_extract_thread_id(char*);
        void incr_producer_succ_count(uint32_t tid, uint64_t step)
        {
            _stat_producer[tid]->succ_count += step;
        }
        void incr_consumer_succ_count(uint32_t tid, uint64_t step)
        {
            _stat_consumer[tid]->succ_count += step;
        }
        uint64_t get_producer_succ_count(uint32_t tid)
        {
            return _stat_producer[tid]->succ_count;
        }
        uint64_t get_consumer_succ_count(uint32_t tid)
        {
            return _stat_consumer[tid]->succ_count;
        }
        void incr_producer_error_count(uint32_t tid, uint64_t step)
        {
            _stat_producer[tid]->error_count += step;
        }
        void incr_consumer_error_count(uint32_t tid, uint64_t step)
        {
            _stat_consumer[tid]->error_count += step;
        }
        void get_producer_count(uint64_t& succ_count, uint64_t& error_count);
        void get_consumer_count(uint64_t& succ_count, uint64_t& error_count);
        void clear();
        void set_producer_thread_state(uint32_t tid, thread_state_t tstate)
        {
            _thread_producer[tid] = tstate;
        }
        thread_state_t get_producer_thread_state(uint32_t tid)
        {
            return _thread_producer[tid];
        }
        void set_consumer_thread_state(uint32_t tid, thread_state_t tstate)
        {
            _thread_consumer[tid] = tstate;
        }
        thread_state_t get_consumer_thread_state(uint32_t tid)
        {
            return _thread_consumer[tid];
        }
        bool get_thread_state();
        void get_thread_info(std::string &info);
        void clear_thread_state()
        {
            _thread_producer.clear();
            _thread_consumer.clear();
            g_logger.reset_error_info();
        }
        void set_producer_thread_source(uint32_t statid, char *str)
        {
            _src_producer[statid].assign(str);
        }
        void set_consumer_thread_source(uint32_t statid, char *str)
        {
            _src_consumer[statid].assign(str);
        }
        void get_thread_stat_info(std::string &info);
        uint32_t get_producer_size()
        {
            return _thread_producer.size();
        }
        uint32_t get_consumer_size()
        {
            return _thread_consumer.size();
        }
        void reset_all_thrift_flag();
        bool get_thrift_flag(uint64_t index);
        void reset_thrift_flag(uint64_t index);
    private:
        pthread_mutex_t _pmutex;
        pthread_mutex_t _smutex;
        std::vector<stat_item_t*> _stat_producer;
        std::vector<stat_item_t*> _stat_consumer;
        std::vector<thread_state_t> _thread_producer;
        std::vector<thread_state_t> _thread_consumer;
        std::vector<mysql_position_t*> _stat_position;
        std::vector<std::string> _src_producer;
        std::vector<std::string> _src_consumer;
        std::vector<bool> _thrift_flag;
};
} //namespace bus
#endif  //__BUS_STAT_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
