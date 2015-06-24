/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_engine.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/28 22:47:16
 * @version $Revision$
 * @brief 
 *  
 **/



#ifndef  __BUS_ENGINE_H_
#define  __BUS_ENGINE_H_

#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <vector>
#include "bus_util.h"
#include "bus_row.h"
#include "threadpool.h"
#include "bus_config.h"
#include "data_source.h"

namespace bus{
class data_source_t;
class schema_t;
class bus_job_t
{
    public:
        bus_job_t(data_source_t *srcds, transfer_type_t jobtype, 
                schema_t *schema,
                const char *db, const char *table, uint32_t pos):
            _srcds(srcds), _job_type(jobtype),
        _schema(schema), _src_ds_index(pos)
        {
            if (table == NULL)
            {
                _table[0] = '\0';
            } else {
                snprintf(_table, sizeof(_table), "%s", table);
            }

            if (db == NULL)
            {
                _db[0] = '\0';
            } else {
                snprintf(_db, sizeof(_db), "%s", db);
            }
        }
        data_source_t* get_data_source()
        {
            return _srcds;
        }

        transfer_type_t get_job_type()
        {
            return _job_type;
        }

        schema_t* get_schema()
        {
            return _schema;
        }
        char* get_table()
        {
            return _table;
        }
        char *get_db()
        {
            return _db;
        }
        uint32_t get_ds_index()
        {
            return _src_ds_index;
        }
        ~bus_job_t() {}
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_job_t);
    private:
        data_source_t *_srcds;
        transfer_type_t _job_type;
        schema_t *_schema;
        char _table[64];
        char _db[64];
        uint32_t _src_ds_index;
};
/**
 * @brief 核心生产者消费者模型
 */
class bus_engine_t
{
    public:
        bus_engine_t():_reader(NULL),_update_num(1) {}
        void set_producer_pool(thread_pool_t *reader)
        {
           _reader = reader;
        }
        void add_consumor_pool(thread_pool_t *sender)
        {
            _sender.push_back(sender);
        }

        void set_dst_source_sz(uint32_t sz)
        {
            _dstsz = sz;
        }
        bool start();
        void stop();
        void add_job(bus_job_t *job);
        int add_row(row_t *row, std::vector<mysql_position_t*> &all_pos);
        int add_row(row_t *row);
        bool add_end_row();
        /* 判断reader的所有线程是否已经退出 */
        bool get_reader_stat()
        {
            if (_reader == NULL) return true;
            return _reader->get_stat();
        }
        /* 判断某个生产者对应的队列是否已经下发完毕 */
        bool is_clean_source(uint32_t src_partnum);
        /*
        bool is_clean_source(uint32_t src_partnum)
        {
            source_t dsttype = g_bus.get_dst_type();
            if (REDIS_DS == dsttype || REDIS_COUNTER_DS == dsttype)
            {
                uint32_t index = src_partnum * _dstsz;
                uint32_t sz = _sender.size();
                if (index >= sz) return true;
                for (uint32_t i = 0; i < _dstsz; ++i)
                {
                    if (_sender[index + i]->get_task_size() != 0) {
                        return false;
                    }
                }
            } else if (HBASE_DS == dsttype) {
                //对于hbase来说，一个mysql对应一个sender
                if (_sender[src_partnum]->get_task_size() != 0) {
                    return false;
                }
            }
            return true;
        }
        */
        int add_row_copy(row_t *row, std::vector<mysql_position_t*> &all_pos, int statid);
        int add_row_copy(row_t *row, int statid);
        /* 判断sender线程池的所有线程是否已经退出 */
        bool get_sender_stat();

        void set_update_num(uint64_t update_num)
        {
            _update_num = update_num;
        }
        uint64_t get_update_num()
        {
            return _update_num;
        }

        void destroy();
        ~bus_engine_t();
        /* 获取生产者消队列大小 */
        uint32_t get_task_size()
        {
            if (_reader == NULL) return 0;
            return _reader->get_task_size();
        }
        void get_thread_num(uint32_t &pnum, uint32_t &cnum)
        {
            pnum = 0;
            cnum = 0;
            if (_reader != NULL) pnum = _reader->get_worker_num();
            uint32_t sz = _sender.size();
            for (uint32_t i = 0; i < sz; ++i)
            {
                cnum += _sender[i]->get_worker_num();
            }
        }
    private:
        thread_pool_t *_reader;
        std::vector<thread_pool_t*> _sender;
        //用于动态设置redis的position信息同步频率
        uint64_t _update_num;
        uint32_t _dstsz;
};

} //namespace bus

#endif  //__BUS_ENGINE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
