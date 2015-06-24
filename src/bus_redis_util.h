/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
/**
 * @file bus_redis_util.h
 * @author haoning(haoning@staff.sina.com.cn)
 * @date 2014/06/17 09:30:18
 * @version $Revision$
 * @brief 定义redis相关操作
 *  
 **/



#ifndef  __BUS_REDIS_UTIL_H_
#define  __BUS_REDIS_UTIL_H_
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include "hiredis.h"
#include "bus_util.h"
#include "threadpool.h"
#include "bus_config.h"
#include "bus_wrap.h"


namespace bus{
//void clear_batch_row(std::vector<row_t*> &batch_vec);
class bus_redis_t
{
    public:
        bus_redis_t(const char* ip, const int port, bool *stop_flag):
            _redis_port(port),_redis_connection(NULL),_stop_flag(stop_flag)
        {
            snprintf(_redis_ip, sizeof(_redis_ip), ip);
        }

        ~bus_redis_t()
        {
            if (_redis_connection != NULL)
            {
                redisFree(_redis_connection);
                _redis_connection = NULL;
            }
        }

        bool init_connection();
        int append_row(row_t* data);
        bool append_batch_row(std::vector<row_t*> &batch_row);
        bool execute_batch_row(std::vector<row_t*> &batch_row, uint32_t statid, std::vector<row_t*> &batch_error);
        int execute_batch(std::vector<row_t*> &batch_row, std::vector<row_t*> &batch_error, uint32_t statid);
        bool execute_batch_error(std::vector<row_t*> &batch_error, uint32_t statid);

        bool append_redis(row_t *data, int send_pos, bool);
        bool append_rediscounter(row_t *data, int send_pos, bool);
        int execute_rediscounter_row(row_t *data, int &send_pos, bool);
        int execute_redis_row(row_t *data, int &send_pos, bool);
        int execute_batch_incr(row_t *data, uint32_t statid);

        int execute_del_redis(row_t *data, bool del_flag, bool);
        int execute_del_rediscounter(row_t *data, bool del_flag, bool);
        bool append_del_redis(row_t *data, bool del_flag, bool);
        bool append_del_rediscounter(row_t *data, bool del_flag, bool);
        int decr_and_del(row_t *data, int &send_pos, bool);
    private:
        redisReply* _exec_rcommand(const char* redis_cmd,
                        const char* redis_key,
                        const char* redis_field,
                        const char* redis_value);
        DISALLOW_COPY_AND_ASSIGN(bus_redis_t);
    private:
        char _redis_ip[128];
        int _redis_port;
        redisContext* _redis_connection;
        bool *_stop_flag;
};
}//namespace bus

#endif  //__BUS_REDIS_UTIL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
