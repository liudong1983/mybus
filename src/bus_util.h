/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
/**
 * @file bus_log.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/03/09 09:30:18
 * @version $Revision$
 * @brief 定义日志模块
 *  
 **/
#ifndef  __BUS_UTIL_H_
#define  __BUS_UTIL_H_
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<stdint.h>
#include<stdarg.h>
#include<time.h>
#include<string.h>
#include<pthread.h>
#include<arpa/inet.h>
#include<ifaddrs.h>
#include "bus_log.h"

namespace bus{
enum source_t {HBASE_DS, MYSQL_DS, REDIS_DS, REDIS_COUNTER_DS, BUS_DS, SOURCE_EMPTY};
enum data_type_t {INT64, UINT64, INT32, UINT32, STRING};
enum transfer_type_t {FULL, INCREMENT, TRANSFER_EMPTY};
enum transfer_state_t {INIT,
    FULL_TRAN_BEGIN,
    FULL_TRAN_WAIT_PRODUCER_EXIT,
    FULL_TRAN_WAIT_CONSUMOR_EXIT,
    FULL_TRAN_STOP_WAIT,
    INCR_TRAN_INIT,
    INCR_TRAN_BEGIN,
    INCR_TRAN_STOP_WAIT,
    SHUTDOWN_WAIT,
    STOP_AND_EXIT,
    TRAN_FAIL};
enum thread_state_t {
    THREAD_INIT,
    THREAD_RUNNING,
    THREAD_FAIL,
    THREAD_EXIT,
    THREAD_EXIT_WITH_FAIL};
extern char *start_error_msg[];
extern char *transfer_state_msg[];
extern char *thread_msg[];
enum col_stat_t {COL_NOT_RRESENT, COL_NULL, COL_NORMAL};
#define INIT_NOT_OK 0
#define GET_META_INFO_FAIL 1
#define INIT_THREAD_POOL_FAIL 2
#define SPLIT_JOB_FAIL 3
#define START_ENGINE_FAIL 4
#define ENGINE_NOT_RUNNING 5
#define SHUTDOWN_NOT_INIT 6
#define RELOAD_NOT_INIT 7
#define RELOAD_FAIL 8
#define ENGINE_INIT 9
#define SLEEP_TIME 5
#define BATCH_COUNT 80
//#define HBASE_BATCH_COUNT 10000
#define HBASE_BATCH_COUNT 2000
#define HBASE_RECONN_COUNT 10

#define PRODUCER_QUEUE_SIZE 20000
#define CONSUMER_QUEUE_SIZE 20000

#define HASH_TABLE_SIZE 10240

#define ROW_NUM_INVALID -1
#define ROW_ADD_STOP -2
#define ROW_ADD_OK 0
#define ROW_IGNORE_OK 1 

#define BUS_NOTUSED(V) ((void) V)
#define DEFAULT_LOG_LEVEL 2
/* disallow copy assign */
void fatal_error();
bool str2num(const char *str, long &num);
bool get_inner_ip(char *ip_str, uint32_t len);
bool copy_file(const char *source, const char *dst);
int translate_data_type(const char *type);
int translate_source_type(const char *type);

}//namespace bus
#endif  //__BUS_UTIL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
