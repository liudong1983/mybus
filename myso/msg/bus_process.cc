/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
/**
 * @file bus_process.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/08 16:34:23
 * @version $Revision$
 * @brief 
 *  自定义行处理函数
 **/
#include <stdlib.h>
#include "bus_row.h"
#include "bus_log.h"
#include "crc32.h"
#ifdef __cplusplus
 extern "C" {
#endif
     int process(bus::row_t *row);
     int init(void* buslog, char *soversion, size_t len);
#ifdef __cplusplus
 }
#endif

bus::bus_log_t *mylogger;
int init(void *buslog, char *soversion, size_t len)
{
    assert(buslog != NULL);
    mylogger = static_cast<bus::bus_log_t*>(buslog);
    mylogger->notice("init so succ");
    snprintf(soversion, len, "%s.%s", VERSION, RELEASE);
    return 0;
}

int process(bus::row_t *in_row)
{
    assert(in_row != NULL);
    long schema_id = in_row->get_schema_id();
    long instance_num = in_row->get_instance_num();
    bus::action_t act = in_row->get_action();
    if (schema_id == 0) {
        char *ts;
        bool ret = in_row->get_value(0, &ts);
        if (!ret) return PROCESS_FATAL;
        if (ts == NULL) return PROCESS_IGNORE;
        long src_num = in_row->get_src_port();
        char tm_key[128];
        snprintf(tm_key, sizeof(tm_key), "%ld_tm", src_num);
        if (act == bus::INSERT) {
            for (int i = 0; i < instance_num; ++i)
            {
                in_row->push_cmd(i, "set", tm_key, NULL, ts);
            }
        } else {
            return PROCESS_IGNORE;
        }
    } else if (schema_id == 1) {
        /* 获得数据 */
        char *owneruid, *type;
        bool ret1 = in_row->get_value(0, &owneruid);
        bool ret2 = in_row->get_value(1, &type);
        if (!ret1 || !ret2) {
            return PROCESS_FATAL;
        }
        char *value_buf = "4294967296";
        if (owneruid ==  NULL ||
                type == NULL) return PROCESS_IGNORE;
        int type_num = atoi(type);
        /* 设定目标切片号 */
        char key_buf[128];
        snprintf(key_buf, sizeof(key_buf), "%s_t", owneruid);

        uint32_t dst_partnum = crc32(0, owneruid, strlen(owneruid)) & 0xffffffff % instance_num;

        if (act == bus::INSERT)
        {
            if (type_num == 0) {
                in_row->push_cmd(dst_partnum, "incrby", key_buf, NULL, value_buf);
            } else if (type_num == 1) {
                in_row->push_cmd(dst_partnum, "incrby", key_buf, NULL, "1");
            } else {
                return PROCESS_IGNORE;
            }
        } else if (act == bus::UPDATE) {
            mylogger->error("the bis don't have update, something is wrong");
            return PROCESS_IGNORE;
        } else if (act == bus::DEL) {
            if (type_num == 0){
                in_row->push_cmd(dst_partnum, "decr_and_del", key_buf, NULL, value_buf);
            } else if (type_num == 1) {
                in_row->push_cmd(dst_partnum, "decr_and_del", key_buf, NULL, "1");
            } else {
                return PROCESS_IGNORE;
            }
        } else {
            return PROCESS_IGNORE;
        }
    } else if (schema_id == 2) {
        /* 获得数据 */
        char *owneruid;
        bool ret1 = in_row->get_value(0, &owneruid);
        if (!ret1) {
            return PROCESS_FATAL;
        }
        if (owneruid ==  NULL) return PROCESS_IGNORE;
        /* 设定目标切片号 */
        char key_buf[128];
        snprintf(key_buf, sizeof(key_buf), "%s_c", owneruid);

        uint32_t dst_partnum = crc32(0, owneruid, strlen(owneruid)) & 0xffffffff % instance_num;

        if (act == bus::INSERT)
        {
            in_row->push_cmd(dst_partnum, "incrby", key_buf, NULL, "1");
        } else if (act == bus::UPDATE) {
            mylogger->error("the bis don't have update, something is wrong");
            return PROCESS_IGNORE;
        } else if (act == bus::DEL) {
            in_row->push_cmd(dst_partnum, "decr_and_del", key_buf, NULL, "1");
        } else {
            return PROCESS_IGNORE;
        }
    } else {
        return PROCESS_IGNORE;
    }
    return PROCESS_OK;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
