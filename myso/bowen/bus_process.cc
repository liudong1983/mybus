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
#include <time.h>
#include "bus_row.h"
#include "bus_log.h"
#ifdef __cplusplus
 extern "C" {
#endif
     int process(bus::row_t *row);
     //void * is bus_log_t* (&g_logger)
     int init(void *, char *soversion, size_t len);
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

#define DEADTIME (14400)UL //1970-01-01 12:00:00 unix timestamp
#define BOWENTABLE "sina_blog_article"

long strtotime(char *datetime)
{
    struct tm tm_time;
    memset(&tm_time, 0, sizeof(struct tm));
    strptime(datetime, "%Y-%m-%d %H:%M:%S", &tm_time);

    return mktime(&tm_time);
}

int process(bus::row_t *in_row)
{
    assert(in_row != NULL);
    long schema_id = in_row->get_schema_id();
    long instance_num = in_row->get_instance_num();
    bus::action_t act = in_row->get_action();

    if (schema_id == 1) {
        // for databus heartbeat
        char *ts;
        bool ret = in_row->get_value_byindex(0, &ts, mylogger);
        if (!ret) return PROCESS_FATAL;
        if (ts == NULL) return PROCESS_IGNORE;

        long src_num = in_row->get_src_port();
        char tm_key[128];
        snprintf(tm_key, sizeof(tm_key), "bowen_tm_%ld", src_num);

        /*
        std::vector<bus::tcolumnvalue_t> tcv_vec;
        tcv_vec.push_back(bus::tcolumnvalue_t("bus", "tm", ts, USE_OLD_VALUE, HBASE_DEFAULT_TIME));
        in_row->push_cmd("put", "databus", tm_key, tcv_vec);
        */
        bus::hbase_cmd_t *hcmd = in_row->get_hbase_put("databus", tm_key, 0);
        hcmd->add_columnvalue("bus", "tm", ts, 0, USE_OLD_VALUE);
    }
    else if (schema_id == 2) {
        //for bowen content
        // id(dataid) uid(x) blog_id blog_title blog_info blog_body
        char *id, *uid, *blog_id = NULL, *blog_title, *blog_info, *blog_body, *old_blog_id;
        bool ret;
        if (act == bus::INSERT || act == bus::UPDATE) {
            in_row->get_value_byindex(2, &blog_id, mylogger); //blog_id
            if (blog_id[0] == '\0') {
                return PROCESS_IGNORE;
            }
            //博文mysql中，没有NULL列，所以这里不判断获取的id、uid...为空
            in_row->get_value_byindex(0, &id, mylogger); //id
            in_row->get_value_byindex(1, &uid, mylogger); //uid x
            in_row->get_value_byindex(3, &blog_title, mylogger); //blog_title
            in_row->get_value_byindex(4, &blog_info, mylogger); //blog_info
            in_row->get_value_byindex(5, &blog_body, mylogger); //blog_body

            if (act == bus::UPDATE) {
                in_row->get_old_value_byindex(2, &old_blog_id, mylogger); //blog_title
                if (blog_id == NULL || old_blog_id == NULL) {
                    mylogger->error("bowen update blog_id || old_blog_id is NULL");
                    return PROCESS_FATAL;
                } else if (strcmp(blog_id, old_blog_id)) {
                    mylogger->error("UNEXCEPT bowen update blog_id:%s old_blog_id:%s", blog_id, old_blog_id);
                }
            }

            bus::hbase_cmd_t *hcmd = in_row->get_hbase_put(BOWENTABLE, blog_id, 0);
            hcmd->add_columnvalue("a", "dataid", id, 0, USE_OLD_VALUE);
            hcmd->add_columnvalue("a", "blog_title", blog_title, 0, USE_OLD_VALUE);
            hcmd->add_columnvalue("a", "blog_info", blog_info, 0, USE_OLD_VALUE);
            hcmd->add_columnvalue("a", "blog_body", blog_body, 0, USE_OLD_VALUE);
            /*
            std::vector<bus::tcolumnvalue_t> tcv_vec;
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "dataid", id, USE_OLD_VALUE, HBASE_DEFAULT_TIME));
            tcv_vec.push_back(bus::tcolumnvalue_t("a", "dataid", id, 0, HBASE_DEFAULT_TIME));
            tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_title", blog_title, USE_OLD_VALUE, HBASE_DEFAULT_TIME));
            tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_info", blog_info, USE_OLD_VALUE, HBASE_DEFAULT_TIME));
            tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_body", blog_body, USE_OLD_VALUE, HBASE_DEFAULT_TIME));
            in_row->push_cmd("put", BOWENTABLE, blog_id, tcv_vec);
            */
        } else if (act == bus::DEL) {
            in_row->get_value_byindex(2, &blog_id, mylogger); //blog_id
            mylogger->error("UNEXCEPT bowen delete blog_id:%s", blog_id);
            //in_row->push_cmd("del", BOWENTABLE, blog_id);
            in_row->get_hbase_delete(BOWENTABLE, blog_id);
        }
    } else if (schema_id == 3) {
        //for bowen index
        //id(indexid) uid blog_id(rowkey) blog_class blog_pubdate blog_uppdate blog_pubip blog_uppip
        //x_pic_flag x_sizes x_cms_flag x_rank status is_top is_sign pubtime tj x_rank_1
        char *id, *uid, *blog_id, *blog_class, *blog_pubdate, *blog_uppdate, *blog_pubip, *blog_uppip;
        char *x_pic_flag, *x_sizes, *x_cms_flag, *x_rank, *status, *is_top, *is_sign, *pubtime, *tj, *x_rank_1;
        char *old_blog_id;
        bool ret;
        //std::vector<bus::tcolumnvalue_t> tcv_vec;
        
        if (act == bus::INSERT || act == bus::UPDATE) {
            in_row->get_value_byindex(2, &blog_id, mylogger); //blog_id rowkey
            if (blog_id[0] == '\0') {
                return PROCESS_IGNORE;
            }
            
            bus::hbase_cmd_t *hcmd = in_row->get_hbase_put(BOWENTABLE, blog_id, 0);

            //mylogger->debug("blog_id:%s", blog_id);
            //博文mysql中，没有NULL列，所以这里不判断获取的id、uid...为空
            ret = in_row->get_value_byindex(4, &blog_pubdate, mylogger); //blog_pubdate unix timestamp
            uint64_t timestamp = strtotime(blog_pubdate);

            ret = in_row->get_value_byindex(0, &id, mylogger); //id
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "idxid", id, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "idxid", id, timestamp, USE_OLD_VALUE);

            ret = in_row->get_value_byindex(1, &uid, mylogger); //uid
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "uid", uid, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "uid", uid, timestamp, USE_OLD_VALUE);

            if (act == bus::UPDATE) {
                in_row->get_old_value_byindex(2, &old_blog_id, mylogger); //blog_id
                if (blog_id == NULL || old_blog_id == NULL) {
                    mylogger->error("bowen update blog_id || old_blog_id is NULL");
                    return PROCESS_FATAL;
                } else if (strcmp(blog_id, old_blog_id)) {
                    mylogger->error("UNEXCEPT bowen update blog_id:%s old_blog_id:%s", blog_id, old_blog_id);
                }
            }

            ret = in_row->get_value_byindex(3, &blog_class, mylogger); //blog_class
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_class", blog_class, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "blog_class", blog_class, timestamp, USE_OLD_VALUE);

            //ret = in_row->get_value_byindex(4, &blog_pubdate, mylogger); //blog_pubdate unix timestamp
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_pubdate", blog_pubdate, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "blog_pubdate", blog_pubdate, timestamp, 1);

            ret = in_row->get_value_byindex(5, &blog_uppdate, mylogger); //blog_uppdate
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_uppdate", blog_uppdate, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "blog_uppdate", blog_uppdate, timestamp, 1);

            ret = in_row->get_value_byindex(6, &blog_pubip, mylogger); //blog_pubip
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_pubip", blog_pubip, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "blog_pubip", blog_pubip, timestamp, 1);

            ret = in_row->get_value_byindex(7, &blog_uppip, mylogger); //blog_uppip
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "blog_uppip", blog_uppip, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "blog_uppip", blog_uppip, timestamp, 1);

            ret = in_row->get_value_byindex(8, &x_pic_flag, mylogger); //x_pic_flag
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "x_pic_flag", x_pic_flag, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "x_pic_flag", x_pic_flag, timestamp, 1);

            ret = in_row->get_value_byindex(9, &x_sizes, mylogger); //x_sizes
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "x_sizes", x_sizes, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "x_sizes", x_sizes, timestamp, 1);

            ret = in_row->get_value_byindex(10, &x_cms_flag, mylogger); //x_cms_flag
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "x_cms_flag", x_cms_flag, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "x_cms_flag", x_cms_flag, timestamp, 1);

            ret = in_row->get_value_byindex(11, &x_rank, mylogger); //x_rank
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "x_rank", x_rank, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "x_rank", x_rank, timestamp, 1);

            ret = in_row->get_value_byindex(12, &status, mylogger); //status
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "status", status, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "status", status, timestamp, 1);

            ret = in_row->get_value_byindex(13, &is_top, mylogger); //is_top
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "is_top", is_top, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "is_top", is_top, timestamp, 1);

            ret = in_row->get_value_byindex(14, &is_sign, mylogger); //is_sign
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "is_sign", is_sign, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "is_sign", is_sign, timestamp, 1);

            ret = in_row->get_value_byindex(15, &pubtime, mylogger); //pubtime
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "pubtime", pubtime, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "pubtime", pubtime, timestamp, 1);

            ret = in_row->get_value_byindex(16, &tj, mylogger); //tj
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "tj", tj, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "tj", tj, timestamp, 1);

            ret = in_row->get_value_byindex(17, &x_rank_1, mylogger); //x_rank_1
            //tcv_vec.push_back(bus::tcolumnvalue_t("a", "x_rank_1", x_rank_1, USE_OLD_VALUE, timestamp));
            hcmd->add_columnvalue("a", "x_rank_1", x_rank_1, timestamp, 1);
            //in_row->push_cmd("put", BOWENTABLE, blog_id, tcv_vec);
        } else if (act == bus::DEL) {
            in_row->get_value_byindex(2, &blog_id, mylogger); //blog_id
            mylogger->error("UNEXCEPT bowen delete blog_id:%s", blog_id);
            //in_row->push_cmd("del", BOWENTABLE, blog_id, HBASE_DEFAULT_TIME);
            in_row->get_hbase_delete(BOWENTABLE, blog_id);
            return PROCESS_IGNORE;
        }
    } else {
        mylogger->error("PROCESS_IGNORE schema_id = %d", schema_id);
        return PROCESS_IGNORE;
    }
    return PROCESS_OK;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
