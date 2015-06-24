/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_redis_util.cc
 * @author haoning(haoning@staff.sina.com.cn)
 * @date 2014/06/17 15:32:46
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_redis_util.h"

namespace bus {
/*
void clear_batch_row(std::vector<row_t*> &batch_vec)
{
    for(std::vector<row_t*>::iterator it = batch_vec.begin();
            it != batch_vec.end();
            ++it)
    {
        row_t* prow = *it;
        delete prow;
    }
    batch_vec.clear();
}
*/
/*************************************/
bool bus_redis_t::init_connection()
{
    struct timeval timeout ={1, 500000};
    if (_redis_connection != NULL)
    {
        redisFree(_redis_connection);
        _redis_connection = NULL;
    }

    _redis_connection = redisConnectWithTimeout(_redis_ip,
            _redis_port, timeout);
    if (NULL == _redis_connection)
    {
        g_logger.error("connet to [%s:%d] fail", _redis_ip, _redis_port);
        return false;
    }else if (_redis_connection->err)
    {
        g_logger.error("connect to [%s:%d] fail, error:%s",
                _redis_ip, _redis_port, _redis_connection->errstr);
        redisFree(_redis_connection);
        _redis_connection = NULL;
        return false;
    }
    return true;
}


redisReply* bus_redis_t::_exec_rcommand(
        const char* redis_cmd, const char* redis_key,
        const char* redis_field, const char* redis_value)
{
    redisReply* result = NULL;
    if (NULL == redis_key)
    {
        result = static_cast<redisReply*>(
                redisCommand(_redis_connection, "%s", redis_cmd));
    } else {
        if (NULL == redis_field && NULL == redis_value)
        {
            result = static_cast<redisReply*>(
                    redisCommand(_redis_connection, "%s %s", redis_cmd, redis_key));
        }
        else if (NULL == redis_field && NULL != redis_value)
        {
            result = static_cast<redisReply*>(
                    redisCommand(_redis_connection, "%s %s %s",
                        redis_cmd, redis_key, redis_value));
        }
        else if (NULL != redis_field && NULL == redis_value)
        {
            result = static_cast<redisReply*>(
                    redisCommand(_redis_connection, "%s %s %s",
                        redis_cmd, redis_key, redis_field));
        }
        else if (NULL != redis_field && NULL != redis_value)
        {
            result = static_cast<redisReply*>(
                    redisCommand(_redis_connection, "%s %s %s %s",
                        redis_cmd, redis_key, redis_field, redis_value));
        }
    }
    return result;
}
int bus_redis_t::append_row(row_t* data)
{
    int ret;
    const char* redis_cmd = data->get_redis_cmd();
    const char* redis_key = data->get_redis_key();
    const char* redis_field = data->get_redis_field();
    const char* redis_value = data->get_redis_value();

    if (redis_cmd == NULL) return REDIS_ERR;
    
    if (NULL == redis_key)
    {
        ret = redisAppendCommand(_redis_connection, "%s", redis_cmd);
    }
    else
    {
        if (NULL == redis_field && NULL == redis_value)
        {
            ret = redisAppendCommand(_redis_connection, "%s %s",
                    redis_cmd, redis_key);
        }
        else if (NULL == redis_field && NULL != redis_value)
        {
            ret = redisAppendCommand(_redis_connection, "%s %s %s",
                    redis_cmd, redis_key, redis_value);
        }
        else if (NULL != redis_field && NULL == redis_value)
        {
            ret = redisAppendCommand(_redis_connection, "%s %s %s",
                    redis_cmd, redis_key, redis_field);
        }
        else if (NULL != redis_field && NULL != redis_value)
        {
            ret = redisAppendCommand(_redis_connection, "%s %s %s %s",
                    redis_cmd, redis_key, redis_field, redis_value);
        }
    }
    return ret;
}
bool bus_redis_t::append_batch_row(std::vector<row_t*> &batch_row)
{
    std::size_t sz = batch_row.size();
    for (int i = sz - 1; i >= 0; --i)
    {
        row_t *currow = batch_row[i];
        if (append_row(currow) != REDIS_OK)
        {
            std::string row_info;
            currow->print(row_info);
            g_logger.error("[%s:%d] append row fail,row=%s",
                    _redis_ip, _redis_port, row_info.c_str());
            return false;
        }
    }
    //g_logger.debug("buf=%s", _redis_connection->obuf);
    return true;
}
bool bus_redis_t::execute_batch_error(std::vector<row_t*> &batch_error, uint32_t statid)
{
    bus_stat_t &stat = g_bus.get_stat();
    redisReply *reply = NULL;
    while (!batch_error.empty())
    {
        row_t *data = batch_error.back();
        const char *redis_cmd = data->get_redis_cmd();
        const char *redis_key = data->get_redis_key();
        const char *redis_field = data->get_redis_field();
        const char *redis_value = data->get_redis_value();
        reply = _exec_rcommand(redis_cmd, redis_key, redis_field, redis_value);
        if (NULL == reply)
        {
            g_logger.error("[%s:%d] execute fail, [cmd=%s %s %s %s]",
                    _redis_ip, _redis_port,
                    redis_cmd, redis_key, redis_field, redis_value);
            return false;
        }
        if (reply->type == REDIS_REPLY_ERROR)
        {
            g_logger.error("[%s:%d] execute error:%s, [cmd=%s %s %s %s]", 
                    _redis_ip, _redis_port, reply->str,
                    redis_cmd, redis_key,  redis_field, redis_value);
            freeReplyObject(reply);
            return false;
        }

        if (data->get_src_schema()->get_record_flag())
            stat.incr_consumer_succ_count(statid, 1);
        freeReplyObject(reply);
        reply = NULL;
        row_t *row = batch_error.back();
        delete row;
        batch_error.pop_back();
    }
    return true;
}

bool bus_redis_t::execute_batch_row(std::vector<row_t*> &batch_row,
        uint32_t statid, std::vector<row_t*> &batch_error)
{
    bus_stat_t &stat = g_bus.get_stat();

    int ret;
    redisReply* reply = NULL;
    while (!batch_row.empty())
    {
        row_t *cur_row = batch_row.back();
        ret = redisGetReply(_redis_connection, (void**)&reply);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d] execute batch command fail, get reply fail",
                    _redis_ip, _redis_port);
            return false;
        }
        assert(reply != NULL);
        if (reply->type == REDIS_REPLY_ERROR)
        {
            batch_error.push_back(cur_row);
            std::string row_info;
            cur_row->print(row_info);
            g_logger.error("[%s:%d] rows execute error:%s, rows=%s",
                    _redis_ip, _redis_port, reply->str, row_info.c_str());
        } else {
            if (cur_row->get_src_schema()->get_record_flag())
                stat.incr_consumer_succ_count(statid, 1);
            delete cur_row;
        }
        freeReplyObject(reply);
        reply = NULL;
        batch_row.pop_back();
    }
    return true;
}
int bus_redis_t::execute_batch(std::vector<row_t*> &batch_row, 
        std::vector<row_t*> &batch_error, uint32_t statid)
{
    bus_stat_t &stat = g_bus.get_stat();
    bool first_flag = true;
    while (!*_stop_flag)
    {
        /* 第一次执行没有成功, 重新进行连接 */
        if (!first_flag && !init_connection())
        {
            stat.set_consumer_thread_state(statid, THREAD_FAIL);
            g_logger.error("[%s:%d] init connection fail, retry",
                    _redis_ip, _redis_port);
            sleep(SLEEP_TIME);
            continue;
        }
        stat.set_consumer_thread_state(statid, THREAD_RUNNING);
        first_flag = false;
        if (!batch_row.empty())
        {
            if (!append_batch_row(batch_row))
            {
                stat.set_consumer_thread_state(statid, THREAD_FAIL);
                g_logger.error("[%s:%d] append command fatal error, exit",
                        _redis_ip, _redis_port);
                return -1;
            }
            if (!execute_batch_row(batch_row, statid, batch_error))
            {
                stat.set_consumer_thread_state(statid, THREAD_FAIL);
                g_logger.error("[%s:%d] execute batch fail, retry",
                        _redis_ip, _redis_port);
                sleep(SLEEP_TIME);
                continue;
            }
        }

        if (!batch_error.empty())
        {
            if (!execute_batch_error(batch_error, statid))
            {
                stat.set_consumer_thread_state(statid, THREAD_FAIL);
                g_logger.error("[%s:%d] execute batch error fail, retry",
                        _redis_ip, _redis_port);
                sleep(SLEEP_TIME);
                continue;
            }
        }
        break;
    }
    return 0;
}
/***************************************************************/
bool bus_redis_t::append_rediscounter(row_t *data, int send_pos, bool update_flag)
{
    int pos = (send_pos == -1 ? 0 : send_pos);
    int ret, ret1, ret2, ret3, ret4;
    if (pos == 0)
    {
        ret = append_row(data);
        if (ret != REDIS_OK)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("[%s:%d], append row data fail,rows=%s", row_info.c_str());
            return false;
        }
    }

    if (update_flag)
    {
        char keyfile[128];
        char keyoffset[128];
        char keyrow[128];
        char keycmd[128];

        char valuefile[128];
        char valueoffset[128];
        char valuerow[128];
        char valuecmd[128];
        const char *bis = g_bus.get_config().get_bss();
        mysql_position_t &rowpos = data->get_position();
        std::vector<data_source_t*> &source_vec = g_bus.get_config().get_source();
        uint32_t src_partnum = data->get_src_partnum();
        data_source_t* ds = source_vec[src_partnum];

        snprintf(keyfile, sizeof(keyfile), "%s_fseq_%d", bis, ds->get_port());
        snprintf(keyoffset, sizeof(keyoffset), "%s_foff_%d", bis, ds->get_port());
        snprintf(keyrow, sizeof(keyrow), "%s_row_%d", bis, ds->get_port());
        snprintf(keycmd, sizeof(keycmd), "%s_cmd_%d", bis, ds->get_port());

        snprintf(valuefile, sizeof(valuefile), "%ld", rowpos.file_seq);
        snprintf(valueoffset, sizeof(valueoffset), "%ld", rowpos.binlog_offset);
        snprintf(valuerow, sizeof(valuerow), "%ld", rowpos.rowindex);
        snprintf(valuecmd, sizeof(valuecmd), "%ld", rowpos.cmdindex);

        ret1 = redisAppendCommand(_redis_connection, "set %s %s", keyfile, valuefile);
        ret2 = redisAppendCommand(_redis_connection, "set %s %s", keyoffset, valueoffset);
        ret3 = redisAppendCommand(_redis_connection, "set %s %s", keyrow, valuerow);
        ret4 = redisAppendCommand(_redis_connection, "set %s %s", keycmd, valuecmd);
        if (ret1 != REDIS_OK || ret2 != REDIS_OK || ret3 != REDIS_OK || ret4 != REDIS_OK)
        {
            g_logger.error("[%s:%d][%s:%d],append rediscounter position [%s:%s:%s:%s] fail", 
                    _redis_ip, _redis_port, ds->get_ip(), ds->get_port(),
                    valuefile, valueoffset, valuerow, valuecmd);
            return false;
        }
    }
    return true;
}
bool bus_redis_t::append_redis(row_t *data, int send_pos, bool update_flag)
{
    int pos = (send_pos == -1 ? 0 : send_pos);
    int ret;
    if (pos == 0)
    {
        ret = append_row(data);
        if (ret != REDIS_OK)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("[%s:%d], append row data fail,rows=%s", row_info.c_str());
            return false;
        }
    }

    if (update_flag)
    {
        char redis_value[128];
        char redis_key[128];
        std::vector<data_source_t*> &source_vec = g_bus.get_config().get_source();
        const char *bis = g_bus.get_config().get_bss();
        uint32_t src_partnum = data->get_src_partnum();
        data_source_t* ds = source_vec[src_partnum];
        mysql_position_t &rowpos = data->get_position();

        snprintf(redis_key, sizeof(redis_key), "%s_pos_%d",
                bis, ds->get_port());

        snprintf(redis_value, sizeof(redis_value), "%s:%lu:%lu:%lu",
                rowpos.binlog_file, rowpos.binlog_offset,
                rowpos.rowindex, rowpos.cmdindex);
        ret = redisAppendCommand(_redis_connection, "set %s %s", redis_key, redis_value);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d], append row position [set %s %s] fail", 
                    _redis_ip, _redis_port, redis_key, redis_value);
            return false;
        }
    }
    return true;
}
int bus_redis_t::execute_redis_row(row_t *data, int &send_pos, bool update_flag)
{
    int ret;
    std::size_t exec_pos = (send_pos == -1 ? 0 : send_pos);
    redisReply* reply = NULL;
    if (exec_pos == 0)
    {
        send_pos = 0;
        ret = redisGetReply(_redis_connection, (void**)&reply);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d] execute row data fail,get reply fail",
                    _redis_ip, _redis_port);
            return 1;
        }
        assert(reply != NULL);
        if(reply->type == REDIS_REPLY_ERROR)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("[%s:%d] execute incr row fail, error:%s, rows=%s",
                     _redis_ip, _redis_port, reply->str, row_info.c_str());
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
        freeReplyObject(reply);
        reply = NULL;
    }
    
    send_pos = 1;
    if (update_flag)
    {
        ret = redisGetReply(_redis_connection, (void**)&reply);
        mysql_position_t &rowpos = data->get_position();
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d] execute incr row set position [%s:%ld:%ld:%ld] fail",
                    _redis_ip, _redis_port, rowpos.binlog_file, 
                    rowpos.binlog_offset, rowpos.rowindex, rowpos.cmdindex);
            return 1;
        }
        assert(reply != NULL);
        if(reply->type == REDIS_REPLY_ERROR)
        {
            g_logger.error("[%s:%d] execute incr row set position [%s:%ld:%ld:%ld] fail",
                    _redis_ip, _redis_port, rowpos.binlog_file, 
                    rowpos.binlog_offset, rowpos.rowindex, rowpos.cmdindex);
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }
    return 0;
}
int bus_redis_t::execute_rediscounter_row(row_t *data, int &send_pos, bool update_flag)
{
    int ret,ret1,ret2,ret3,ret4;
    std::size_t exec_pos = (send_pos == -1 ? 0 : send_pos);
    redisReply* reply = NULL;
    redisReply* reply1 = NULL;
    redisReply* reply2 = NULL;
    redisReply* reply3 = NULL;
    redisReply* reply4 = NULL;
    if (exec_pos == 0)
    {
        send_pos = 0;
        ret = redisGetReply(_redis_connection, (void**)&reply);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d] execute row data fail,get reply(%u) fail", 
                    _redis_ip, _redis_port, ret);
            return 1;
        }
        assert(reply != NULL);
        if(reply->type == REDIS_REPLY_ERROR)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("[%s:%d] execute incr batch fail, error:%s, rows=%s",
                     _redis_ip, _redis_port, reply->str, row_info.c_str());
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
    }
    freeReplyObject(reply);
    reply = NULL;
    
    send_pos = 1;
    if (update_flag)
    {
        ret1 = redisGetReply(_redis_connection, (void**)&reply1);
        ret2 = redisGetReply(_redis_connection, (void**)&reply2);
        ret3 = redisGetReply(_redis_connection, (void**)&reply3);
        ret4 = redisGetReply(_redis_connection, (void**)&reply4);
        mysql_position_t &rowpos = data->get_position();
        if (ret1 != REDIS_OK || ret2 != REDIS_OK || ret3 != REDIS_OK || ret4 != REDIS_OK)
        {
            g_logger.error("[%s:%d] exec row set position [%s:%ld:%ld:%ld] fail",
                    _redis_ip, _redis_port, rowpos.binlog_file, 
                    rowpos.binlog_offset, rowpos.rowindex, rowpos.cmdindex);
            if (reply1 != NULL) freeReplyObject(reply1);
            if (reply2 != NULL) freeReplyObject(reply2);
            if (reply3 != NULL) freeReplyObject(reply3);
            if (reply4 != NULL) freeReplyObject(reply4);
            return 1;
        }
        if(reply1->type == REDIS_REPLY_ERROR ||
                reply2->type == REDIS_REPLY_ERROR ||
                reply3->type == REDIS_REPLY_ERROR ||
                reply4->type == REDIS_REPLY_ERROR)
        {
            g_logger.error("[%s:%d] exec row set position [%s:%ld:%ld:%ld] fail",
                    _redis_ip, _redis_port, rowpos.binlog_file, 
                    rowpos.binlog_offset, rowpos.rowindex, rowpos.cmdindex);
            freeReplyObject(reply1);
            freeReplyObject(reply2);
            freeReplyObject(reply3);
            freeReplyObject(reply4);
            return -1;
        }
        freeReplyObject(reply1);
        freeReplyObject(reply2);
        freeReplyObject(reply3);
        freeReplyObject(reply4);
    }
    return 0;
}

int bus_redis_t::execute_batch_incr(row_t *data, uint32_t statid)
{
    bus_config_t &config = g_bus.get_config();
    source_t dst_type = config.get_dstds_type();
    bus_stat_t &stat = g_bus.get_stat();
    bus_engine_t &engine = g_bus.get_engine();
    uint64_t update_num = engine.get_update_num();
    uint64_t succ_num = stat.get_consumer_succ_count(statid);
    bool update_flag = (succ_num%update_num) ? false:true;

    int send_pos = -1;
    while (!*_stop_flag)
    {
        /* 第一次执行没有成功, 重新进行连接 */
        if (send_pos != -1 && !init_connection())
        {
            stat.set_consumer_thread_state(statid, THREAD_FAIL);
            g_logger.error("init connection [%s:%d] fail, retry",
                    _redis_ip, _redis_port);
            sleep(SLEEP_TIME);
            continue;
        }
        stat.set_consumer_thread_state(statid, THREAD_RUNNING);
        const char *cmd = data->get_redis_cmd();
        if (cmd != NULL && !strcmp(cmd, "decr_and_del"))
        {
            int decr_ret = decr_and_del(data, send_pos, update_flag);
            if (decr_ret == -1 || decr_ret == 1) {
                stat.set_consumer_thread_state(statid, THREAD_FAIL);
                g_logger.error("[%s:%d] execute decr_and_del fail, retry",
                        _redis_ip, _redis_port);
                sleep(SLEEP_TIME);
                continue;
            } 
        } else {
            int batch_ret;
            if (dst_type == REDIS_DS) {
                if (!append_redis(data, send_pos, update_flag))
                {
                    g_logger.error("[%s:%d] append command fatal error, retry",
                            _redis_ip, _redis_port);
                    return -1;
                }
                batch_ret = execute_redis_row(data, send_pos, update_flag);
            } else if (dst_type == REDIS_COUNTER_DS) {
                if (!append_rediscounter(data, send_pos, update_flag))
                {
                    g_logger.error("[%s:%d] append command fatal error, retry",
                            _redis_ip, _redis_port);
                    return -1;
                }
                batch_ret = execute_rediscounter_row(data, send_pos, update_flag);
            }
            if (batch_ret == -1 || batch_ret == 1)
            {
                stat.set_consumer_thread_state(statid, THREAD_FAIL);
                g_logger.error("[%s:%d] execute batch fail, retry",
                        _redis_ip, _redis_port);
                sleep(SLEEP_TIME);
                continue;
            } 
        }
        break;
    }//while
    return 0;
}
/******************************************************************/
int bus_redis_t::execute_del_redis(row_t *data, bool del_flag, bool update_flag)
{
    redisReply* reply = NULL;
    int ret;
    if (del_flag) {
        ret = redisGetReply(_redis_connection, (void**)&reply);
        if (ret != REDIS_OK)
        {
            g_logger.error("execute del command fail,get reply fail");
            return 1;
        }
        assert(reply != NULL);
        if(reply->type == REDIS_REPLY_ERROR)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("execute del cmd error, error:%s, rows=%s",
                    reply->str, row_info.c_str());
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
        freeReplyObject(reply);
        reply = NULL;
    }

    if (update_flag)
    {
        ret = redisGetReply(_redis_connection, (void**)&reply);
        if (ret != REDIS_OK)
        {
            g_logger.error("execute set position command fail,get reply fail");
            return 1;
        }
        assert(reply != NULL);
        if(reply->type == REDIS_REPLY_ERROR)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("exec set pos commanderror, error:%s, rows=%s",
                    reply->str, row_info.c_str());
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
        freeReplyObject(reply);
        reply = NULL;
    }

    return 0;
}
int bus_redis_t::execute_del_rediscounter(row_t *data, bool del_flag, bool update_flag)
{
    redisReply* reply = NULL;
    redisReply *reply1 = NULL;
    redisReply *reply2 = NULL;
    redisReply *reply3 = NULL;
    redisReply *reply4 = NULL;
    int ret, ret1, ret2, ret3, ret4;
    if (del_flag) {
        ret = redisGetReply(_redis_connection, (void**)&reply);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d] exec del cmd fail, get reply fail", 
                    _redis_ip, _redis_port);
            return 1;
        }
        assert(reply != NULL);
        if(reply->type == REDIS_REPLY_ERROR)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("[%s:%d] exec del cmd error, error:%s, rows=%s",
                    _redis_ip, _redis_port, reply->str, row_info.c_str());
            freeReplyObject(reply);
            reply = NULL;
            return -1;
        }
        freeReplyObject(reply);
        reply = NULL;
    }

    if (update_flag)
    {
        ret1 = redisGetReply(_redis_connection, (void**)&reply1);
        ret2 = redisGetReply(_redis_connection, (void**)&reply2);
        ret3 = redisGetReply(_redis_connection, (void**)&reply3);
        ret4 = redisGetReply(_redis_connection, (void**)&reply4);
        assert(reply1 != NULL && reply2 != NULL && reply3 != NULL && reply4 != NULL);
        if (ret1 != REDIS_OK || ret2 != REDIS_OK || ret3 != REDIS_OK || ret4 != REDIS_OK)
        {
            g_logger.error("[%s:%d] exec set pos fail, get reply fail",
                    _redis_ip, _redis_port);
            if (reply1 != NULL) freeReplyObject(reply1);
            if (reply2 != NULL) freeReplyObject(reply2);
            if (reply3 != NULL) freeReplyObject(reply3);
            if (reply4 != NULL) freeReplyObject(reply4);
            return 1;
        }
        if(reply1->type == REDIS_REPLY_ERROR ||
                reply2->type == REDIS_REPLY_ERROR ||
                reply3->type == REDIS_REPLY_ERROR ||
                reply4->type == REDIS_REPLY_ERROR)
        {
            std::string row_info;
            data->print(row_info);
            g_logger.error("set pos exec fail,error:%s, rows=%s",
                    reply->str, row_info.c_str());
            freeReplyObject(reply1);
            freeReplyObject(reply2);
            freeReplyObject(reply3);
            freeReplyObject(reply4);
            return -1;
        }
        freeReplyObject(reply1);
        freeReplyObject(reply2);
        freeReplyObject(reply3);
        freeReplyObject(reply4);
    }
    return 0;
}
bool bus_redis_t::append_del_rediscounter(row_t *data, bool del_flag, bool update_flag)
{
    int ret, ret1, ret2, ret3, ret4;
    if (del_flag) {
        const char* redis_cmd_key = data->get_redis_key();
        ret = redisAppendCommand(_redis_connection, "del %s", redis_cmd_key);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d], append [del %s] fail", 
                    _redis_ip, _redis_port, redis_cmd_key);
            return false;
        }
    }
    
    if (update_flag)
    {
        char keyfile[128];
        char keyoffset[128];
        char keyrow[128];
        char keycmd[128];

        char valuefile[128];
        char valueoffset[128];
        char valuerow[128];
        char valuecmd[128];
        const char *bis = g_bus.get_config().get_bss();
        mysql_position_t &rowpos = data->get_position();
        std::vector<data_source_t*> &source_vec = g_bus.get_config().get_source();
        uint32_t src_partnum = data->get_src_partnum();
        data_source_t* ds = source_vec[src_partnum];

        snprintf(keyfile, sizeof(keyfile), "%s_fseq_%d", bis, ds->get_port());
        snprintf(keyoffset, sizeof(keyoffset), "%s_foff_%d", bis, ds->get_port());
        snprintf(keyrow, sizeof(keyrow), "%s_row_%d", bis, ds->get_port());
        snprintf(keycmd, sizeof(keycmd), "%s_cmd_%d", bis, ds->get_port());

        snprintf(valuefile, sizeof(valuefile), "%ld", rowpos.file_seq);
        snprintf(valueoffset, sizeof(valueoffset), "%ld", rowpos.binlog_offset);
        snprintf(valuerow, sizeof(valuerow), "%ld", rowpos.rowindex);
        snprintf(valuecmd, sizeof(valuecmd), "%ld", rowpos.cmdindex);

        ret1 = redisAppendCommand(_redis_connection, "set %s %s", keyfile, valuefile);
        ret2 = redisAppendCommand(_redis_connection, "set %s %s", keyoffset, valueoffset);
        ret3 = redisAppendCommand(_redis_connection, "set %s %s", keyrow, valuerow);
        ret4 = redisAppendCommand(_redis_connection, "set %s %s", keycmd, valuecmd);
        if (ret1 != REDIS_OK || ret2 != REDIS_OK || ret3 != REDIS_OK || ret4 != REDIS_OK)
        {
            g_logger.error("[%s:%d][%s:%d],append rediscounter position [%s:%s:%s:%s] fail", 
                    _redis_ip, _redis_port, ds->get_ip(), ds->get_port(),
                    valuefile, valueoffset, valuerow, valuecmd);
            return false;
        }
    }
    return true;
}
bool bus_redis_t::append_del_redis(row_t *data, bool del_flag, bool update_flag)
{
    int ret;
    if (del_flag) {
        const char* redis_cmd_key = data->get_redis_key();
        ret = redisAppendCommand(_redis_connection, "del %s", redis_cmd_key);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d], append [del %s] fail", 
                    _redis_ip, _redis_port, redis_cmd_key);
            return false;
        }
    }

    if (update_flag)
    {
        char redis_value[128];
        char redis_key[128];
        std::vector<data_source_t*> &source_vec = g_bus.get_config().get_source();
        const char *bis = g_bus.get_config().get_bss();
        uint32_t src_partnum = data->get_src_partnum();
        data_source_t* ds = source_vec[src_partnum];
        mysql_position_t &rowpos = data->get_position();

        snprintf(redis_key, sizeof(redis_key), "%s_pos_%d",
                bis, ds->get_port());

        snprintf(redis_value, sizeof(redis_value), "%s:%lu:%lu:%lu",
                rowpos.binlog_file, rowpos.binlog_offset,
                rowpos.rowindex, rowpos.cmdindex);
        ret = redisAppendCommand(_redis_connection, "set %s %s", redis_key, redis_value);
        if (ret != REDIS_OK)
        {
            g_logger.error("[%s:%d], append row position [set %s %s] fail", 
                    _redis_ip, _redis_port, redis_key, redis_value);
            return false;
        }
    }
    return true;
}

int bus_redis_t::decr_and_del(row_t *data, int &send_pos, bool update_flag)
{
    const char* redis_cmd = data->get_redis_cmd();
    assert(!strcmp(redis_cmd, "decr_and_del"));
    BUS_NOTUSED(redis_cmd);
    const char* redis_key = data->get_redis_key();
    const char* redis_value = data->get_redis_value();
    
    const char *decr_cmd = "decrby";
    redisReply *reply = NULL;
    int exec_pos = (send_pos < 0 ? 0 : send_pos);
    if (exec_pos == 0)
    {
        send_pos = 0;
        reply = _exec_rcommand(decr_cmd, redis_key, NULL, redis_value);
        if (NULL == reply)
        {
            g_logger.error("[%s:%d] fail to execute [cmd=%s %s %s]",
                    _redis_ip, _redis_port, decr_cmd, redis_key, redis_value);
            return 1;
        }
        if (reply->type == REDIS_REPLY_ERROR)
        {
            g_logger.error("[%s:%d] execute [cmd=%s %s %s] fail, error:%s",
                    _redis_ip, _redis_port, decr_cmd, redis_key, redis_value,
                    reply->str);
            freeReplyObject(reply);
            reply = NULL;
            return 1;
        }
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            long long decr_ret = reply->integer;
            if (decr_ret <= 0) {
                send_pos = 1;
            } else {
                send_pos = 2;
            }
        }
        freeReplyObject(reply);
        reply = NULL;
    }
    bool del_flag = false;
    if (send_pos == 1 || exec_pos == 1)
    {
        del_flag = true;
    }
    int ret;
    bus_config_t &config = g_bus.get_config();
    source_t dst_type = config.get_dstds_type();
    if (dst_type == REDIS_DS) {
        if (!append_del_redis(data, del_flag, update_flag)) {
            g_logger.error("append redis del pos fail");
            return -1;
        }
        ret = execute_del_redis(data, del_flag, update_flag);
    } else if (dst_type == REDIS_COUNTER_DS) {
        if (!append_del_rediscounter(data, del_flag, update_flag)) {
            g_logger.error("append rediscounter del pos fail");
            return -1;
        }
        ret = execute_del_rediscounter(data, del_flag, update_flag);
    }
    return ret;
}
}//namespace bus

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
