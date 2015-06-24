/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file data_source.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/24 06:27:23
 * @version $Revision$
 * @brief 
 *  
 **/
#include <string.h>
#include "data_source.h"
#include "bus_wrap.h"

namespace bus{
extern bus_t g_bus;
bool parse_position(std::string &str, std::string &filename,
        std::string &offset, std::string &rowoffset, std::string &cmdoffset)
{
    std::size_t matchpos = str.find(':');
    if (matchpos == std::string::npos)
    {
        return false;
    }
    filename = str.substr(0, matchpos);
    
    std::size_t matchpos1 = str.find(':', matchpos + 1);
    if (matchpos1 == std::string::npos)
    {
        return false;
    }

    std::size_t matchpos2 = str.find(':', matchpos1 + 1);
    if (matchpos2 == std::string::npos)
    {
        return false;
    }

    offset = str.substr(matchpos + 1, matchpos1 - matchpos - 1);
    rowoffset = str.substr(matchpos1 + 1, matchpos2 - matchpos1 - 1);
    cmdoffset = str.substr(matchpos2 + 1);
    if (filename.size() == 0 ||
            offset.size() == 0 || rowoffset.size() == 0 ||
            cmdoffset.size() == 0)
    {
        return false;
    }
    
    return true;
}
bool parse_target_source(std::string &str, std::string &ip,
        std::string &port)
{
    std::size_t matchpos = str.find(':');
    if (matchpos == std::string::npos)
    {
        return false;
    }
    ip = str.substr(0, matchpos);
    port = str.substr(matchpos + 1);
    if (ip.size() == 0 || 
            port.size() == 0)
    {
        return false;
    }
    return true;
}
/**********************************************************************/
bool data_source_t::set_position(const char *filename,  const char *pos,
        const char *rowoffset, const char*cmdoffset, const char *prefix, bool force)
{
    if (force || _sourcepos.empty())
    {
        return _sourcepos.set_position(filename, pos, rowoffset, cmdoffset, prefix);
    }
    
    mysql_position_t newpos;
    newpos.set_position(filename, pos, rowoffset, cmdoffset, prefix);
    int ret = _sourcepos.compare_position(newpos);
    if (ret > 0)
    {
        return _sourcepos.set_position(filename, pos, rowoffset, cmdoffset, prefix);
    }
    return true;
}
bool data_source_t::set_position(const char *filename,  long pos,
        long rowoffset, long cmdoffset, bool force)
{
    if (force || _sourcepos.empty())
    {
        return _sourcepos.set_position(filename, pos, rowoffset, cmdoffset);
    }

    int ret = _sourcepos.compare_position(filename, pos, rowoffset, cmdoffset);
    if (ret > 0)
    {
        return _sourcepos.set_position(filename, pos, rowoffset, cmdoffset);
    }
    return true;
}

bool data_source_t::construct_hash_table()
{
    if (_type != MYSQL_DS)
    {
        return false;
    }
    if (_table_charset != NULL)
    {
        delete _table_charset;
        _table_charset = NULL;
    }
    _table_charset = new(std::nothrow) hashtable_t<std::string>(HASH_TABLE_SIZE);
    if (_table_charset == NULL) oom_error();

    bus_config_t &config = g_bus.get_config();
    const char *user = config.get_user_name();
    const char *pwd = config.get_user_pwd();
    const char *charset = config.get_charset();
    bus_mysql_t mconn(_ip, _port, user, pwd);
    if (!mconn.init(charset))
    {
        g_logger.error("init ip=%s,port=%d mysql connection fail",
                _ip, _port);
        return false;
    }
    if (!mconn.select_table_charset(this))
    {
        g_logger.error("get charset from ip=%s,port=%dmysql datasource fail",
                _ip, _port);
        return false;
    }
    return true;
}
bool data_source_t::check_struct(schema_t *sch,
        std::vector<db_object_id> &data)
{
    if (_type != MYSQL_DS)
    {
        return false;
    }
    bus_config_t &config = g_bus.get_config();
    const char *user = config.get_user_name();
    const char *pwd = config.get_user_pwd();
    const char *charset = config.get_charset();
    bus_mysql_t mconn(_ip, _port, user, pwd);
    if (!mconn.init(charset))
    {
        g_logger.error("%s:%d init mysql connection fail",
                _ip, _port);
        return false;
    }

    if (!mconn.check_table_struct(sch, data))
    {
        g_logger.error("%s:%d check table struct fail",
                _ip, _port);
        return false;
    }
    return true;
}

data_source_t::~data_source_t()
{
    pthread_mutex_destroy(&src_mutex);
    if (_table_charset != NULL)
    {
        delete _table_charset;
        _table_charset = NULL;
    }
}
/*********************************************************************/
bus_master_t::~bus_master_t()
{
    if (_context != NULL)
    {
        redisFree(_context);
        _context = NULL;
    }
}

bool bus_master_t::init()
{
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    uint32_t i = 0;
    
    while(i < 3)
    {
        _context = redisConnectWithTimeout(_ip, _port, tv);
        if (_context != NULL && _context->err == 0)
        {
           //g_logger.debug("connect to redis [%s:%d] succ", _ip, _port);
           return true; 
        }
        g_logger.error("connect to redis, ip = %s, port = %d fail", _ip, _port);
        ++i;
    }
    g_logger.error("init master redis,ip=%s, port=%d fail", _ip, _port);
    return false;
}

int bus_master_t::get_source(const char *bis, const char *ip, int32_t port,
        std::vector<data_source_t*> &source, const source_t &srctype)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_task_%s_%d", bis, ip, port);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "smembers %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute command=[smembers %s] to get source fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("source datasource [key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_ARRAY)
    {
       uint32_t sz = r->elements;
       if (sz == 0)
       {
           g_logger.error("source datasource [key=%s] is not exist", key);
           freeReplyObject(r);
           return MASTER_REDIS_EMPTY;
       }

       for (uint32_t i = 0; i < sz; ++i)
       {
            redisReply *reply = r->element[i];
            assert(reply != NULL);
            if(reply->type == REDIS_REPLY_STRING)
            {
                std::size_t len = static_cast<std::size_t>(reply->len);
                std::string adr(reply->str);
                std::size_t sep = adr.find(':');
                if (sep == std::string::npos || sep >= len - 1)
                {
                    g_logger.error("datasource [key=%s], [address=%s] is invalid",
                            key, adr.c_str());
                    return false;
                }
                std::string ip = adr.substr(0, sep);
                std::string port = adr.substr(sep + 1, len - 1 - sep);
                long iport;
                if (!str2num(port.c_str(), iport))
                {
                    g_logger.error("port=%s is invalid", port.c_str());
                    return false;
                }
                data_source_t *src_ptr = new(std::nothrow) data_source_t(
                        ip.c_str(), (int)iport, srctype);
                if (src_ptr == NULL) oom_error();
                source.push_back(src_ptr);
            }
       }
    }
    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}

int bus_master_t::get_all_source_schema(const char *bis, std::vector<schema_t*> &source_schema) 
{
    char key[128];
    //清空source_schema
    std::size_t sz = source_schema.size();
    for (std::size_t i = 0; i < sz; ++i)
    {
        delete source_schema[i];
        source_schema[i] = NULL;
    }
    source_schema.clear();

    snprintf(key, sizeof(key), "%s_schema", bis);
    //g_logger.debug("----------SCHEMA KEY [%s]", key);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "hgetall %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute command=[hgetall %s] to get source schema fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("source schema [key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_ARRAY)
    {
       uint32_t sz = r->elements;
       if (sz == 0 || sz%2)
       {
           g_logger.error("source schema [key=%s size=%d] is invalid", key, sz);
           freeReplyObject(r);
           return MASTER_REDIS_EMPTY;
       }

       for (uint32_t i = 0; i < sz; ++i)
       {
            redisReply *reply = r->element[i];
            assert(reply != NULL);
            if(reply->type == REDIS_REPLY_STRING)
            {
                char str[1024];
                long ischema_id;
                snprintf(str, sizeof(str), "%s", reply->str);
                if (0 == i%2) {
                    char *schema_id = str;
                    if (NULL == schema_id)
                    {
                        g_logger.error("source schema schema_id [key=%s] is invalid", key);
                        freeReplyObject(r);
                        return false;
                    }
                    if (!str2num(schema_id, ischema_id))
                    {
                        g_logger.error("schema_id=%s is invalid", schema_id);
                        freeReplyObject(r);
                        return false;
                    }
                    continue;
                } else {
                    char *schema_table = strtok(str, ":");
                    if (NULL == schema_table) {
                        g_logger.error("source schema schema_table [key=%s], [schema=%s] is invalid",
                                key, reply->str);
                        freeReplyObject(r);
                        return false;
                    }

                    char *schema_name = strtok(NULL, ":");
                    if (NULL == schema_name)
                    {
                        g_logger.error("source schema schema_name [key=%s], [schema=%s] is invalid",
                                key, reply->str);
                        freeReplyObject(r);
                        return false;
                    }

                    schema_t *schema = new (std::nothrow) schema_t(ischema_id, schema_table, schema_name);
                    if (NULL == schema) oom_error();

                    char *record = strtok(NULL, ":");
                    if (NULL == record) {
                        g_logger.error("source schema boolean record [key=%s], [schema=%s] is invalid",
                                key, reply->str);
                        freeReplyObject(r);
                        return false;
                    }

                    if (!strcmp(record, "yes")) {
                        schema->set_record_flag(true);
                    } else {
                        schema->set_record_flag(false);
                    }

                    char *columns = strtok(NULL, "#");
                    if (NULL == columns) {
                        g_logger.error("source schema columns [key=%s], [schema=%s] is invalid",
                                key, reply->str);
                        freeReplyObject(r);
                        return false;
                    }
                    g_logger.debug("shema_t %d:%s:%s", ischema_id, schema_table, schema_name);
                    //'*'表示获取所有列的数据，设置标志到schema中
                    if ('*' == *columns) {
                        g_logger.debug("column_t %s", columns);
                        schema->set_all_columns_flag(true);
                    } else {
                        //获取所有列存储到schema_t::vector<column_t*>
                        column_t *tmpcol;
                        while (NULL != columns) {
                            g_logger.debug("column_t %s", columns);
                            tmpcol = new (std::nothrow)column_t(columns);
                            if (NULL == tmpcol) oom_error();
                            schema->add_column(tmpcol);
                            columns = strtok(NULL, "#");
                        }
                    }
                    source_schema.push_back(schema);
                }
            }
       }
    }
    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}

bool bus_master_t::set_rediscounter_full_position()
{
    bus_config_t &config = g_bus.get_config();
    const char *bis = config.get_bss();
    std::vector<data_source_t*> &src_source  = config.get_source();
    char keyfile[128];
    char keyoffset[128];
    char keyrow[128];
    char keycmd[128];

    char valuefile[128];
    char valueoffset[128];
    char valuerow[128];
    char valuecmd[128];
    for (std::vector<data_source_t*>::iterator it = src_source.begin();
            it != src_source.end();
            ++it)
    {
        data_source_t *src = *it;
        const char *src_ip = src->get_ip();
        const int src_port = src->get_port();
        mysql_position_t &srcpos = src->get_position();
        snprintf(keyfile, sizeof(keyfile), "%s_fseq_%d", bis, src_port);
        snprintf(keyoffset, sizeof(keyoffset), "%s_foff_%d", bis, src_port);
        snprintf(keyrow, sizeof(keyrow), "%s_row_%d", bis, src_port);
        snprintf(keycmd, sizeof(keycmd), "%s_cmd_%d", bis, src_port);

        snprintf(valuefile, sizeof(valuefile), "%ld", srcpos.file_seq);
        snprintf(valueoffset, sizeof(valueoffset), "%ld", srcpos.binlog_offset);
        snprintf(valuerow, sizeof(valuerow), "%ld", srcpos.rowindex);
        snprintf(valuecmd, sizeof(valuecmd), "%ld", srcpos.rowindex);

        redisReply *r1 = static_cast<redisReply*>(redisCommand(_context, "set %s %s", keyfile, valuefile));
        redisReply *r2 = static_cast<redisReply*>(redisCommand(_context, "set %s %s", keyoffset, valueoffset));
        redisReply *r3 = static_cast<redisReply*>(redisCommand(_context, "set %s %s", keyrow, valuerow));
        redisReply *r4 = static_cast<redisReply*>(redisCommand(_context, "set %s %s", keycmd, valuecmd));
        if (r1 == NULL || r2 == NULL || r3 == NULL || r4 == NULL)
        {
            g_logger.error("[%s:%d] set [%s:%d] position fail",
                    _ip, _port, src_ip, src_port);
            if (r1 != NULL) freeReplyObject(r1);
            if (r2 != NULL) freeReplyObject(r2);
            if (r3 != NULL) freeReplyObject(r3);
            if (r4 != NULL) freeReplyObject(r4);
            return false;
        }
        
        if (r1->type == REDIS_REPLY_ERROR ||
                r2->type == REDIS_REPLY_ERROR ||
                r3->type == REDIS_REPLY_ERROR ||
                r4->type == REDIS_REPLY_ERROR)
        {
            g_logger.error("[%s:%d] set [%s:%d] position fail",
                    _ip, _port, src_ip, src_port);
            freeReplyObject(r1);
            freeReplyObject(r2);
            freeReplyObject(r3);
            freeReplyObject(r4);
            return false;
        }
        g_logger.notice("[%s:%d] set [%s:%d] position [%s:%s:%s:%s] succ",
                _ip, _port, src_ip, src_port,
                valuefile, valueoffset, valuerow, valuecmd);
        freeReplyObject(r1);
        freeReplyObject(r2);
        freeReplyObject(r3);
        freeReplyObject(r4);
    }
    return true;
}

bool bus_master_t::set_redis_full_position()
{
    bus_config_t &config = g_bus.get_config();
    const char *bis = config.get_bss();
    std::vector<data_source_t*> &src_source  = config.get_source();
    char key[128];
    char value[128];
    for (std::vector<data_source_t*>::iterator it = src_source.begin();
            it != src_source.end();
            ++it)
    {
        data_source_t *src = *it;
        const char *src_ip = src->get_ip();
        const int src_port = src->get_port();
        mysql_position_t &srcpos = src->get_position();
        snprintf(key, sizeof(key), "%s_pos_%d", bis, src_port);
        snprintf(value, sizeof(value), "%s:%ld:%ld:%ld",
                srcpos.binlog_file, srcpos.binlog_offset, srcpos.rowindex, srcpos.cmdindex);
        redisReply *r = static_cast<redisReply*>(redisCommand(_context, "set %s %s", key, value));
        if (r == NULL || r->type == REDIS_REPLY_ERROR)
        {
            g_logger.error("[%s:%d][%s:%d] set command=[set %s %s]",
                    _ip, _port, src_ip, src_port, key, value);
            if (r != NULL)
            {
                freeReplyObject(r);
                r = NULL;
            }
            return false;
        }
        g_logger.notice("[%s:%d][%s:%d] [set %s %s] succ",
                _ip, _port, src_ip, src_port, key, value);
        freeReplyObject(r);
    }
    return true;
}

int bus_master_t::get_dst(const char *bis, std::vector<data_source_t*> &dst,
        const source_t &dsttype)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_target", bis);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context,
                "zrangebyscore %s -inf +inf withscores", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute [command=smembers %s] to get target datasource fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("target [key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    char scorebuf[5];
    if (r->type == REDIS_REPLY_ARRAY)
    {
       uint32_t sz = r->elements;
       if (sz == 0)
       {
           g_logger.error("target ds [key=%s] is empty", key);
           freeReplyObject(r);
           return MASTER_REDIS_EMPTY;
       }
       for (uint32_t i = 0; i < sz; ++i)
       {
           redisReply *reply = r->element[i];
           assert(reply != NULL);
           if(reply->type == REDIS_REPLY_STRING)
           {
               if (i % 2 == 0)
               {
                   std::string adr(reply->str);
                   std::string str_ip, str_port;
                   if (!parse_target_source(adr, str_ip, str_port))
                   {
                       g_logger.error("target datasource [key=%s] [address=%s] is invalid",
                               key, adr.c_str());
                       freeReplyObject(r);
                       return MASTER_REDIS_DATA_FAIL;
                   }
                   long iport;
                   if (!str2num(str_port.c_str(), iport))
                   {
                       g_logger.error("port=%s is invalid", str_port.c_str());
                       freeReplyObject(r);
                       return MASTER_REDIS_DATA_FAIL;
                   }
                   data_source_t *dst_ptr = new(std::nothrow) data_source_t(str_ip.c_str(),
                           (int)iport, dsttype, 1);
                   if (dst_ptr == NULL) oom_error();
                   dst.push_back(dst_ptr);
               } else {
                   snprintf(scorebuf, sizeof(scorebuf), "%u", (i-1)/2);
                   if (strcmp(reply->str, scorebuf))
                   {
                       g_logger.error("target source partnum %s is invalid", reply->str);
                       freeReplyObject(r);
                       return MASTER_REDIS_DATA_FAIL;
                   }
               }
           }
       }
    }
    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}
int bus_master_t::get_ds_online_flag(const char *bis, data_source_t *ds)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_online_%s_%d", bis, ds->get_ip(), ds->get_port());
    
    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("get target online flag fail, execute [command=get %s]", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("get target online flag fail,[key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_STRING)
    {
        if (!strcmp(r->str, "yes")) {
            ds->set_online_flag(1);
        } else if (!strcmp(r->str, "no")) {
            ds->set_online_flag(0);
        } else {
            g_logger.error("get target online flag fail, [online_flag=%s] is invalid", r->str);
            freeReplyObject(r);
            return MASTER_REDIS_FAIL;
        }
    }
    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}
int bus_master_t::get_user_info(const char *bis, char *user, int user_len,
        char *pwd, int pwd_len)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_user", bis);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("get postion, execute [command=get %s]", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_STRING)
    {
        assert(r->str != NULL);
        std::size_t len = static_cast<std::size_t>(r->len);
        std::string cur_position(r->str);
        std::size_t sep = cur_position.find(':');
        if (sep == std::string::npos || sep >= len - 1)
        {
            g_logger.error("[key=%s] [position=%s] is invalid", key, cur_position.c_str());
            freeReplyObject(r);
            return MASTER_REDIS_EMPTY;
        }
        std::string muser = cur_position.substr(0, sep);
        std::string mpwd = cur_position.substr(sep + 1, len - 1 - sep);
        snprintf(user, user_len, "%s", muser.c_str());
        snprintf(pwd, pwd_len, "%s", mpwd.c_str());
    }

    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}

int bus_master_t::get_rediscounter_position(const char *bis,
        data_source_t *ds, mysql_position_t *conn_pos)
{
    const char *srcip = ds->get_ip();
    const int32_t srcport = ds->get_port();

    char keyfile[128];
    char keyoffset[128];
    char keyrow[128];
    char keycmd[128];
    snprintf(keyfile, sizeof(keyfile), "%s_fseq_%d", bis, srcport);
    snprintf(keyoffset, sizeof(keyoffset), "%s_foff_%d", bis, srcport);
    snprintf(keyrow, sizeof(keyrow), "%s_row_%d", bis, srcport);
    snprintf(keycmd, sizeof(keycmd), "%s_cmd_%d", bis, srcport);
    redisReply *r1 = static_cast<redisReply*>(redisCommand(_context, "get %s", keyfile));
    redisReply *r2 = static_cast<redisReply*>(redisCommand(_context, "get %s", keyoffset));
    redisReply *r3 = static_cast<redisReply*>(redisCommand(_context, "get %s", keyrow));
    redisReply *r4 = static_cast<redisReply*>(redisCommand(_context, "get %s", keycmd));
    if (r1 == NULL || r2 == NULL || r3 == NULL || r4 == NULL)
    {
        g_logger.error("[%s:%d][%s:%d] get position fail",
                _ip, _port, srcip, srcport);
        if (r1 != NULL) freeReplyObject(r1);
        if (r2 != NULL) freeReplyObject(r2);
        if (r3 != NULL) freeReplyObject(r3);
        if (r4 != NULL) freeReplyObject(r4);
        return MASTER_REDIS_FAIL;
    }

    if (r1->type == REDIS_REPLY_ERROR ||
            r2->type == REDIS_REPLY_ERROR ||
            r3->type == REDIS_REPLY_ERROR ||
            r4->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("[%s:%d][%s:%d] get position fail",
                _ip, _port, srcip, srcport);
        freeReplyObject(r1);
        freeReplyObject(r2);
        freeReplyObject(r3);
        freeReplyObject(r4);
        return MASTER_REDIS_FAIL;
    }

    if(r1->type == REDIS_REPLY_NIL ||
            r2->type == REDIS_REPLY_NIL ||
            r3->type == REDIS_REPLY_NIL ||
            r4->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[%s:%d],[%s:%d][%s:%s:%s:%s] is not exist",
                _ip, _port, srcip, srcport, keyfile, keyoffset, keyrow, keycmd);
        freeReplyObject(r1);
        freeReplyObject(r2);
        freeReplyObject(r3);
        freeReplyObject(r4);
        return MASTER_REDIS_EMPTY;
    }
    const char *fileseq = r1->str;
    const char *fileoff = r2->str;
    const char *row = r3->str;
    const char *cmd = r4->str;
    
    bus_config_t &config = g_bus.get_config();
    const char *binlog_prefix = config.get_binlog_prefix();
    
    if (conn_pos != NULL)
        conn_pos->set_position(fileseq, fileoff, row, cmd, binlog_prefix);
    ds->set_position(fileseq, fileoff, row, cmd, binlog_prefix, false);

    freeReplyObject(r1);
    freeReplyObject(r2);
    freeReplyObject(r3);
    freeReplyObject(r4);
    return MASTER_REDIS_SUCC;
}
int bus_master_t::get_redis_position(const char *bis,
        data_source_t *ds, mysql_position_t *pos)
{
    const char *srcip = ds->get_ip();
    const int32_t srcport = ds->get_port();

    char key[128];
    snprintf(key, sizeof(key), "%s_pos_%d", bis, srcport);
    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("[%s:%d][%s:%d] get postion fail",
                _ip, _port, srcip, srcport);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[%s:%d][%s:%d][key=%s] is not exist",
                _ip, _port, srcip, srcport, key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_STRING)
    {
        assert(r->str != NULL);
        std::string cur_position(r->str);
        std::string filename, offset, rowoffset, cmdoffset; 
        if(!parse_position(cur_position, filename, offset, rowoffset, cmdoffset)) {
            g_logger.error("[%s:%d][%s:%d][key=%s] [position=%s] is invalid",
                    _ip, _port, srcip, srcport, key, cur_position.c_str());
            freeReplyObject(r);
            return MASTER_REDIS_EMPTY;
        }
        long pos_num, row_num, cmd_num;
        if (!str2num(offset.c_str(), pos_num))
        {
            g_logger.error("get position from redis fail,pos=%s can't convert to int",
                    offset.c_str());
            return  MASTER_REDIS_POS_FAIL;
        }
        if (!str2num(rowoffset.c_str(), row_num))
        {
            g_logger.error("get position from redis fail,"
                    "row offset=%s can't convert to int",
                    rowoffset.c_str());
            return  MASTER_REDIS_POS_FAIL;
        }
        if (!str2num(cmdoffset.c_str(), cmd_num))
        {
            g_logger.error("get position from redis fail,"
                    "cmd offset=%s can't convert to int",
                    cmdoffset.c_str());
            return  MASTER_REDIS_POS_FAIL;
        }
        if (pos != NULL)
            pos->set_position(filename.c_str(), pos_num, row_num, cmd_num);
        ds->set_position(filename.c_str(), pos_num, row_num, cmd_num, 0);
    }

    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}

int bus_master_t::get_all_source_type(const char *bis, source_t &src_type, source_t &dst_type)
{
    char keyup[128];
    char keydown[128];
    snprintf(keyup, sizeof(keyup), "%s_source_type", bis);
    snprintf(keydown, sizeof(keydown), "%s_target_type", bis);

    redisReply *r1 = static_cast<redisReply*>(redisCommand(_context, "get %s", keyup));
    redisReply *r2 = static_cast<redisReply*>(redisCommand(_context, "get %s", keydown));
    if (r1 == NULL || r2 == NULL)
    {
        g_logger.error("get up/down source type fail");
        if (r1 != NULL) freeReplyObject(r1);
        if (r2 != NULL) freeReplyObject(r2);
        return MASTER_REDIS_FAIL;
    }
    if (r1->type == REDIS_REPLY_ERROR || r2->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("get up/down source type fail,upkey=%s, downkey=%s", 
                keyup, keydown);
        freeReplyObject(r1);
        freeReplyObject(r2);
        return MASTER_REDIS_FAIL;
    }

    if(r1->type == REDIS_REPLY_NIL || r2->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[key=%s] or [key=%s] is not exist", keyup, keydown);
        freeReplyObject(r1);
        freeReplyObject(r2);
        return MASTER_REDIS_EMPTY;
    }
    
    int src_type_num = translate_source_type(r1->str);
    int dst_type_num = translate_source_type(r2->str);
    if (src_type_num == BUS_FAIL || dst_type_num == BUS_FAIL)
    {
        g_logger.error("source [%s] or [%s] is invalid",
                r1->str, r2->str);
        return false;
    }
    src_type = static_cast<source_t>(src_type_num);
    dst_type = static_cast<source_t>(dst_type_num);

    freeReplyObject(r1);
    freeReplyObject(r2);
    return MASTER_REDIS_SUCC;
}

int bus_master_t::get_hbase_full_sender_thread_num(const char *bis, uint64_t &threadnum)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_hbase_full_sender_thread_num", bis);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute [command=get %s] to get transfer type fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    int64_t ithreadnum;
    if (r->type == REDIS_REPLY_STRING)
    {
        if (!str2num(r->str, ithreadnum))
        {
            g_logger.error("[thread_num = %s] is invalid", r->str);
            freeReplyObject(r);
            return MASTER_REDIS_FAIL;
        }
    } else {
        g_logger.error("%s type is not string", key);
        freeReplyObject(r);
        return MASTER_REDIS_FAIL;
    }

    threadnum = static_cast<uint64_t>(ithreadnum);
    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}

int bus_master_t::get_mysql_full_check_pos(const char *bis, bool &checkpos)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_mysql_full_check_pos", bis);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute [command=get %s] to get transfer type fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_STRING)
    {
        if (!strcmp(r->str, "true"))
        {
            checkpos = true;
        } else if (!strcmp(r->str, "false")) {
            checkpos = false;
        } else {
            g_logger.error("[%s = %s] is invalid", key,  r->str);
            freeReplyObject(r);
            return MASTER_REDIS_FAIL;
        }
    } else {
        g_logger.error("%s type is not string", key);
        freeReplyObject(r);
        return MASTER_REDIS_FAIL;
    }

    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}

int bus_master_t::get_transfer_type(const char *bis, transfer_type_t &transfer_type)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_transfer_type", bis);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute [command=get %s] to get transfer type fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_STRING)
    {
        if (!strcmp("full", r->str))
        {
            transfer_type = FULL;
        }else if (!strcmp("incr", r->str))
        {
            transfer_type = INCREMENT;
        }else
        {
            g_logger.error("[type=%s] is invalid", r->str);
            freeReplyObject(r);
            return MASTER_REDIS_EMPTY;
        }
    }

    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}
int bus_master_t::get_charset_info(const char *bis, char *charset, int len)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_charset", bis);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute [command=get %s] to get transfer type fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }

    if (r->type == REDIS_REPLY_STRING)
    {
        snprintf(charset, len, "%s", r->str);
    }

    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}
int bus_master_t::get_target_size(const char *bis, long &target_size)
{
    char key[128];
    snprintf(key, sizeof(key), "%s_target_size", bis);

    redisReply *r = static_cast<redisReply*>(redisCommand(_context, "get %s", key));
    if (r == NULL || r->type == REDIS_REPLY_ERROR)
    {
        g_logger.error("execute [command=get %s] to get transfer type fail", key);
        if (r != NULL)
        {
            freeReplyObject(r);
            r = NULL;
        }
        return MASTER_REDIS_FAIL;
    }

    if(r->type == REDIS_REPLY_NIL)
    {
        g_logger.error("[key=%s] is not exist", key);
        freeReplyObject(r);
        return MASTER_REDIS_EMPTY;
    }
    if (r->type == REDIS_REPLY_STRING)
    {
        if (!str2num(r->str, target_size))
        {
            g_logger.error("[target_size=%s] is invalid", r->str);
            freeReplyObject(r);
            return MASTER_REDIS_FAIL;
        }
    } else {
        g_logger.error("reply type is not string");
        freeReplyObject(r);
        return MASTER_REDIS_FAIL;
    }

    freeReplyObject(r);
    return MASTER_REDIS_SUCC;
}

/********************************************************/
bus_mysql_t::bus_mysql_t(const char *ip,
        int32_t port,
        const char *user,
        const char *pwd):_conn(NULL), _port(port)
{
    snprintf(_ip, sizeof(_ip), "%s", ip);
    snprintf(_user, sizeof(_user), "%s", user);
    snprintf(_pwd, sizeof(_pwd), "%s", pwd);
}

bool bus_mysql_t::init(const char *ip, int32_t port,
        const char *user, const char *pwd, const char *charset)
{
    _port = port;
    snprintf(_ip, sizeof(_ip), "%s", ip);
    snprintf(_user, sizeof(_user), "%s", user);
    snprintf(_pwd, sizeof(_pwd), "%s", pwd);
    
    if (_conn != NULL)
    {
        mysql_close(_conn);
        _conn = NULL;
    }
    
    _conn = mysql_init(NULL);
    if (_conn == NULL)
    {
        g_logger.error("allocate MYSQL object fail");
        return false;
    }

    mysql_options(_conn, MYSQL_SET_CHARSET_NAME, charset);
    
    if(mysql_real_connect(_conn, _ip, _user, _pwd, NULL,  _port, NULL, 0) == NULL) {
        g_logger.error("connet to mysql server [%s:%d] fail", _ip, _port);
        return false;
    }
    return true;
}

bool bus_mysql_t::init(const char *charset)
{
    if (_conn != NULL)
    {
        mysql_close(_conn);
        _conn = NULL;
    }
    _conn = mysql_init(NULL);
    if (_conn == NULL)
    {
        g_logger.error("allocate MYSQL object fail");
        return false;
    }
    mysql_options(_conn, MYSQL_SET_CHARSET_NAME, charset);
    if(mysql_real_connect(_conn, _ip, _user, _pwd, NULL,  _port, NULL, 0) == NULL) {
        g_logger.error("connet to mysql server [%s:%d] fail", _ip, _port);
        return false;
    }
    return true;
}

void bus_mysql_t::destroy()
{
    if (_conn != NULL)
    {
        mysql_close(_conn);
        _conn = NULL;
    }
}

bus_mysql_t::~bus_mysql_t()
{
    if (_conn != NULL)
    {
        mysql_close(_conn);
        _conn = NULL;
    }
}

bool bus_mysql_t::check_table_all_struct(schema_t *sch,
        std::vector<db_object_id> &data)
{
    char *format_query = "select column_name, ordinal_position, column_type like '%%unsigned%%'"
                  "from information_schema.columns "
                  "where table_schema='%s' and table_name='%s'; ";
    char query[512];

    std::size_t tbsz = data.size();
    if (tbsz == 0) return true;
    for (std::size_t j = 0; j < tbsz; ++j)
    {
        std::string &schema_name = data[j].schema_name;
        std::string &table_name = data[j].table_name;
        snprintf(query, sizeof(query), format_query, schema_name.c_str(),
            table_name.c_str());
        int ret = mysql_real_query(_conn, query, strlen(query));
        if (ret != 0)
        {
            g_logger.error("execute query=[%s] fail", query);
            return false;
        }

        MYSQL_RES* res = mysql_store_result(_conn);
        if (res == NULL && mysql_errno(_conn))
        {
            g_logger.error("use result fail,query=[%s] fail,error:%s",
                    query, mysql_error(_conn));
            return false;
        }else if (res != NULL)
        {
            unsigned int num_rows = mysql_num_rows(res);
            std::vector<column_t*> cols = sch->get_columns();
            std::size_t colsz = cols.size();
            if (0 != colsz && colsz != num_rows) {
                g_logger.error("query:%s [%s.%s] on [%s:%d] num_fields not match %d:%d col",
                        query, schema_name.c_str(), table_name.c_str(), _ip, _port, num_rows, colsz);
                mysql_free_result(res);
                return false;
            }
            
            MYSQL_ROW mysql_row = NULL;
            //如果是空的，填充
            if (0 == colsz) {
                while((mysql_row = mysql_fetch_row(res))) {
                    char *column_name = mysql_row[0];
                    int32_t column_seq = atoi(mysql_row[1]) - 1;
                    int32_t column_type = atoi(mysql_row[2]);
                    g_logger.debug("column_t %s:%s %s:%d:%d", schema_name.c_str(), table_name.c_str(), column_name, column_seq, column_type);
                    column_t *column = new (std::nothrow)column_t(column_name, column_seq, column_type);
                    if (NULL == column) oom_error();
                    sch->add_column(column);
                }
            } else {
            //进行比较, @假定query是按照行的顺序返回的
                int index = 0;
                while((mysql_row = mysql_fetch_row(res))) {
                   column_t *tmpcol = cols[index++];
                   const char *tmpname = tmpcol->get_column_name();
                   int32_t tmpseq = tmpcol->get_column_seq();
                   int32_t tmptype = tmpcol->get_column_type();
                   if (strcmp(tmpname, mysql_row[0]) ||
                           (tmpseq != atoi(mysql_row[1]) -1) ||
                           tmptype != atoi(mysql_row[2])) {
                       g_logger.error("[%s.%s] on [%s:%d] column %s:%d:%d not match %s:%d:%d",
                               schema_name.c_str(), table_name.c_str(), _ip, _port,
                               mysql_row[0], atoi(mysql_row[1]) - 1, atoi(mysql_row[2]),
                               tmpname, tmpseq, tmptype);
                        mysql_free_result(res);
                        return false;
                   }
                }
            }
        }
        mysql_free_result(res);
    }
    return true;
}

bool bus_mysql_t::check_table_struct(schema_t *sch,
        std::vector<db_object_id> &data)
{
    char *format_query = "select ordinal_position, column_type like '%%unsigned%%'"
                  "from information_schema.columns "
                  "where table_schema='%s' and table_name='%s' "
                  "and column_name='%s'";
    std::vector<column_t*> cols = sch->get_columns();
    std::size_t colsz = cols.size();

    const char *src_schema_db = sch->get_schema_db();
    const char *src_schema_name = sch->get_schema_name();

    char query[512];
    std::size_t tbsz = data.size();
    if (tbsz == 0) return true;
    for (std::size_t i = 0; i < colsz; ++i)
    {
        int tmp_col_index = -1;
        int tmp_col_unsigned = -1;
        column_t* cur_col = cols[i];
        const char *cur_colname = cur_col->get_column_name();

        for (std::size_t j = 0; j < tbsz; ++j)
        {
            std::string &schema_name = data[j].schema_name;
            std::string &table_name = data[j].table_name;
            snprintf(query, sizeof(query), format_query, schema_name.c_str(),
                table_name.c_str(), cur_colname);
            int ret = mysql_real_query(_conn, query, strlen(query));
            if (ret != 0)
            {
                g_logger.error("execute query=[%s] fail", query);
                return false;
            }

            MYSQL_RES* res = mysql_use_result(_conn);
            if (res == NULL && mysql_errno(_conn))
            {
                g_logger.error("use result fail,query=[%s] fail,error:%s",
                        query, mysql_error(_conn));
                return false;
            }else if (res != NULL)
            {
                MYSQL_ROW row = NULL;
                row = mysql_fetch_row(res);
                if (row == NULL)
                {
                    g_logger.error("[%s.%s] don't have [%s] col",
                            schema_name.c_str(), table_name.c_str(), cur_colname);
                    mysql_free_result(res);
                    return false; 
                }
                int col_index = atoi(row[0]);
                int col_unsigned = atoi(row[1]);
                if (tmp_col_index == -1)
                {
                    tmp_col_index = col_index;
                } else if (tmp_col_index != col_index) {
                    g_logger.error("[%s.%s][col=%s] position=[%d] diff [%d]",
                            schema_name.c_str(), table_name.c_str(), cur_colname,
                            col_index, tmp_col_index);
                    mysql_free_result(res);
                    return false;
                }

                if (tmp_col_unsigned == -1) {
                    tmp_col_unsigned = col_unsigned;
                } else if (tmp_col_unsigned != col_unsigned) {
                    g_logger.error("[%s.%s][col=%s] unsigned=[%d] diff [%d]",
                            schema_name.c_str(), table_name.c_str(), cur_colname,
                            col_unsigned, tmp_col_unsigned);
                    mysql_free_result(res);
                    return false;
                }
                mysql_free_result(res);
            }
        }
        if (tmp_col_index == -1)
        {
            g_logger.error("[%s:%s][col=%s] is not exist in [%s:%d]", src_schema_db,
                 src_schema_name, cur_colname, _ip, _port);
            return false;
        }
        int col_seq = cur_col->get_column_seq();
        if (col_seq == -1)
        {
            cur_col->set_column_seq(tmp_col_index - 1);
        } else if (col_seq != tmp_col_index - 1)
        {
            g_logger.error("[%s:%s][col=%s][%s:%d] type diff with other datasource",
                    src_schema_db, src_schema_name, cur_colname, _ip, _port);
            return false;
        }

        int col_unsigned = cur_col->get_column_type();
        if (col_unsigned == -1) {
            cur_col->set_column_type(tmp_col_unsigned);
        } else if (col_unsigned != tmp_col_unsigned) {
            g_logger.error("[%s:%s][col=%s][%s:%d] unsigned diff with other datasource",
                    src_schema_db, src_schema_name, cur_colname, _ip, _port);
            return false;
        }
    }
    return true;
}

bool bus_mysql_t::get_match_all_table(schema_t *sch,
        std::vector<db_object_id> &data)
{
    char *query = "select table_schema,table_name "
                  "from information_schema.tables "
                  "where table_type='BASE TABLE' and "
                  "table_schema not in "
                  "('mysql','performance_schema','information_schema')";
    int ret = mysql_real_query(_conn, query, strlen(query));
    if (ret != 0)
    {
        g_logger.error("execute query=[%s] fail", query);
        return false;
    }

    MYSQL_RES* res = mysql_use_result(_conn);
    if (res == NULL && mysql_errno(_conn))
    {
        g_logger.error("use result fail,query=[%s] fail,error:%s",
                query, mysql_error(_conn));
        return false;
    }else if (res != NULL)
    {
        MYSQL_ROW row = NULL;
        while ((row = mysql_fetch_row(res)))
        {
            if (sch->check_schema(row[0], row[1]))
            {
                db_object_id id(row[0], row[1]);
                data.push_back(id);
            }
        }
        mysql_free_result(res);
    }
    return true;
}
bool bus_mysql_t::get_full_transfer_position(data_source_t &source)
{
    char *query = "show master status";
    int ret = mysql_real_query(_conn, query, strlen(query));
    if (ret != 0)
    {
        g_logger.error("execute query=[%s] fail %d:%s", 
                query, ret, mysql_error(_conn));
        return false;
    }

    MYSQL_RES* res = mysql_use_result(_conn);
    if (res == NULL && mysql_errno(_conn))
    {
        g_logger.error("use result fail,query=[%s] fail,error:%s",
                query, mysql_error(_conn));
        return false;
    }else if (res != NULL)
    {
        unsigned int num_fields = mysql_num_fields(res);
        BUS_NOTUSED(num_fields);
        assert(num_fields >= 2);
        MYSQL_ROW row = NULL;
        long pos;
        while ((row = mysql_fetch_row(res)))
        {
            if(!str2num(row[1], pos))
            {
                g_logger.error("pos=%s can't convert to int", row[1]);
                return false;
            }
            source.set_position(row[0], pos, 0, 0, 1);
        }

        mysql_free_result(res);
    }

    return true;
}

bool bus_mysql_t::get_full_data(bus_job_t &job, bool &stop_flag,
        const char *charset, uint32_t statid)
{
    int ret;
    bus_engine_t &engine = g_bus.get_engine();
    bus_stat_t &stat = g_bus.get_stat();
    bus_config_t &config = g_bus.get_config();
    long target_size = config.get_target_size();
    uint32_t pos = job.get_ds_index();
    schema_t *curschema = job.get_schema();
    long schema_id = curschema->get_schema_id();
    bool all_columns_flag = curschema->get_all_columns_flag();
    //schema_t *cur_dst_schema = job.get_dst_schema();
    const char *schema_name = job.get_table();
    const char *schema_db = job.get_db();
    data_source_t *src_ds = job.get_data_source();
    const int src_port = src_ds->get_port();
    source_t dst_type = config.get_dstds_type();
    
    std::vector<column_t*>& columns = curschema->get_columns();
    char column_str[512];
    column_str[0] = '\0';
    if (all_columns_flag) {
        strcat(column_str, "*");
    } else {
        for(std::vector<column_t*>::iterator it = columns.begin();
                it != columns.end();
                ++it)
        {
            column_t* col = *it;
            assert(col != NULL);
            if (it != columns.begin()) strcat(column_str, ",");
            strcat(column_str, col->get_column_name());
        }
    }

    char query[1024];
    snprintf(query, sizeof(query), "select %s from %s.%s",
            column_str, schema_db, schema_name);
    g_logger.notice("[%s:%d] query=%s",
            _ip, _port, query);
    uint64_t processed_row_count = 0;
    while (!stop_flag)
    {
        //初始化mysql链接
        if (!init(charset))
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("[%s:%d] init mysql connection fail", _ip, _port);
            sleep(SLEEP_TIME);
            continue;
        }
        //检查位置
        if (!check_full_position(*src_ds))
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("[%s:%d] check position fail", _ip, _port);
            sleep(SLEEP_TIME);
            continue;
        }
        //执行查询
        ret = mysql_real_query(_conn, query, strlen(query));
        if (ret != 0)
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("[%s:%d] execute query=[%s] fail,error:%s",
                    _ip, _port, query, mysql_error(_conn));
            sleep(SLEEP_TIME);
            continue;
        }
        //扫描结果
        MYSQL_RES* res = mysql_use_result(_conn);
        if (res == NULL)
        {
            stat.set_producer_thread_state(statid, THREAD_FAIL);
            g_logger.error("[%s:%d] use result fail,query=[%s],error:%s",
                    _ip, _port, query, mysql_error(_conn));
            sleep(SLEEP_TIME);
            continue;
        } else if (res != NULL) {
            unsigned int num_fields = mysql_num_fields(res);
            assert(num_fields == sz);
            MYSQL_ROW mysql_row = NULL;
            uint64_t temp_row_count = 0;
            stat.set_producer_thread_state(statid, THREAD_RUNNING);
            while (!stop_flag && (mysql_row = mysql_fetch_row(res)))
            {
                ++temp_row_count;
                if (temp_row_count <= processed_row_count) continue;
                processed_row_count = 0;
                //创建并初始化row
                row_t *currow = new(std::nothrow)row_t(num_fields);
                if (currow == NULL) oom_error();
                currow->set_action(INSERT);
                currow->set_src_port(src_port);
                currow->set_src_partnum(pos);
                currow->set_instance_num(target_size);
                //currow->set_dst_schema(cur_dst_schema);
                currow->set_src_schema(curschema);
                currow->set_column_pnamemap(curschema->get_column_namemap());
                currow->set_db(schema_db);
                currow->set_table(schema_name);
                currow->set_schema_id(schema_id);
                currow->set_dst_type(dst_type);
                for (uint32_t i = 0; i < num_fields; i++)
                {
                    currow->push_back(mysql_row[i], false);
                }

                //对row进行转换
                ret = g_bus.process(currow);
                if (ret == PROCESS_FATAL)
                {
                    std::string row_info;
                    currow->print(row_info);
                    g_logger.error("[%s:%d] process row fatal, row=%s",
                            _ip, _port, row_info.c_str());
                    delete currow;
                    mysql_free_result(res);
                    return false;
                } else if (ret == PROCESS_IGNORE) {
                    std::string row_info;
                    currow->print(row_info);
                    g_logger.notice("[%s:%d] process row fail, rows=%s",
                            _ip, _port, row_info.c_str());
                    stat.incr_producer_error_count(statid, 1);
                    delete currow;
                    continue;
                }
                //将row添加到消费者队列
                ret = engine.add_row_copy(currow, statid);
                if (ret == ROW_NUM_INVALID)
                {
                    std::string row_info;
                    currow->print(row_info);
                    g_logger.error("[%s:%d] add row fail, row=%s", _ip, _port, row_info.c_str());
                    delete currow;
                    mysql_free_result(res);
                    return false;
                } else if (ret == ROW_ADD_STOP) {
                    g_logger.notice("[%s:%d] mysql full extractor recv stop signal",
                            _ip, _port);
                    delete currow;
                    break;
                }
                //stat.incr_producer_succ_count(statid, 1);
            }//while
            if (!stop_flag && mysql_errno(_conn))
            {
                stat.set_producer_thread_state(statid, THREAD_FAIL);
                g_logger.error("[%s:%d] fetch row from mysql fail,error:%s",
                        _ip, _port, mysql_error(_conn));
                mysql_free_result(res);
                if (processed_row_count == 0)
                    processed_row_count = temp_row_count;
                sleep(SLEEP_TIME);
                continue;
            }
            //释放内存
            mysql_free_result(res);
            break;
        }
    }//while
    return true;
}

bool bus_mysql_t::check_full_position(data_source_t &source)
{
    lock_t mylock(&source.src_mutex);
    bus_config_t &config = g_bus.get_config();
    mysql_position_t &srcpos = source.get_position();

    if (!config.get_mysql_full_check_pos()) {
        g_logger.debug("Not need checkpos");
        return true;
    }

    if (!get_full_transfer_position(source))
    {
        g_logger.error("[%s:%d] get mysql position fail", 
                _ip, _port);
        return false;
    }
    mysql_position_t newpos;
    newpos.set_position(srcpos.binlog_file,
            srcpos.binlog_offset, srcpos.rowindex, srcpos.cmdindex);
    if (!config.get_source_position(&source))
    {
        g_logger.error("[%s:%d] get position fail",
                _ip, _port);
        return false;
    }
    mysql_position_t start_pos;
    start_pos.set_position(srcpos.binlog_file,
            srcpos.binlog_offset, srcpos.rowindex, srcpos.rowindex);
    int ret = start_pos.compare_position(newpos);
    if (ret != 0)
    {
        g_logger.error("[%s:%d] Down [%s:%ld:%ld:%ld] !=  mysql [%s:%ld:%ld:%ld]",
                _ip, _port, start_pos.binlog_file, start_pos.binlog_offset,
                start_pos.rowindex, start_pos.cmdindex, newpos.binlog_file, newpos.binlog_offset,
                newpos.rowindex, newpos.cmdindex);
        return false;
    }
    g_logger.debug("[%s:%d] Down [%s:%ld:%ld:%ld] ==  mysql [%s:%ld:%ld:%ld]",
            _ip, _port, start_pos.binlog_file, start_pos.binlog_offset,
            start_pos.rowindex, start_pos.cmdindex, newpos.binlog_file, newpos.binlog_offset,
            newpos.rowindex, newpos.cmdindex);
    return true;
}
bool bus_mysql_t::init_library()
{
    unsigned int ret = mysql_thread_safe();
    if (ret != 1)
    {
        g_logger.error("msql library is not thread safe");
        return false;
    }

    if (mysql_library_init(0, NULL, NULL))
    {
        g_logger.error("init mysql library fail");
        return false;
    }
    return true;
}

void bus_mysql_t::destroy_library()
{
    mysql_library_end();
}

bool bus_mysql_t::get_bin_prefix(std::string &prefix)
{
    char *query = "show master status";
    int ret = mysql_real_query(_conn, query, strlen(query));
    if (ret != 0)
    {
        g_logger.error("execute query=[%s] fail, error: %s", query, mysql_error(_conn));
        return false;
    }

    MYSQL_RES* res = mysql_use_result(_conn);
    if (res == NULL && mysql_errno(_conn))
    {
        g_logger.error("use result fail,query=[%s] fail,error:%s",
                query, mysql_error(_conn));
        return false;
    }else if (res != NULL)
    {
        unsigned int num_fields = mysql_num_fields(res);
        assert(num_fields >= 2);
        BUS_NOTUSED(num_fields);
        MYSQL_ROW row = NULL;
        while ((row = mysql_fetch_row(res)))
        {
            std::string tmp(row[0]);
            std::size_t pos = tmp.rfind('.');
            if (pos == std::string::npos) {
                g_logger.error("binlog_file=%s is invalid");
                return false;
            }
            prefix = tmp.substr(0, pos);
        }
        if (mysql_errno(_conn))
        {
            g_logger.error("fetch binlog prefix from mysql fail,error:%s", 
                    mysql_error(_conn));
            mysql_free_result(res);
            return false;
        }
        mysql_free_result(res);
    }
    return true;
}
bool bus_mysql_t::get_bin_open_flag(std::string &open_flag)
{
    char *query = "show variables like 'log_bin'";
    int ret = mysql_real_query(_conn, query, strlen(query));
    if (ret != 0)
    {
        g_logger.error("execute query=[%s] fail", query);
        return false;
    }

    MYSQL_RES* res = mysql_use_result(_conn);
    if (res == NULL && mysql_errno(_conn))
    {
        g_logger.error("use result fail,query=[%s] fail,error:%s",
                query, mysql_error(_conn));
        return false;
    }else if (res != NULL)
    {
        unsigned int num_fields = mysql_num_fields(res);
        assert(num_fields >= 2);
        BUS_NOTUSED(num_fields);
        MYSQL_ROW row = NULL;
        while ((row = mysql_fetch_row(res)))
        {
            open_flag.assign(row[1]);
        }
        if (mysql_errno(_conn))
        {
            g_logger.error("fetch binlog open flag from mysql fail,error:%s", 
                    mysql_error(_conn));
            mysql_free_result(res);
            return false;
        }
        mysql_free_result(res);
    }
    return true;
}
bool bus_mysql_t::get_bin_format_flag(std::string &format_flag)
{
    char *query = "show variables like 'binlog_format'";
    int ret = mysql_real_query(_conn, query, strlen(query));
    if (ret != 0)
    {
        g_logger.error("execute query=[%s] fail", query);
        return false;
    }

    MYSQL_RES* res = mysql_use_result(_conn);
    if (res == NULL && mysql_errno(_conn))
    {
        g_logger.error("use result fail,query=[%s] fail,error:%s",
                query, mysql_error(_conn));
        return false;
    }else if (res != NULL)
    {
        unsigned int num_fields = mysql_num_fields(res);
        assert(num_fields >= 2);
        BUS_NOTUSED(num_fields);
        MYSQL_ROW row = NULL;
        while ((row = mysql_fetch_row(res)))
        {
            format_flag.assign(row[1]);
        }
        if (mysql_errno(_conn))
        {
            g_logger.error("fetch binlog format from mysql fail,error:%s", 
                    mysql_error(_conn));
            mysql_free_result(res);
            return false;
        }
        mysql_free_result(res);
    }
    return true;
}

bool bus_mysql_t::select_table_charset(data_source_t *ds)
{
    bus_config_t &config = g_bus.get_config();
    char *query = "select table_name, table_schema, character_set_name "
        "from information_schema.tables,information_schema.collations "
        "where table_collation = collation_name and "
        "table_type='BASE TABLE' and  "
        "table_schema not in ('mysql','information_schema', 'performance_schema');";
        int ret = mysql_real_query(_conn, query, strlen(query));
        if (ret != 0)
        {
            g_logger.error("execute query=%s fail, error:%s",query, mysql_error(_conn));
            return false;
        }
        MYSQL_RES* res = mysql_store_result(_conn);
        if (res == NULL)
        {
            g_logger.error("store result fail,error:%s", mysql_error(_conn));
            return false;
        } else {
            MYSQL_ROW row = NULL;
            char key_charset[64];
            while ((row = mysql_fetch_row(res)))
            {
                char *table_name = row[0];
                char *table_schema = row[1];
                char *charset = row[2];
                if (config.get_match_schema(table_schema, table_name) == NULL)
                {
                    g_logger.debug("%s:%s can't find match schema",
                            table_schema, table_name);
                    continue;
                }
                snprintf(key_charset, sizeof(key_charset), "%s.%s", table_schema, table_name);
                ds->set_table_charset(key_charset, charset);
                g_logger.debug("%s:%s", key_charset, charset);
            }
            if (mysql_errno(_conn))
            {
                g_logger.error("fetch row from mysql fail,error:%s", mysql_error(_conn));
                mysql_free_result(res);
                return false;
            }
            mysql_free_result(res);
        }
        return true;
}
}//namespace bus

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
