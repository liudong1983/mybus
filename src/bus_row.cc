/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_row.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/08 17:50:25
 * @version $Revision$
 * @brief 
 *  
 **/
#include <sys/time.h>
#include <sys/types.h>
#include "bus_row.h"
namespace bus {
std::vector<tobject_t*> NULL_VEC;
cmd_t::~cmd_t(){}

void dump_string(const char *p, char** dst)
{
    if (p == NULL) {
        *dst = NULL;
        return;
    }

    uint32_t sz = strlen(p);
    char *str = new (std::nothrow)char[sz + 1];
    if (str == NULL) oom_error();

    if (sz != 0) memcpy(str, p, sz);
    str[sz] = '\0';

    *dst = str;
}

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

void clear_batch_vector(std::vector<void*> &batch_vec)
{
    for(std::vector<void*>::iterator it = batch_vec.begin();
            it != batch_vec.end();
            ++it) 
    {
        row_t* prow = static_cast<row_t*>(*it);
        delete prow;
    }
    batch_vec.clear();
}

hbase_cmd_t::hbase_cmd_t(uint64_t cmd, char* table, char* row, 
        uint64_t time):_cmd(cmd), _time(time) {
    snprintf(_table, sizeof(_table), "%s", table);
    snprintf(_row, sizeof(_row), "%s", row);
}

hbase_cmd_t::hbase_cmd_t(uint64_t cmd, char* table, char* row):_cmd(cmd) {
    snprintf(_table, sizeof(_table), "%s", table);
    snprintf(_row, sizeof(_row), "%s", row);
}

void hbase_cmd_t::add_columnvalue(char* cf, char* qu, char* value, uint64_t time, uint64_t flag)
{
    if(_cmd != HBASE_CMD_PUT)
        return;
    tobject_t *tobj = new (std::nothrow)tobject_t(cf, qu, value, time, flag);
    if (NULL == tobj) oom_error();
    _tobject_vector.push_back(tobj);
}

void hbase_cmd_t::add_column(char* cf, char* qu, uint64_t time)
{
    if(_cmd != HBASE_CMD_DEL)
        return;
    tobject_t *tobj = new (std::nothrow)tobject_t(cf, qu, time);
    if (NULL == tobj) oom_error();
    _tobject_vector.push_back(tobj);
}

void hbase_cmd_t::add_increment(char* cf, char* qu, int64_t amount)
{
    if (_cmd != HBASE_CMD_INCR)
        return;
    tobject_t *tobj = new (std::nothrow)tobject_t(cf, qu, amount);
    if (NULL == tobj) oom_error();
    _tobject_vector.push_back(tobj);
}

void hbase_cmd_t::print(std::string &info) const
{
    info.append("hbase_cmd_t=");
    if (_cmd == HBASE_CMD_PUT) {
        info.append("put"); 
    } else if (_cmd == HBASE_CMD_DEL) {
        info.append("delete");
    } else if (_cmd == HBASE_CMD_INCR) {
        info.append("increment");
    } else {
        info.append("unknown");
    }
    info.append("+");

    if (_table != NULL) {
        info.append(_table); 
    }
    info.append("+");

    if (_row != NULL) {
        info.append(_row); 
    }
}

hbase_cmd_t::~hbase_cmd_t()
{
    for(std::vector<tobject_t*>::iterator it = _tobject_vector.begin();
            it != _tobject_vector.end();
            ++it)
    {
        if (*it != NULL)
        {
            delete *it;
            *it = NULL;
        }
    }
}

redis_cmd_t::redis_cmd_t(long dst, char* cmd, char* key,
        char* field, char* value):cmd_t(dst)
{
    dump_string(cmd, &_cmd);
    dump_string(key, &_key);
    dump_string(field, &_field);
    dump_string(value, &_value);
}

void redis_cmd_t::print(std::string &info) const
{
    info.append("redis_cmd_t=");
    if (_cmd != NULL) {
        info.append(_cmd);
    }
    info.append("+");

    if (_key != NULL) {
        info.append(_key); 
    }
    info.append("+");

    if (_field != NULL) {
        info.append(_field); 
    }
    info.append("+");

    if (_value != NULL) {
        info.append(_value); 
    }
}

redis_cmd_t::~redis_cmd_t()
{
    if (_cmd != NULL) {
        delete []_cmd;
    }
    if (_key != NULL) {
        delete []_key;
    }
    if (_field != NULL) {
        delete []_field;
    }
    if (_value != NULL) {
        delete []_value;
    }
}
//Unnormal, and specially for add_row_copy()
row_t::row_t(const row_t &other):
    _rowpos(other._rowpos),
    _dst_type(other._dst_type),
    _dst_partnum(0),
    _source_partnum(other._source_partnum),
    _source_port(other._source_port),
    _src_schema(other._src_schema),
    _schema_id(other._schema_id)
{
}

void row_t::dup_cmd(row_t* parent, long n) 
{
    std::vector<cmd_t*>& rcols = parent->get_rcols();
    assert(rcols[n] != NULL);
    set_cmd(rcols[n]);
    rcols[n] = NULL;
    //set_position_cmdindex(n);
}

void row_t::dup_cmd_and_position(row_t* parent, long cmdindex)
{
    dup_cmd(parent, cmdindex);
    _rowpos = parent->get_position();
    set_position_cmdindex(cmdindex);
}

void row_t::push_back(const char *p, bool is_old)
{
    if (p == NULL)
    {
        _cols.push_back(NULL);
        return;
    }
    uint32_t sz = strlen(p);
    char *str = new (std::nothrow)char[sz + 1];
    if (str == NULL) oom_error();
    
    if (sz != 0) memcpy(str, p, sz);
    str[sz] = '\0';
    if (is_old) {
        _oldcols.push_back(str);
    } else {
        _cols.push_back(str);
    }
}

// 
hbase_cmd_t* row_t::get_hbase_put(char* table, char* row, 
        uint64_t time)
{
    hbase_cmd_t *hcmd = new (std::nothrow)hbase_cmd_t(HBASE_CMD_PUT,
            table, row, time);
    if (NULL == hcmd) oom_error();
    _zcols.push_back(static_cast<cmd_t*>(hcmd));
    return hcmd;
}

hbase_cmd_t* row_t::get_hbase_delete(char* table, char* row)
{
    hbase_cmd_t *hcmd = new (std::nothrow)hbase_cmd_t(HBASE_CMD_DEL, table, row);
    if (NULL == hcmd) oom_error();
    _zcols.push_back(static_cast<cmd_t*>(hcmd));
    return hcmd;
}

hbase_cmd_t* row_t::get_hbase_increment(char* table, char* row)
{
    hbase_cmd_t *hcmd = new (std::nothrow)hbase_cmd_t(HBASE_CMD_INCR, table, row);
    if (NULL == hcmd) oom_error();
    _zcols.push_back(static_cast<cmd_t*>(hcmd));
    return hcmd;
}

//for redis
void row_t::push_cmd(long dst, char* cmd, char* key, 
        char* field, char* value)
{
    redis_cmd_t *rcmd = new (std::nothrow)redis_cmd_t(dst,
            cmd, key, field, value);
    if (NULL == rcmd) oom_error();
    _zcols.push_back(static_cast<cmd_t*>(rcmd));
}

void row_t::print(std::string &info)
{
    info.reserve(10240);
    info.append("cols=");
    std::size_t sz = _cols.size();
    for (std::size_t i = 0; i < sz; ++i)
    {
        if (_cols[i] != NULL) info.append(_cols[i]);
        info.append(" ");
    }
    info.append("oldcols=");

    sz = _oldcols.size();
    for(std::size_t i = 0; i < sz; ++i)
    {
        if (_oldcols[i] != NULL) info.append(_oldcols[i]);
        info.append(" ");
    }

    char buf[32];
    snprintf(buf, sizeof(buf), "%ld ", _dst_partnum);
    info.append("dst_partnum=");
    info.append(buf);

    if (NULL != _cmd)
    {
        _cmd->print(info);
    }
}

void row_t::push_back(const char *p, int sz, bool is_old)
{
    char *str = new (std::nothrow)char[sz + 1];
    if (str == NULL) oom_error();
    
    if (sz != 0) memcpy(str, p, sz);
    str[sz] = '\0';
    if (is_old) {
        _oldcols.push_back(str);
    } else {
        _cols.push_back(str);
    }
}

row_t::~row_t()
{
    for(std::vector<char*>::iterator it = _cols.begin();
            it != _cols.end();
            ++it)
    {
        if (*it != NULL)
        {
            delete [] *it;
            *it = NULL;
        }
    }

    for(std::vector<char*>::iterator it = _oldcols.begin();
            it != _oldcols.end();
            ++it)
    {
        if (*it != NULL)
        {
            delete [] *it;
            *it = NULL;
        }
    }

    for(std::vector<cmd_t*>::iterator it = _zcols.begin();
            it != _zcols.end();
            ++it)
    {
        if (*it != NULL)
        {
            delete *it;
            *it = NULL;
        }
    }
    
    
    if (NULL != _cmd) {
        delete _cmd;
    } 
}

bool check_redis_row(row_t *row)
{
    char *redis_cmd = row->get_redis_cmd();
    if (redis_cmd == NULL) return false;
    return true;
}

int64_t get_timestamp()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((tv.tv_sec<<20)|(tv.tv_usec&0xfffff));//时间戳
}

}//namespace bus

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
