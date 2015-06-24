/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_stat.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/06 13:50:52
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_stat.h"
#include "threadpool.h"
#include "bus_wrap.h"
namespace bus {
void bus_stat_t::clear()
{
    for(std::vector<stat_item_t*>::iterator it = _stat_producer.begin();
            it != _stat_producer.end();
            ++it)
    {
        stat_item_t *pitem = *it;
        if (pitem != NULL)
        {
            delete pitem;
            pitem = NULL;
        }
    }
    for(std::vector<stat_item_t*>::iterator it = _stat_consumer.begin();
            it != _stat_consumer.end();
            ++it)
    {
        stat_item_t *pitem = *it;
        if (pitem != NULL)
        {
            delete pitem;
            pitem = NULL;
        }
    }
    for(std::vector<mysql_position_t*>::iterator it = _stat_position.begin();
            it != _stat_position.end();
            ++it)
    {
        mysql_position_t *pitem = *it;
        if (pitem != NULL)
        {
            delete pitem;
            pitem = NULL;
        }
    }
    _stat_producer.clear();
    _stat_consumer.clear();
    _thread_producer.clear();
    _thread_consumer.clear();
    _stat_position.clear();
    _src_producer.clear();
    _src_consumer.clear();
    g_logger.reset_error_info();
}
bus_stat_t::~bus_stat_t()
{

    for(std::vector<stat_item_t*>::iterator it = _stat_producer.begin();
            it != _stat_producer.end();
            ++it)
    {
        stat_item_t *pitem = *it;
        if (pitem != NULL)
        {
            delete pitem;
            pitem = NULL;
        }
    }
    for(std::vector<stat_item_t*>::iterator it = _stat_consumer.begin();
            it != _stat_consumer.end();
            ++it)
    {
        stat_item_t *pitem = *it;
        if (pitem != NULL)
        {
            delete pitem;
            pitem = NULL;
        }
    }
    for(std::vector<mysql_position_t*>::iterator it = _stat_position.begin();
            it != _stat_position.end();
            ++it)
    {
        mysql_position_t *pitem = *it;
        if (pitem != NULL)
        {
            delete pitem;
            pitem = NULL;
        }
    }
    pthread_mutex_destroy(&_pmutex);
    pthread_mutex_destroy(&_smutex);
}

//for hbase reconnect
void bus_stat_t::set_sender_thread_name(int statid, char *name)
{
    _src_consumer[statid] = name;
}

uint32_t bus_stat_t::get_sender_thread_id(char *sourceinfo)
{
    lock_t mylock(&_smutex);

    stat_item_t *pitem = new(std::nothrow) stat_item_t();
    if (pitem == NULL) oom_error();

    _stat_consumer.push_back(pitem);
    _thread_consumer.push_back(THREAD_RUNNING);
    _src_consumer.push_back(sourceinfo);
    //用于标识 需要重连 最初分配的thriftserver
    _thrift_flag.push_back(true);
    uint32_t ret = _stat_consumer.size() - 1;
    return ret;
}

uint32_t bus_stat_t::get_extract_thread_id(char *sourceinfo)
{
    stat_item_t *pitem = new(std::nothrow) stat_item_t();
    if (pitem == NULL) oom_error();

    lock_t mylock(&_pmutex);
    _stat_producer.push_back(pitem);
    _thread_producer.push_back(THREAD_RUNNING);
    _src_producer.push_back(sourceinfo);
    uint32_t ret = _stat_producer.size() - 1;
    return ret;
}

void bus_stat_t::reset_all_thrift_flag()
{
    uint32_t sz = _thrift_flag.size();
    for (uint32_t i = 0; i < sz; ++i)
    {
        _thrift_flag[i] = true;
    }
}

void bus_stat_t::reset_thrift_flag(uint64_t index)
{
    uint32_t sz = _thrift_flag.size();
    if (index >= sz) {
        g_logger.error("out of index %d:%d", sz, index);
        return;
    }
    _thrift_flag[index] = false;
}

bool bus_stat_t::get_thrift_flag(uint64_t index)
{
    uint32_t sz = _thrift_flag.size();
    if (index >= sz) {
        g_logger.error("out of index %d:%d", sz, index);
        return false;
    }
    return _thrift_flag[index];
}
void bus_stat_t::get_thread_stat_info(std::string &info)
{
    uint32_t sz = _stat_consumer.size();
    char buff[128];
    for (uint32_t i = 0; i < sz; ++i)
    {
        snprintf(buff, sizeof(buff), "%s:%lu\r\n", _src_consumer[i].c_str(), 
                _stat_consumer[i]->succ_count);
        info.append(buff);
    }
}

void bus_stat_t::get_thread_info(std::string &info)
{
    uint32_t sz = _src_producer.size();
    for (uint32_t i = 0; i < sz; ++i)
    {
        info.append("up:");
        info.append(_src_producer[i]);
        info.append(":");
        info.append(thread_msg[_thread_producer[i]]);
        info.append("\r\n");
    }

    sz = _src_consumer.size();
    for (uint32_t i = 0; i < sz; ++i)
    {
        info.append("down:");
        info.append(_src_consumer[i]);
        info.append(":");
        info.append(thread_msg[_thread_consumer[i]]);
        info.append("\r\n");
    }
}

void bus_stat_t::get_producer_count(uint64_t& succ_count, uint64_t& error_count)
{
    lock_t mylock(&_pmutex);
    succ_count = 0;
    error_count = 0;
    for(std::vector<stat_item_t*>::iterator it = _stat_producer.begin();
            it != _stat_producer.end();
            ++it)
    {
        stat_item_t *pitem = *it;
        succ_count += pitem->succ_count;
        error_count += pitem->error_count;
    }
}
void bus_stat_t::get_consumer_count(uint64_t& succ_count, uint64_t& error_count)
{
    lock_t mylock(&_smutex);
    succ_count = 0;
    error_count = 0;
    for(std::vector<stat_item_t*>::iterator it = _stat_consumer.begin();
            it != _stat_consumer.end();
            ++it)
    {
        stat_item_t *pitem = *it;
        succ_count += pitem->succ_count;
        error_count += pitem->error_count;
    }
}
/*
void bus_stat_t::get_ds_position(std::string &str)
{
    bus_config_t &config = g_bus.get_config();
    std::vector<data_source_t*> src_source = config.get_source();
    std::vector<data_source_t*> dst_source = config.get_dst();
    config.get_stat_position(_stat_position);

    std::size_t srcsz = src_source.size();
    std::size_t dstsz = dst_source.size();
    std::size_t possz = _stat_position.size();
    assert(possz == srcsz * dstsz);
    for (std::size_t i = 0; i < srcsz; ++i)
    {
        data_source_t *srcds = src_source[i];
        const char *srcip = srcds->get_ip();
        const int32_t srcport = srcds->get_port();
        for (std::size_t j = 0; j < dstsz; ++j)
        {
            data_source_t *dstds = dst_source[j];
            const char *dstip = dstds->get_ip();
            const int32_t dstport = dstds->get_port();
            mysql_position_t *curpos = _stat_position[i * dstsz + j];
            const char *filename = curpos->binlog_file;
            uint64_t offset = curpos->binlog_offset;
            uint64_t rowindex = curpos->rowindex;
            char source_info[128];
            if (filename[0] == '\0')
                snprintf(source_info, sizeof(source_info),
                        "position:%s_%d_%s_%d:unkown\r\n", srcip, srcport, dstip, dstport);
            else
                snprintf(source_info, sizeof(source_info),
                        "position:%s_%d_%s_%d-%s_%ld_%ld\r\n",
                        srcip, srcport, dstip, dstport, filename, offset, rowindex);
            str.append(source_info);
        }
    }
}
*/
bool bus_stat_t::get_thread_state()
{
    std::size_t sz = _thread_producer.size();
    for (std::size_t i = 0; i < sz; ++i)
    {
        if (_thread_producer[i] == THREAD_FAIL ||
                _thread_producer[i] == THREAD_EXIT_WITH_FAIL) {
            g_logger.notice("thread_producer[%d] = %d fail", i, _thread_producer[i]);
            return false;
        }
    }

    sz = _thread_consumer.size();
    for (std::size_t i = 0; i < sz; ++i)
    {
        if (_thread_consumer[i] == THREAD_FAIL ||
                _thread_consumer[i] == THREAD_EXIT_WITH_FAIL) {
            g_logger.notice("thread_consumer[%d] = %d fail", i, _thread_consumer[i]);
            return false;
        }
    }
    return true;
}
}//namespace bus
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
