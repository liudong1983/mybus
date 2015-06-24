/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_engine.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/28 22:51:59
 * @version $Revision$
 * @brief 
 *  
 **/

#include "bus_engine.h"
#include "bus_wrap.h"

namespace bus
{
bool bus_engine_t::start()
{
    /* 启动生产者 */
    if (!_reader->init())
    {
        g_logger.error("producer thread pool start fail");
        return false;
    }
    /* 启动消费者 */
    std::size_t sz = _sender.size();
    for (uint32_t i = 0; i < sz; ++i)
    {
        if (!_sender[i]->init())
        {
            g_logger.error("start %u  consumer thread pool fail", i);
            return false;
        }
    }
    return true;
}

void bus_engine_t::stop()
{
    for(std::vector<thread_pool_t*>::iterator it = _sender.begin();
            it != _sender.end();
            ++it)
    {
        thread_pool_t *pool = *it;
        pool->shutdown();
    }
    _reader->shutdown();
}
bool bus_engine_t::get_sender_stat()
{
    std::size_t sz = _sender.size();
    for (uint32_t i = 0; i < sz; ++i)
    {
        if (!_sender[i]->get_stat()) return false;
    }
    return true;
}
void bus_engine_t::destroy()
{
    for (std::vector<thread_pool_t*>::iterator it = _sender.begin();
            it != _sender.end();
            ++it)
    {
        if (*it != NULL)
        {
            delete *it;
            *it = NULL;
        }
    }
    _sender.clear();

    if (_reader != NULL)
    {
        delete _reader;
        _reader = NULL;
    }
    
}

void bus_engine_t::add_job(bus_job_t *job)
{
    _reader->simple_add(job);
}
bool bus_engine_t::is_clean_source(uint32_t src_partnum)
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

int bus_engine_t::add_row_copy(row_t *row, int statid)
{
    bus_stat_t &stat = g_bus.get_stat();
    std::vector<cmd_t*>& rcols = row->get_rcols();
    std::size_t sz = rcols.size();
    uint64_t sender_size = _sender.size();
    long schema_id = row->get_schema_id();
    bool record_flag = row->get_src_schema()->get_record_flag();
    int ret = ROW_ADD_OK;
    std::size_t i = 0;
    for (; i < sz - 1; i++)
    { 
        row_t *tmprow = new (std::nothrow)row_t(*row);
        if (NULL == tmprow) oom_error();
        tmprow->dup_cmd(row, i);
        //全量同步，多线程发送时，上游不同行写入到hbase同一行时，
        //由于多线程无法确定先后执行顺序，因此结果不确定
        //需要添加timestamp来保证一致性
        if (HBASE_DS == row->get_dst_type()) {
            tmprow->set_hbase_time(stat.get_producer_succ_count(statid));
            //对于hbase，通过取模 将row发送到不同的sender上去
            //对于redis, 则是在.so中指定了dst
            tmprow->set_dst_partnum(stat.get_producer_succ_count(statid)%sender_size);
        }
        ret = add_row(tmprow);
        if (ret != ROW_ADD_OK) {
            std::string info;
            tmprow->print(info);
            g_logger.error("row add fail %s ", info.c_str());
            delete tmprow;
            tmprow = NULL;
        } else {
            if (record_flag)
                stat.incr_producer_succ_count(statid, 1);
        }
    }

    if (ROW_ADD_OK == ret) {
        row->dup_cmd(row, i);
        if (HBASE_DS == row->get_dst_type()) {
            row->set_hbase_time(stat.get_producer_succ_count(statid));
            row->set_dst_partnum(stat.get_producer_succ_count(statid)%sender_size);
        }
        ret = add_row(row);
        if (ret != ROW_ADD_OK) {
           std::string info;
           row->print(info);
           g_logger.error("row add fail %s ", info.c_str());
        } else {
            if (record_flag)
                stat.incr_producer_succ_count(statid, 1);
        }
    }

    return ret;
}

int bus_engine_t::add_row(row_t *row)
{
    long srcnum = row->get_src_partnum();
    long dstnum = row->get_dst_partnum();
    if (REDIS_DS == row->get_dst_type() || REDIS_COUNTER_DS == row->get_dst_type()) {
        if (srcnum < 0 || dstnum < 0 || dstnum >= _dstsz)
        {
            g_logger.error("check row partnum fail, [srcnum=%ld][dstnum=%ld][dstsize=%u]",
                    srcnum, dstnum, _dstsz);
            return ROW_NUM_INVALID;
        }
    }

    source_t dst_type = g_bus.get_dst_type();
    uint32_t index = 0;
    switch (dst_type) {
        case REDIS_DS:
        case REDIS_COUNTER_DS:
            index = srcnum * _dstsz + dstnum;
            break;
        case HBASE_DS:
            index = dstnum;
            break;
        default:
            g_logger.error("dst_type is invalid %d", dst_type);
            return ROW_NUM_INVALID;
    }

    if (index >= _sender.size())
    {
        g_logger.error("add row index=[%ld:%u:%ld][%u] is invalid", 
                srcnum, _dstsz, dstnum, index);
        return ROW_NUM_INVALID;
    }

    if (!_sender[index]->add(row)) {
        g_logger.notice("add row fail, stop");
        return ROW_ADD_STOP;
    }
    return ROW_ADD_OK;
}

int bus_engine_t::add_row_copy(row_t *row, 
        std::vector<mysql_position_t*> &all_pos, int statid)
{
    bus_stat_t &stat = g_bus.get_stat();
    std::vector<cmd_t*>& rcols = row->get_rcols();
    std::size_t sz = rcols.size();
    std::size_t sender_size = _sender.size();
    long schema_id = row->get_schema_id();
    bool record_flag = row->get_src_schema()->get_record_flag();
    int ret = ROW_ADD_OK;
    std::size_t i = 0;
    for (; i < sz - 1; i++) 
    {
        row_t *tmprow = new (std::nothrow)row_t(*row);
        if (NULL == tmprow) oom_error();
        tmprow->dup_cmd_and_position(row, i);
        if (HBASE_DS == tmprow->get_dst_type()) {
            tmprow->set_dst_partnum(stat.get_producer_succ_count(statid)%sender_size);
        }
        ret = add_row(tmprow, all_pos);
        //if (ROW_ADD_OK != ret && ROW_IGNORE_OK != ret) {
        if (ROW_ADD_OK != ret) { //bug resolved
            std::string info;
            tmprow->print(info);
            g_logger.warn("row add fail %s", info.c_str());
            delete tmprow;
            tmprow = NULL;
        } else {
            if (record_flag)
                stat.incr_producer_succ_count(statid, 1);
        }
    }

    if (ROW_ADD_OK == ret || ROW_IGNORE_OK == ret) {
        //row->dup_cmd_and_position(row, i);
        row->dup_cmd(row, i);
        row->set_position_cmdindex(static_cast<long>(i));
        if (HBASE_DS == row->get_dst_type()) {
            row->set_dst_partnum(stat.get_producer_succ_count(statid)%sender_size);
        }
        ret = add_row(row, all_pos);
        //if (ROW_ADD_OK != ret && ROW_IGNORE_OK != ret) {
        if (ROW_ADD_OK != ret) {
            std::string info;
            row->print(info);
            g_logger.warn("row add fail %s", info.c_str());
        } else {
            //更新计数
            if (record_flag)
                stat.incr_producer_succ_count(statid, 1);
        }
    }

    return ret;
}


int bus_engine_t::add_row(row_t *row, std::vector<mysql_position_t*> &all_pos)
{
    long srcnum = row->get_src_partnum();
    long dstnum = row->get_dst_partnum();
    int srcport = row->get_src_port();

    if (REDIS_DS == row->get_dst_type() || REDIS_COUNTER_DS == row->get_dst_type()) {
        if (srcnum < 0 || dstnum < 0 || dstnum >= _dstsz)
        {
            g_logger.error("check row partnum fail, [srcnum=%ld][dstnum=%ld][dstsize=%u]",
                    srcnum, dstnum, _dstsz);
            return ROW_NUM_INVALID;
        }
    }

    source_t dst_type = g_bus.get_dst_type();
    uint32_t index = 0;
    switch (dst_type) {
        case REDIS_DS:
        case REDIS_COUNTER_DS:
            index = srcnum * _dstsz + dstnum;
            break;
        case HBASE_DS:
            index = srcnum;
            break;
        default:
            g_logger.error("dst_type is invalid %d", dst_type);
            return ROW_NUM_INVALID;
    }

    if (index >= _sender.size())
    {
        g_logger.error("add row index=[%ld:%u:%ld][%u] is invalid", 
                srcnum, _dstsz, dstnum, index);
        return ROW_NUM_INVALID;
    }

    mysql_position_t *base_pos = all_pos[index];
    mysql_position_t &row_pos = row->get_position();
    if (base_pos->compare_position(row_pos) >= 0) {
        g_logger.warn("[%d.%ld] row position=[%s:%lu:%lu:%lu] <= base pos=[%s:%lu:%lu:%lu] is ignore",
                srcport, dstnum,
                row_pos.binlog_file, row_pos.binlog_offset, row_pos.rowindex, row_pos.cmdindex,
                base_pos->binlog_file, base_pos->binlog_offset, base_pos->rowindex, base_pos->cmdindex);
        return ROW_IGNORE_OK;
    }

    if (!_sender[index]->add(row)) {
        g_logger.notice("add row fail, stop");
        return ROW_ADD_STOP;
    }
    return ROW_ADD_OK;
}

bool bus_engine_t::add_end_row()
{
    for (std::vector<thread_pool_t*>::iterator it = _sender.begin();
            it != _sender.end();
            ++it)
    {
        (*it)->add(NULL);
    }
    return true;
}

bus_engine_t::~bus_engine_t()
{
    for (std::vector<thread_pool_t*>::iterator it = _sender.begin();
            it != _sender.end();
            ++it)
    {
        delete *it;
        *it = NULL;
    }
    if (_reader != NULL)
    {
        delete _reader;
        _reader = NULL;
    }
}

}//namesapce bus


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
