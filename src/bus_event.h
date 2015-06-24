/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_event.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/14 17:47:25
 * @version $Revision$
 * @brief 
 * 解析binlog event事件
 *  
 **/
#ifndef  __BUS_EVENT_H_
#define  __BUS_EVENT_H_
#include <stdio.h>
#include <unistd.h>
#include <math.h>
#include <errno.h>
#include <assert.h>
#include <string>
#include <vector>
#include "mysql.h"
#include "bus_util.h"
#include "bus_row.h"
#include "bus_config.h"
#include "bus_wrap.h"
#include "bus_charset.h"

namespace bus {
#define PACKET_NORMAL_SIZE 0xffffff

#define MYSQL_TYPE_DECIMAL  0
#define MYSQL_TYPE_TINY  1          
#define MYSQL_TYPE_SHORT  2
#define MYSQL_TYPE_LONG  3
#define MYSQL_TYPE_FLOAT  4
#define MYSQL_TYPE_DOUBLE  5
#define MYSQL_TYPE_NULL  6
#define MYSQL_TYPE_TIMESTAMP 7
#define MYSQL_TYPE_LONGLONG  8
#define MYSQL_TYPE_INT24  9
#define MYSQL_TYPE_DATE  10
#define MYSQL_TYPE_TIME  11
#define MYSQL_TYPE_DATETIME 12
#define MYSQL_TYPE_YEAR 13
#define MYSQL_TYPE_NEWDATE 14
#define MYSQL_TYPE_VARCHAR  15
#define MYSQL_TYPE_BIT  16
#define MYSQL_TYPE_NEWDECIMAL 246
#define MYSQL_TYPE_ENUM 247
#define MYSQL_TYPE_SET 248
#define MYSQL_TYPE_TINY_BLOB 249
#define MYSQL_TYPE_MEDIUM_BLOB 250
#define MYSQL_TYPE_LONG_BLOB 251
#define MYSQL_TYPE_BLOB 252
#define MYSQL_TYPE_VAR_STRING 253
#define MYSQL_TYPE_STRING  254
#define MYSQL_TYPE_GEOMETRY 255

typedef unsigned char uchar;

#define int2store(T,A)       do { uint def_temp= (uint) (A) ;\
    *((uchar*) (T))=  (uchar)(def_temp); \
    *((uchar*) (T)+1)=(uchar)((def_temp >> 8)); \
} while(0)
#define int3store(T,A)       do { /*lint -save -e734 */\
    *((uchar*)(T))=(uchar) ((A));\
    *((uchar*) (T)+1)=(uchar) (((A) >> 8));\
    *((uchar*)(T)+2)=(uchar) (((A) >> 16)); \
    /*lint -restore */} while(0)
#define int4store(T,A)       do { *((char *)(T))=(char) ((A));\
    *(((char *)(T))+1)=(char) (((A) >> 8));\
    *(((char *)(T))+2)=(char) (((A) >> 16));\
    *(((char *)(T))+3)=(char) (((A) >> 24)); } while(0)
class bus_event_t;
class bus_error_packet_t
{
    public:
        bus_error_packet_t():_errflag(0xff) {}
        ~bus_error_packet_t() {}
        int32_t parse(char *buf, uint32_t &pos, uint32_t pack_len);
        void print_info()
        {
            g_logger.error("error packet,[errcode=%s], error:%s", 
                    _state, _info.c_str());
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_error_packet_t);
        uint8_t _errflag;
        uint16_t _errcode;
        char _state[6];
        std::string _info;
};
class bus_dump_cmd_t
{
    public:
        bus_dump_cmd_t(const char *binlog_filename,
                uint32_t pos,
                uint32_t server_id):_cmd(0x12),
        _binlog_pos(pos),
        _flags(0x00),
        _server_id(server_id),
        _binlog_filename(binlog_filename) {}
        
        void set_flag(uint8_t flag)
        {
            _flags = flag;
        }
        int32_t write_packet(int fd);
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_dump_cmd_t);
        uint8_t _cmd;
        uint32_t _binlog_pos;
        uint16_t _flags;
        uint32_t _server_id;
        std::string _binlog_filename;
};
class bus_event_head_t
{
    public:
        bus_event_head_t() {}
        ~bus_event_head_t() {}
        void parse_event_head(char *buf, uint32_t &pos);
        uint32_t get_event_pos()
        {
            return log_pos - event_sz;
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_event_head_t);
    public:
        uint32_t time_stamp;
        uint8_t event_type;
        uint32_t server_id;
        uint32_t event_sz;
        uint32_t log_pos;
        uint16_t flags;
};

class bus_rotate_event_t
{
    public:
        bus_rotate_event_t(const char *filename):
            _next_binlog_filename(filename) {}
        ~bus_rotate_event_t() {}
        int32_t parse_rotate_event_body(char *buf,
                uint32_t &pos, bus_event_t &cur_event);
        std::string& get_binlog_filename()
        {
            return _next_binlog_filename;
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_rotate_event_t);
        uint64_t _next_pos;
        std::string _next_binlog_filename;
};

class bus_format_event_t
{
    public:
        bus_format_event_t() {}
        ~bus_format_event_t() {}
        int32_t parse_format_event_body(char *buf,
                uint32_t &pos, bus_event_t &cur_event);
        uint8_t get_event_posthead_len(uint8_t type)
        {
            assert(type <= _post_head_len_str.size());
            return (uint8_t)_post_head_len_str[type - 1];
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_format_event_t);
        uint16_t _binlog_ver;
        char _serv_ver[51];
        uint32_t _time_stamp;
        uint8_t _head_len;
        std::string _post_head_len_str;
};

class bus_table_map_t
{
    public:
        bus_table_map_t():_cmd(0x13) {}
        ~bus_table_map_t() {}
        int32_t parse_map_event_body(char *buf,
                uint32_t &pos,
                bus_event_t &cur_event);
        void print_info()
        {
            g_logger.debug("tablemap=%s.%s %lu\n",
                    _schema_name.c_str(),
                    _table_name.c_str(),
                    _column_count);
        }
        uint8_t get_column_type(uint64_t index)
        {
            return (uint8_t)_column_type_def.at(index);
        }

        uint16_t get_column_meta(uint64_t index)
        {
            return _meta_vec[index];
        }
        std::string& get_schema_name()
        {
            return _schema_name;
        }
        std::string& get_table_name()
        {
            return _table_name;
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_table_map_t);
        uint8_t _cmd;
        uint64_t _table_id;
        uint16_t _flags;
        uint8_t _schema_name_length;
        std::string _schema_name;
        uint8_t _table_name_length;
        std::string _table_name;
        uint64_t _column_count;
        bus_mem_block_t _column_type_def;
        uint64_t _column_meta_length;
        bus_mem_block_t _column_meta_def;
        std::vector<uint16_t> _meta_vec;
        bus_mem_block_t _null_bitmap;
};
class bus_row_event_t
{
    public:
        bus_row_event_t() {}
        void init(uint8_t cmd, uint8_t ver)
        {
            _cmd = cmd;
            _ver = ver;
        }
        ~bus_row_event_t() {}
        int32_t parse_row_event_body(char *buf,
                uint32_t &pos,
                bus_event_t &cur_event);
        bool parse_row(char *buf,
                uint32_t &pos,
                uint32_t null_bitmap_size,
                row_t *row,
                bus_mem_block_t &present_bitmap,
                bool is_old,
                bus_event_t &cur_event);
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_row_event_t);
    private:
        int32_t _ver;
        uint8_t _cmd;
        uint64_t _table_id;
        uint16_t _flags;
        uint16_t _extra_data_length;
        std::string _extra_data;
        uint64_t _column_count;
        bus_mem_block_t _present_bitmap1;
        bus_mem_block_t _present_bitmap2;
        bus_mem_block_t _null_bitmap;
};
class bus_event_t
{
    public:
        bus_event_t(uint32_t _statid,
                data_source_t *_src_source,
                uint32_t _src_index,
                const char *_filename,
                uint64_t _offset,
                uint64_t _rowindex,
                uint64_t _cmdindex):rotate_event(_filename),
        statid(_statid), src_source(_src_source),
        src_partnum(_src_index), check_position_flag(true)
        {
            dump_position.set_position(_filename, _offset, _rowindex, _cmdindex);
        }
        ~bus_event_t() {}
        int32_t parse_event(char *buf, uint32_t &pos, uint32_t packlen);
        bool init_event_schema_charset();
        uint8_t get_event_posthead_len(uint8_t type)
        {
            return format_event.get_event_posthead_len(type);
        }
        std::string& get_map_table_name()
        {
            return map_event.get_table_name();
        }
        std::string& get_map_schema_name()
        {
            return map_event.get_schema_name();
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_event_t);
    public:
        bus_event_head_t event_head;
        bus_table_map_t map_event;
        bus_format_event_t format_event;
        bus_rotate_event_t rotate_event;
        bus_row_event_t row_event;
        /* 统计id */
        uint32_t statid;
        /* mysql数据源的指针以及当前索引号 */
        data_source_t *src_source;
        uint32_t src_partnum;
        /* dump的开始位置 */
        mysql_position_t dump_position;
        /* map event的当前位置 */
        mysql_position_t map_position;
        /* map table的schema指针 */
        schema_t* map_schema;
        /* map table的字符集 */
        std::string* map_charset;
        convert_charset_t map_convert;
        /* 位置检查 */
        bool check_position_flag;
        /* 包得大小 */
        uint32_t pack_len;
};
class bus_packet_t
{
    public:
        bus_packet_t(uint32_t statid,
                const char *filename,
                uint64_t offset,
                uint64_t rowindex,
                uint64_t cmdindex,
                data_source_t *src_source,
                uint32_t src_index);
        ~bus_packet_t();
        int read_packet(int fd);
        int32_t parse_packet();
    private:
        void _add_mem(uint32_t sz);
        DISALLOW_COPY_AND_ASSIGN(bus_packet_t);
    protected:
        uint32_t _bodylen;
        uint8_t _seqid;
        char *_body;
        uint32_t _body_mem_sz;
    public:
        bus_event_t event_pack;
        bus_error_packet_t error_pack;
};
class bus_repl_t
{
    public:
        bus_repl_t(uint32_t statid,
                const char *filename,
                uint64_t offset,
                uint64_t rowindex,
                uint64_t cmdindex,
                int _fd,
                data_source_t *src_source,
                uint32_t src_index,
                uint32_t server_id):
            dump_cmd(filename, offset, server_id),
            pack(statid, filename, offset, rowindex, cmdindex, src_source, src_index),
            fd(_fd) {}
        ~bus_repl_t() {}
        int32_t start_repl(bool &stop_flag);
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_repl_t);
    public:
        bus_dump_cmd_t dump_cmd;
        bus_packet_t pack;
        int fd;
};
}//namespace bus
#endif  //__BUS_EVENT_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
