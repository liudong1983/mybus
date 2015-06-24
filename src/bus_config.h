/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
/**
 * @file bus_resource.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/14 20:57:36
 * @version $Revision$
 * @brief 主要数据结构定义
 * 
 **/

#ifndef  __BUS_RESOURCE_H_
#define  __BUS_RESOURCE_H_
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <vector>
#include <map>
#include <string>
#include "bus_util.h"
#include "data_source.h"
#include "tinyxml2.h"
#include "bus_position.h"
#include "threadpool.h"
//#include "bus_hbase_util.h"

namespace bus {
#define BUS_SUCC 0
#define BUS_FAIL -1

class column_t
{
    public:
        column_t(const char *column_name):_type(-1),_seq(-1)
        {
            snprintf(_column_name, sizeof(_column_name), "%s", column_name);
        }

        column_t(const char *column_name, int32_t seq, 
                int32_t type):_type(type), _seq(seq) 
        {
            snprintf(_column_name, sizeof(_column_name), "%s", column_name);
        }

        const char* get_column_name() const
        {
            return _column_name;
        }

        uint32_t get_column_type() const
        {
            return _type;
        }

        void set_column_type(uint32_t type)
        {
            _type = type;
        }

        uint32_t get_column_seq() const
        {
            return _seq;
        }

        void set_column_seq(uint32_t seq)
        {
            _seq = seq;
        }

    private:
        char _column_name[64];
        uint32_t _type;
        // 此列在行中的位置 
        uint32_t _seq;
};


/**
 * @brief 表元数据数据结构
 */
class schema_t
{
    public:
        schema_t(const long id, const char *name, const char *db);
        ~schema_t();

        void add_column(column_t* col);
        const char* get_schema_db() const
        {
            return _db;
        }
        const char* get_schema_name() const
        {
            return _table;
        }

        std::vector<column_t*>& get_columns()
        {
            return _columns;
        }
        bool init_regex();
        bool init_namemap();
        long get_schema_id()
        {
            return _schema_id;
        }
        bool get_all_columns_flag()
        {
            return _all_columns_flag;
        }
        void set_all_columns_flag(bool l)
        {
            _all_columns_flag = l;
        }
        void set_record_flag(bool l)
        {
            _record_flag = l;
        }
        bool get_record_flag()
        {
            return _record_flag;
        }
        
        std::map<std::string, int>* get_column_namemap()
        {
            return &_namemap;
        }
        bool check_schema(const char *dbname,
                const char *tablename);
        column_t *get_column_byseq(int seq)
        {
            std::size_t sz = _columns.size();
            for (uint32_t i = 0; i < sz; ++i)
            {
                int col_seq = _columns[i]->get_column_seq();
                if (seq == col_seq) return _columns[i];
            }
            return NULL;
        }
        uint32_t get_column_index(const char *name);
    private:
        /* 除了下面这些特殊列之外的其他列 */
        std::vector<column_t*> _columns;
        //_namemap帮助row_t::get_value_byname()快速查询
        std::map<std::string, int> _namemap;

        /* table的名称 */
        char _table[64];
        char _db[64];
        
        /* 正则表达式对象 */
        bus_regex_t _table_name_regex;
        bus_regex_t _db_name_regex;
        long _schema_id;
        bool _all_columns_flag;
        /*该schema是否添加到统计信息中*/
        bool _record_flag;
};

/**
 * @brief 配置信息数据结构
 */
class bus_config_t
{
    public:
        bus_config_t();
        schema_t* get_target_schema(const char *schema_name);
        std::vector<schema_t*>& get_src_schema();
        std::vector<schema_t*>& get_dst_schema();
        schema_t* get_match_schema(const char *db_name, const char *table_name);
        //bool parse(const char *bin_file, const char *config_filepath);
        bool parse(const int argc, char *const *argv);
        std::vector<data_source_t*>& get_source();
        std::vector<data_source_t*>& get_dst();
        bool get_master_info();
        bool check_mysql_schema();
        transfer_type_t get_transfer_type()
        {
            return _transfer_type;
        }
        source_t get_srcds_type()
        {
            return _source_type;
        }
        source_t  get_dstds_type()
        {
            return _dst_type;
        }
        const char* get_user_name() const
        {
            return _user;
        }
        const char* get_user_pwd() const
        {
            return _pwd;
        }
        bool init_so_path(const char *bin_file);
        bool init_tmp_so();
        char* get_tmp_so_path()
        {
            return _tmp_path;
        }

        const char* get_so_path()
        {
            return _so_path;
        }

        int get_server_id()
        {
            return _server->get_port();
        }

        const char* get_bss()
        {
            return _bss;
        }
        const char *get_metabss()
        {
            return _metabss;
        }

        const char *get_charset()
        {
            return _charset;
        }

        /*
        bool init_pidfile_log();

        const char* get_config_dir()
        {
            return _config_dirpath;
        }
        */
        char* get_pidfile()
        {
            return _pidfile;
        }

        bool init_charset_table();
        bool get_all_redis_position();
        bool get_all_hbase_position();
        bool get_source_position(data_source_t *srcds);
        std::vector<mysql_position_t*>& get_all_pos()
        {
            return _all_pos;
        }
        ~bus_config_t();
        data_source_t *get_local_server()
        {
            return _server;
        }
        long get_target_size()
        {
            return _target_size;
        }
        bool get_prefix();
        bool check_binlog_config();
        char *get_binlog_prefix()
        {
            return _binlog_prefix;
        }
        void add_black_list(const char *dbname, const char *tbname)
        {
            db_object_id mytb(dbname, tbname);
            if (is_black(dbname, tbname))
            {
                return;
            }
            _filter_list.push_back(mytb);
        }
        void del_black_list(const char *dbname, const char *tbname)
        {
            for (std::vector<db_object_id>::iterator it = _filter_list.begin();
                    it != _filter_list.end();
                    ++it)
            {
                db_object_id &curid = *it;
                if (curid.is_equal(dbname, tbname))
                {
                    _filter_list.erase(it);
                    return;
                }
            }
        }
        bool is_black(const char *dbname, const char *tbname)
        {
            std::size_t sz = _filter_list.size();
            for (uint32_t i = 0; i < sz; ++i)
            {
                db_object_id &curid = _filter_list[i];
                if (curid.is_equal(dbname, tbname))
                {
                    return true;
                }
            }
            return false;
        }

        void init_thrift_record()
        {
            _thrift_record.clear();
            for (uint32_t i = 0; i < _target_size; ++i)
                _thrift_record.push_back(0);
        }

        bool set_thrift_record(uint32_t index, int32_t value)
        {
            lock_t lock(&_mutex);
            if (index >= _thrift_record.size())
            {
                g_logger.error("out of index");
                return false;
            }

            //设置为不可用
            if (-1 == value) 
            {
                _thrift_record[index] = -1;
                return true;
            }
            int32_t count = _thrift_record[index];
            _thrift_record[index] = ++count;
            return true;
        }

        void reset_thrift_record()
        {
            uint32_t size = _thrift_record.size();
            for (uint32_t i = 0; i < size; i++)
            {
                if (-1 == _thrift_record[i])
                    _thrift_record[i] = 0;
            }
        }

        uint32_t get_thrift()
        {
            lock_t lock(&_mutex);
            uint32_t size = _thrift_record.size();
            if (0 == size) 
            {
                g_logger.error("size of thrift record is 0");
                return size;
            }

            uint32_t index = size;
            int32_t count = size + 1;
            bool find_flag = false;
            for (uint32_t i = 0; i < size; i++)
            {
                if (-1 == _thrift_record[i]) continue;
                if (count > _thrift_record[i]) 
                {
                    count = _thrift_record[i];
                    index = i;
                    find_flag = true;
                }
            }

            if (!find_flag)
            {
                g_logger.error("can not find availble thrift server");
                return size;
            }

            return index;
        }

        bool update_source_position(uint32_t src_partnum);

        void clear_black_list()
        {
            _filter_list.clear();
        }

        void get_blacklist(std::string &info)
        {
            std::size_t sz = _filter_list.size();
            for (uint32_t i = 0; i < sz; ++i)
            {
                db_object_id &curid = _filter_list[i];
                info.append(curid.schema_name);
                info.append(".");
                info.append(curid.table_name);
                info.append(";");
            }
        }
        bool get_master_source();
        bool get_master_dst();
        bool get_source_type();
        bool get_user_info();
        bool get_charset_info();

        //add by zhangbo6@staff.sina.com.cn
        //for hbase_positions
        bool get_position_map(std::map<std::string, std::string> &posmap);

        //使用了strtok(),线程不安全的
        bool get_source_schema();
        bool init_schema_regex();
        bool init_schema_namemap();
        char* get_logfile()
        {
            return _logfile;
        }
        bool get_master_transfer_type();
        bool get_master_mysql_full_check_pos();
        //从meta server中获取thread num
        bool get_master_hbase_full_thread_num();
        //从command line设置thread num
        /*
        void set_hbase_full_thread_num(uint64_t num)
        {
            _hbase_full_thread_num = num;
        }
        */
        uint64_t get_hbase_full_thread_num()
        {
            return _hbase_full_thread_num;
        }
        //从command line设置全量reader结束后，是否check position
        void set_mysql_full_check_pos(bool check)
        {
            _mysql_full_check_pos = check;
        }
        bool get_mysql_full_check_pos()
        {
            return _mysql_full_check_pos;
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_config_t);
        bool _parse_meta(const char*);
        bool _init_logpath(const char*, const char*);
        bool _get_target_size();
        bool _get_online_flag();
        /* .pid .log .so文件路径 */
        char _pid_dirpath[1024];
        /* 上游数据源信息 */
        std::vector<data_source_t*> _source;
        /* 下游数据源信息 */
        std::vector<data_source_t*> _dst;
        /* 下游thriftserver的连接数 */
        std::vector<int32_t> _thrift_record;
        /* 上游schema元数据 */
        std::vector<schema_t*> _source_schema;
        /* 下游schema元数据 */
        std::vector<schema_t*> _dst_schema;
        /* redis配置库数据源信息  */
        data_source_t *_meta;
        /* 本地server的ip地址，以及端口 */
        data_source_t *_server;
        /* 上游数据源的类型 */
        source_t _source_type;
        /* 下游数据源的类型 */
        source_t _dst_type;
        /* 业务的前缀 */
        char _bss[32];
        char _metabss[32];
        /* 复制的类型 */
        transfer_type_t _transfer_type;
        /* 用户名，密码 */
        char _user[32];
        char _pwd[32];
        /* so路径 */
        char _so_path[1024];
        char _tmp_path[1024];
        /* server ID */
        int _server_id;
        /* charset */
        char _charset[16];
        /* pidfile */
        char _pidfile[1024];
        char _logfile[1024];
        /* 位置信息 */
        std::vector<mysql_position_t*> _all_pos;
        /*下游个数 */
        long _target_size;
        /* binlog prefix */
        char _binlog_prefix[64];
        /* hbase全量sender的线程数 */
        uint64_t _hbase_full_thread_num;
        /* mysql全量检查position开关 */
        bool _mysql_full_check_pos;
        /* schema黑名单 */
        std::vector<db_object_id> _filter_list;
        /* 用于get/set thrift record 互斥 */
        pthread_mutex_t _mutex;
};


} //namespace bus


#endif  //__BUS_RESOURCE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
