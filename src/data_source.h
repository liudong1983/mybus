/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
/**
 * @file data_source.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/23 22:06:10
 * @version $Revision$
 * @brief 
 *  封装redis, hbase等数据源读写操作
 **/

#ifndef  __DATA_SOURCE_H_
#define  __DATA_SOURCE_H_
#include <sys/types.h>
#include </usr/include/regex.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <string>
#include <vector>
#include "hiredis.h"
#include "mysql.h"
#include "bus_util.h"
#include "hash_table.h"
#include "bus_position.h"

namespace bus{

#define MASTER_REDIS_FAIL -1
#define MASTER_CMD_FAIL -2
#define MASTER_REDIS_SUCC 0
#define MASTER_REDIS_EMPTY -3
#define MASTER_REDIS_POS_FAIL -4
#define MASTER_REDIS_DATA_FAIL 5
class schema_t;
class bus_job_t;
class db_object_id
{
    public:
        db_object_id(const char *dbname, const char *tname):
            schema_name(dbname),table_name(tname) {}
        bool is_equal(const char *dbname, const char *tbname)
        {
            if (!strcmp(schema_name.c_str(), dbname) &&
                    !strcmp(table_name.c_str(), tbname))
            {
                return true;
            }
            return false;
        }
        std::string schema_name;
        std::string table_name;
};

class bus_regex_t
{
    public:
        explicit bus_regex_t()
        {
            _ok = false;
            _match_flag = REG_EXTENDED;
        }

        bool init(const char *pattern)
        {
            if (pattern == NULL) {
                g_logger.error("pattern is NULL");
                return false;
            }
            snprintf(_pattern, sizeof(_pattern), "%s", pattern);
            char ebuffer[128];
            int ret = regcomp(&_reg, _pattern, _match_flag);
            if (ret != 0)
            {
                regerror(ret, &_reg, ebuffer, sizeof(ebuffer));
                g_logger.error("compile regex=%s fail, error:%s", _pattern, ebuffer);
                return false;
            }
            _ok = true;
            return true;
        }

        bool check(const char *text)
        {
            assert(text != NULL);
            if (!_ok)
            {
                g_logger.error("regex should be init first");
                return false;
            }
            size_t nmatch = 1;
            regmatch_t pmatch[1];
            int status = regexec(&_reg, text, nmatch, pmatch, 0);
            if(status == 0)
            {
                return true;
            }
            return false;
        }

        ~bus_regex_t() 
        {
            if (_ok)
            {
                regfree(&_reg);
                _ok = false;
            }
        };

        bool is_ok()
        {
            return _ok;
        }
    private:
        bool _ok;
        regex_t _reg;
        int _match_flag;
        char _pattern[64];
};

/**
 * @brief 数据源信息描述
 */
class data_source_t
{
    public:
        data_source_t(const char *ip, int port,
                source_t type, int online_flag):
            _type(type), _table_charset(NULL)
        {
            snprintf(_ip, sizeof(_ip), "%s", ip);
            _port = port;
            _online_flag = online_flag;
            pthread_mutex_init(&src_mutex, NULL);
        }

        data_source_t(const char *ip, int port,
                source_t type):
            _type(type), _table_charset(NULL)
        {
            snprintf(_ip, sizeof(_ip), "%s", ip);
            _port = port;
            _online_flag = 1;
            pthread_mutex_init(&src_mutex, NULL);
        }
        ~data_source_t();
        const char* get_ip() const
        {
            return _ip;
        }

        const int get_port() const
        {
            return _port;
        }

        mysql_position_t &get_position()
        {
            return _sourcepos;
        }

        /**
         *  @brief 对于给定的位置信息和原数据源的位置信息进行比较，
         *  如果小于当前的位置，则设定为指定位置
         */
        bool set_position(const char *filename, const char* pos,
                const char* rowoffset, const char* cmdoffset, const char* prefix, bool force);
        bool set_position(const char *filename,  long pos,
                long rowoffset, long cmdoffset, bool force);

        std::string* get_table_charset(const char *table_name)
        {
            return _table_charset->get_value(table_name);
        }

        void set_table_charset(const char *table_name, const char *charset)
        {
            std::string charset_str(charset);
            _table_charset->set_value(table_name, charset_str);
        }

        bool construct_hash_table();
        bool check_struct(schema_t *sch,
                std::vector<db_object_id> &data);
        int32_t get_online_flag()
        {
            return _online_flag;
        }
        void set_online_flag(int online_flag)
        {
            _online_flag = online_flag;
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(data_source_t);
        /* 数据源ip,port */
        char _ip[128];
        int32_t _port;
        /* 数据源位置信息  */
        mysql_position_t _sourcepos;
        /* 数据源类型 */
        source_t _type;
        /* online flag */
        int32_t _online_flag;
        /* 字符集相关 */
        hashtable_t<std::string> *_table_charset;
        /* pthread mutex */
    public:
        pthread_mutex_t src_mutex;
};

class bus_master_t
{
    public:
        bus_master_t(const char *ip, const int32_t port):_port(port), 
        _context(NULL)
        {
            snprintf(_ip, sizeof(_ip), "%s", ip);
        }
        ~bus_master_t();
        bool init();
        /**
         * @brief 获取bis对应的指定bus的所有上有上游数据源
         */
        int get_source(const char *bis, 
                const char *ip, 
                int32_t port, 
                std::vector<data_source_t*> &source,
                const source_t &srctype);
        //获取source schema
        int get_all_source_schema(const char *bis, 
                std::vector<schema_t*> &source);
        /**
         * @brief 获取bis目标数据源的所有切片
         */
        int get_dst(const char *bis, 
                std::vector<data_source_t*> &dst,
                const source_t &dsttype);
        /**
         * @breif 获取某个数据源在某个下游的位置信息
         *
         */
        int get_rediscounter_position(const char *bis,
                data_source_t *ds, mysql_position_t *conn_pos);
        int get_redis_position(const char *bis,
                data_source_t *ds, mysql_position_t *pos);
        bool set_rediscounter_full_position();
        bool set_redis_full_position();
        /**
         * @brief 获取某个业务进行全量或者增量复制
         */
        int get_transfer_type(const char *bis, transfer_type_t &transfer_type);
        int get_all_source_type(const char *bis, source_t &src_type, source_t &dst_type);
        int get_charset_info(const char *bis, char *charset, int len);
        int get_target_size(const char *bis,  long &target_size);

        /**
         * @brief 获取某个业务的用户名以及密码
         */
        int get_user_info(const char *bis, char *user, int user_len,
        char *pwd, int pwd_len);
        int get_ds_online_flag(const char *bis, data_source_t *ds);

        /**
         * @brief 获取hbase全量sender的线程数目
         */
        int get_hbase_full_sender_thread_num(const char *, uint64_t &);
        /**
         * @brief 获取全量同步结束后，是否checkpos
         */
        int get_mysql_full_check_pos(const char *, bool &);
    private:
    private:
        char _ip[128];
        int32_t _port;
        redisContext *_context;
};
class bus_mysql_t
{
    public:
        bus_mysql_t():_conn(NULL) {}
        bus_mysql_t(const char *ip, int32_t port,
                const char *user, const char *pwd);
        ~bus_mysql_t();
        bool init(const char *charset);
        bool init(const char *ip, int32_t port,
                const char *user, const char *pwd, const char* charset);
        void destroy();
        bool check_full_position(data_source_t &source);
        bool get_full_transfer_position(data_source_t &source);
        bool get_full_data(bus_job_t &job, bool &stop_flag,
                const char *charset, uint32_t statid);
        static bool init_library();
        static void destroy_library();
        bool get_bin_prefix(std::string &prefix);
        int get_fd()
        {
            if (_conn == NULL) return -1;
            return _conn->net.fd;
        }
        bool set_block_mode()
        {
            if (_conn == NULL) return false;
            int flags;
            if ((flags = fcntl(_conn->net.fd, F_GETFL)) == -1) {
                g_logger.error("fcntl(F_GETFL): %s", strerror(errno));
                return false;
            }   
            flags &= ~O_NONBLOCK;
            if (fcntl(_conn->net.fd, F_SETFL, flags) == -1) {
                g_logger.error("fcntl(F_SETFL,O_BLOCK): %s", strerror(errno));
                return false;
            }   
            return true;
        }
        bool select_table_charset(data_source_t *ds);
        bool check_table_struct(schema_t *sch,
                std::vector<db_object_id> &data);
        bool check_table_all_struct(schema_t *sch,
                std::vector<db_object_id> &data);
        bool get_match_all_table(schema_t *sch, 
                std::vector<db_object_id> &data);
        bool get_bin_format_flag(std::string &format_flag);
        bool get_bin_open_flag(std::string &open_flag);

    private:
        DISALLOW_COPY_AND_ASSIGN(bus_mysql_t);
        MYSQL *_conn;
        char _ip[128];
        int32_t _port;
        char _user[32];
        char _pwd[32];
};
bool parse_position(std::string &str, std::string &filename,
    std::string &offset, std::string &rowoffset, std::string &cmdoffset);


}//namespace bus


#endif  //__DATA_SOURCE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
