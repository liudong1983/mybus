/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_warp.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/29 11:13:41
 * @version $Revision$
 * @brief 
 *  
 **/

#ifndef  __BUS_WARP_H_
#define  __BUS_WARP_H_
#include <stdio.h>
#include <vector>
#include <dlfcn.h>
#include <string>
#include "bus_config.h"
#include "bus_engine.h"
#include "bus_server.h"
#include "bus_row.h"
#include "bus_stat.h"

namespace bus {
/**
 * @brief 封装了配置，引擎，服务
 */
typedef int (*transform)(row_t *);
typedef int (*initso)(void*, char *, size_t len);
class bus_t
{
    public:
        bus_t():_trans_stat(INIT), _func(NULL), _init_func(NULL),
        _handle(NULL) 
        {
            _so_version[0] = '\0';
        }
        ~bus_t() 
        {
            if (_handle != NULL)
            {
                dlclose(_handle);
                _handle = NULL;
            }
        }
        bool init(const int argc, char* const*);
        bool start_trans(int &errorcode);
        bool stop_trans(int &errorcode);
        schema_t* get_schema(const char *src_table_name)
        {
            return _config.get_match_schema(NULL, src_table_name);
        }

        bus_config_t& get_config()
        {
            return _config;
        } 

        bus_engine_t& get_engine()
        {
            return _engine;
        }

        bus_server_t& get_server()
        {
            return _server;
        }

        bus_stat_t& get_stat()
        {
            return _stat;
        }

        void set_mysql_full_check_pos(bool check)
        {
            return _config.set_mysql_full_check_pos(check);
        }

        source_t get_dst_type()
        {
            return _config.get_dstds_type();
        }

        void set_posfreq(uint64_t freq)
        {
            _engine.set_update_num(freq);
        }

        int process(row_t *row)
        {
            assert(row != NULL);
            return _func(row);
        }

        void info(char *buf, size_t len);

        void set_transfer_stat(transfer_state_t trans_stat)
        {
            _trans_stat = trans_stat;
        }

        transfer_state_t get_transfer_stat()
        {
            return _trans_stat;
        }

        bool add_blacklist(const char *dbname, const char *tbname)
        {
            if (_trans_stat != INIT)
            {
                return false;
            }
            _config.add_black_list(dbname, tbname);
            return true;
        }

        bool del_blacklist(const char *dbname, const char *tbname)
        {
            if (_trans_stat != INIT)
            {
                return false;
            }
            _config.del_black_list(dbname, tbname);
            return true;
        }
        bool clear_blacklist()
        {
            if (_trans_stat != INIT)
            {
                return false;
            }
            _config.clear_black_list();
            return true;
        }
        
        bool get_source_info(std::string &info);
        void cron_func();
        bool shutdown(int &errorcode);
        void stop_trans_and_exit();
        void prepare_exit();
        bool reload_so(int &errorcode);
        bool get_match_info(std::string &info);
        void get_thread_info(std::string &info)
        {
            if (is_init_ok())
            {
                _stat.get_thread_info(info);
            }
        }
        void get_thread_stat_info(std::string &info)
        {
            if (is_init_ok())
            {
                _stat.get_thread_stat_info(info);
            }
        }
        bool is_init_ok()
        {
            uint32_t psz = _stat.get_producer_size();
            uint32_t csz = _stat.get_consumer_size();
            uint32_t pnum, cnum;
            _engine.get_thread_num(pnum, cnum);
            if (pnum == psz && cnum == csz) {
                return true;
            }
            return false;
        }
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_t);
        bool _split();
        bool _init_pool();
        //redis/rediscounter消费者线程池
        bool _init_redis_consumer_pool();
        //hbase消费者线程池
        bool _init_hbase_consumer_pool();
        bool _load_transform();
        bus_config_t _config;
        bus_engine_t _engine;
        bus_server_t _server;
        bus_stat_t _stat;
        transfer_state_t _trans_stat;
        transform _func;
        initso _init_func;
        void* _handle;
        char _so_version[32];
};
//bool parse_position(std::string &str, std::string &filename,
//        std::string &offset, std::string &rowoffset);
extern bus_t g_bus;
}//namespace bus


#endif  //__BUS_WARP_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
