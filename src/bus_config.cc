/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
/**
 * @file bus_resource.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/15 15:34:05
 * @version $Revision$
 * @brief 实现bus_resource定义
 *  
 **/
#include <getopt.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "bus_config.h"
#include "bus_wrap.h"
#include "bus_position.h"
#include "bus_hbase_util.h"

namespace bus{
void clear_all_ds(std::vector<data_source_t*> &source)
{
    std::size_t sz = source.size();
    for (std::size_t i = 0; i < sz; ++i)
    {
        delete source[i];
        source[i] = NULL;
    }
    source.clear();
}
void clear_all_pos(std::vector<mysql_position_t*> &pos_vec)
{
    std::size_t psz = pos_vec.size();
    for (std::size_t i = 0; i < psz; ++i)
    {
        delete pos_vec[i];
        pos_vec[i] = NULL;
    }
    pos_vec.clear();
}
/*****************************************************/
schema_t::schema_t(const long id, const char *db, const char *name)
    :_schema_id(id), _all_columns_flag(false), _record_flag(true)
{
    snprintf(_table, sizeof(_table), "%s", name);
    if(db != NULL)
    {
        snprintf(_db, sizeof(_db), "%s", db);
    } else 
    {
        _db[0] = '\0';
    }
}

void schema_t::add_column(column_t* col)
{
    _columns.push_back(col);
}

bool schema_t::init_regex()
{
    /* 编译正则表达式 */
    if (_table[0] == '\0' || !_table_name_regex.init(_table))
    {
        g_logger.error("table name=%s regex init fail", _table);
        return false;
    }

    if (_db[0] == '\0' || !_db_name_regex.init(_db))
    {
        g_logger.error("db name=%s regex init fail", _db);
        return false;
    }
    return true;
}

bool schema_t::init_namemap()
{
    //填充std::map<std::string, int> namemap，用于row_t::get_value_byname()中快速查询
    for(std::vector<column_t*>::iterator it = _columns.begin();
            it != _columns.end();
            ++it)
    {
        column_t* col = *it;
        assert(col != NULL);
        std::string name = const_cast<char*>(col->get_column_name());
        int32_t seq = col->get_column_seq();
        g_logger.debug("%s:%s _namemap %s:%d", _db, _table, name.c_str() , seq);
        assert(name != NULL);
        _namemap[name] = seq;
    }

    return true;
}

uint32_t schema_t::get_column_index(const char* name)
{
    std::map<std::string, int>::iterator it = _namemap.find(const_cast<char*>(name));
    if (it == _namemap.end()) {
        g_logger.error("column %s can not find", name);
        return static_cast<uint32_t>(-1);
    }
    return it->second;
}

bool schema_t::check_schema(const char *dbname, const char *tablename)
{
    bus_config_t &config = g_bus.get_config();
    if (config.is_black(dbname, tablename))
    {
        return false;
    }
    if (!_table_name_regex.is_ok() || !_db_name_regex.is_ok())
    {
        g_logger.error("[%s.%s] regex is not ok", _db, _table);
        return false;
    }
    if (!_table_name_regex.check(tablename))
    {
        return false;
    }

    if (!_db_name_regex.check(dbname))
    {
        return false;
    }

    return true;
}

schema_t::~schema_t()
{
    //delete other rows
    for (std::vector<column_t*>::iterator it = _columns.begin(); 
            it != _columns.end(); 
            ++it)
    {
        if (*it != NULL)
        {
            delete *it;
            *it = NULL;
        }
    }
}

/******************************************************/
bus_config_t::bus_config_t():_hbase_full_thread_num(1),
    _mysql_full_check_pos(false)
{
    _pid_dirpath[0] = '\0';
    _bss[0] = '\0';
    _metabss[0] = '\0';
    _transfer_type = TRANSFER_EMPTY;
    _source_type = SOURCE_EMPTY;
    _dst_type = SOURCE_EMPTY;
    _user[0] = '\0';
    _pwd[0] = '\0';
    _so_path[0] = '\0';
    _tmp_path[0] = '\0';
    _charset[0] = '\0';
    _pidfile[0] = '\0';
    _logfile[0] = '\0';
    _target_size = 0;
    _binlog_prefix[0] = '\0';
    pthread_mutex_init(&_mutex, NULL);
}
bus_config_t::~bus_config_t()
{
    for (std::vector<data_source_t*>::iterator it =  _source.begin();
            it != _source.end(); 
            ++it)
    {
        if (*it != NULL) {
            delete *it;
            *it = NULL;
        }
    }

    for (std::vector<data_source_t*>::iterator it =  _dst.begin();
            it != _dst.end(); 
            ++it)
    {
        if (*it != NULL) {
            delete *it;
            *it = NULL;
        }
    }

    for (std::vector<schema_t*>::iterator it =  _source_schema.begin();
            it != _source_schema.end(); 
            ++it)
    {
        if (*it != NULL) delete *it;
    }

    for (std::vector<schema_t*>::iterator it =  _dst_schema.begin();
            it != _dst_schema.end(); 
            ++it)
    {
        if (*it != NULL) delete *it;
    }

    if (_server != NULL)
    {
        delete _server;
        _server = NULL;
    }

    clear_all_pos(_all_pos);
    pthread_mutex_destroy(&_mutex);
}

bool bus_config_t::_init_logpath(const char* port, const char* logdir)
{
    /*设置.pid .so .log文件路径 logdir/databusXXXXX/ */
    snprintf(_pid_dirpath, sizeof(_pid_dirpath), "%s/databus%s", logdir, port);
    int rt = mkdir(_pid_dirpath, 0755);
    if (rt != 0 && errno != EEXIST) {
        g_logger.error("create %s fail: %s", _pid_dirpath, strerror(errno));
        return false;
    }
    snprintf(_pidfile, sizeof(_pidfile), "%s/bus%s.pid",
             _pid_dirpath, port);
    snprintf(_logfile, sizeof(_logfile), "%s/bus%s.log",
             _pid_dirpath, port);

    g_logger.set_logname(_logfile);

    return true;
}

bool bus_config_t::parse(const int argc, char* const *argv)
{
    const char *short_options = "x:p:m:d:l:";
    struct option long_options[] = {
        {"prefix", 1, NULL, 'x'}, 
        {"port", 1, NULL, 'p'}, 
        {"logdir", 1, NULL, 'd'}, 
        {"loglevel", 1, NULL, 'l'}, 
        {"meta", 1, NULL, 'm'}, 
        {0, 0, 0, 0},
    };

    int c = 0;
    int loglevel_n = 0;
    char *prefix = NULL;
    char *port = NULL;
    char *logdir = NULL;
    char *loglevel = NULL;
    char *meta = NULL;
    while((c = getopt_long(argc, argv, short_options, long_options, NULL)) != -1) {
        switch (c) {
            case 'x':
                prefix = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'd':
                logdir = optarg;
                break;
            case 'l':
                loglevel = optarg;
                break;
            case 'm':
                meta = optarg;
                break;
            default:
                g_logger.warn("unknown option %c", c);
                break;
        }
    }
    if (prefix == NULL || port == NULL || meta == NULL) {
        g_logger.error("some of command line argument is NULL");
        return false;
    }

    if (NULL == logdir) logdir = "/data1";
    if (!_init_logpath(port, logdir)) return false;
    g_logger.notice("Command line, prefix:%s port:%s logdir:%s meta:%s", prefix, port, logdir, meta);

    if (NULL != loglevel) {
        loglevel_n = atoi(loglevel);
        if (loglevel_n < LOG_DEBUG || loglevel_n > LOG_ERROR) {
            g_logger.warn("--loglevel:%s is invalid", loglevel);
        }

        if (loglevel_n == 0 && loglevel[0] != '0') {
            g_logger.warn("--loglevel:%s is invalid", loglevel);
        } else {
            g_logger.debug("set loglevel --loglevel:%s", loglevel);
            g_logger.set_loglevel(loglevel_n);
        }

    }

    char ip[32];
    if (!get_inner_ip(ip, sizeof(ip))) {
        g_logger.error("get interface ip faile");
        return false;
    }

    long iport;
    if (!str2num(port, iport) || iport < 0 || iport > 65535)
    {
        g_logger.error("port=%s is invalid", port);
        return false;
    }

    _server = new (std::nothrow)data_source_t(ip, (int)iport, BUS_DS);
    if (_server == NULL) oom_error();
    std::string fullbis(prefix);
    std::size_t sep = fullbis.rfind('_');
    if (sep == std::string::npos || sep == fullbis.size() - 1) {
        g_logger.error("config prefix=%s invalid", prefix);
        return false;
    }
    std::string part1 = fullbis.substr(0, sep);
    snprintf(_metabss, sizeof(_metabss), "%s", prefix);
    snprintf(_bss, sizeof(_bss), "%s", part1.c_str());

    if (!_parse_meta(meta))
    {
        g_logger.error("parse xml meta fail");
        return false;
    }

    if (!init_so_path(argv[0]))
    {
        g_logger.error("init so path fail");
        return false;
    }
    return true;
}
schema_t* bus_config_t::get_match_schema(const char *db_name, const char *table_name)
{
    for (std::vector<schema_t*>::iterator it = _source_schema.begin();
            it != _source_schema.end();
            ++it)
    {
        schema_t *curschema = *it;
        if (curschema->check_schema(db_name, table_name))
        {
            return curschema;
        }
    }

    return NULL;
}

bool bus_config_t::init_schema_regex()
{
    for (std::vector<schema_t*>::iterator it = _source_schema.begin();
            it != _source_schema.end();
            ++it)
    {
        schema_t *curschema = *it;
        if (!curschema->init_regex())
        {
            g_logger.error("init regex schema=%s, table=%s fail",
                    curschema->get_schema_db(), curschema->get_schema_name());
            return false;
        }
    }
    return true;
}

bool bus_config_t::init_schema_namemap()
{
    for (std::vector<schema_t*>::iterator it = _source_schema.begin();
            it != _source_schema.end();
            ++it)
    {
        schema_t *curschema = *it;
        if (!curschema->init_namemap())
        {
            g_logger.error("init namemap schema=%s, table=%s fail",
                    curschema->get_schema_db(), curschema->get_schema_name());
            return false;
        }
    }
    return true;
}

bool bus_config_t::_parse_meta(const char *meta)
{
    std::string str_meta(meta);
    std::size_t sep = str_meta.rfind(':');
    if (sep == std::string::npos || sep == str_meta.size() - 1) {
        g_logger.error("command line argument meta=%s error", meta);
        return false;
    }
    std::string sip = str_meta.substr(0, sep);
    std::string sport = str_meta.substr(sep+1, str_meta.size()-sep-1);

    char ip[32];
    char port[32];
    snprintf(ip, sizeof(ip), "%s", sip.c_str());
    snprintf(port, sizeof(port), "%s", sport.c_str());
    g_logger.debug("meta ip:port %s:%s", ip, port);

    long iport;
    if (!str2num(port, iport))
    {
        g_logger.error("port=%s is invalid", port);
        return false;
    }
    
    _meta = new(std::nothrow) data_source_t(ip, (int)iport, REDIS_DS);
    if (_meta == NULL) oom_error();
    return true;
}

bool bus_config_t::get_source_schema()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    //g_logger.debug("----------CONFIG SERVER %s:%d", _meta->get_ip(), _meta->get_port());
    if (!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    
    int ret =  master.get_all_source_schema(_metabss, _source_schema);
    if (ret == MASTER_REDIS_FAIL)
    {
        g_logger.error("get master source schema fail");
        return false;
    }

    if (ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("master source schema is empty");
        return false;
    }

    return true;
}

bool bus_config_t::check_mysql_schema()
{
    std::size_t sourcesz = _source.size();
    std::size_t schemasz = _source_schema.size();
    
    std::vector<db_object_id> idvec;
    idvec.reserve(10240);
    for (std::size_t i = 0; i < sourcesz; ++i)
    {
        data_source_t *ds = _source[i];
        const char *ip = ds->get_ip();
        const int port = ds->get_port();
        bus_mysql_t conn(ip, port, _user, _pwd);
        if (!conn.init(_charset))
        {
            g_logger.error("[%s:%d] init conn fail", ip, port);
            return false;
        }

        for(std::size_t j = 0; j < schemasz; ++j)
        {
            schema_t *cur_schema = _source_schema[j];
            const char *schema_db = cur_schema->get_schema_db();
            const char *schema_name = cur_schema->get_schema_name();
            idvec.clear();
            if (!conn.get_match_all_table(cur_schema, idvec))
            {
                g_logger.error("[%s:%d][%s:%s] get match all table fail", ip, port,
                        schema_db, schema_name);
                return false;
            }
            //如果schema中指定获取所有行 *
            if (cur_schema->get_all_columns_flag()) {
                if (!conn.check_table_all_struct(cur_schema, idvec))
                {
                    g_logger.error("[%s:%d][%s:%s] check table all struct fail", ip, port,
                            schema_db, schema_name);
                    return false;
                }
            //schema中指定获取特定行 column1#column2#column3......
            } else {
                if (!conn.check_table_struct(cur_schema, idvec))
                {
                    g_logger.error("[%s:%d][%s:%s] check table struct fail", ip, port,
                            schema_db, schema_name);
                    return false;
                }
            }
        }
    }
    return true;
}

bool bus_config_t::get_master_info()
{
    if (!get_source_type())
    {
        g_logger.error("get source type fail");
        return false;
    }

    if (!get_master_source())
    {
        g_logger.error("get master source fail");
        return false;
    }

    if (!get_source_schema())
    {
        g_logger.error("get source schema fail");
        return false;
    }

    if (!init_schema_regex())
    {
        g_logger.error("init schema regex fail");
        return false;
    }

    if (!get_master_dst())
    {
        g_logger.error("get master target fail");
        return false;
    }

    if (!_get_online_flag())
    {
        g_logger.error("get master online flag fail");
        return false;
    }

    if (!get_charset_info())
    {
        g_logger.error("get charset info fail");
        return false;
    }

    if (!_get_target_size())
    {
        g_logger.error("get target size fail");
        return false;
    }
    long targetsz = (long)_dst.size();
    if (_target_size != targetsz)
    {
        g_logger.error("target datasouce size=%ld != %ld",
                targetsz, _target_size);
        return false;
    }

    if (!get_user_info())
    {
        g_logger.error("get username/password fail");
        return false;
    }

    if (_source_type == MYSQL_DS)
    {
        if (!check_mysql_schema())
        {
            g_logger.error("check mysql schema struct fail");
            return false;
        }

        if (!init_schema_namemap())
        {
            g_logger.error("init mysql schema namemap fail");
            return false;
        }
    }

    if (!get_master_transfer_type())
    {
        g_logger.error("get transfer type fail");
        return false;
    }

    if (!get_prefix())
    {
        g_logger.error("get binlog prefix fail");
        return false;
    }

    if (!check_binlog_config())
    {
        g_logger.error("check binlog config fail");
        return false;
    }

    if (_transfer_type == FULL 
            && _source_type == MYSQL_DS
            && !get_master_mysql_full_check_pos())
    {
        g_logger.error("get mysql full check pos fail");
        return false;
    }

    if (_transfer_type == FULL 
            && _dst_type == HBASE_DS
            && !get_master_hbase_full_thread_num())
    {
        g_logger.error("get hbase full therad num fail");
        return false;
    }


    if (_transfer_type == INCREMENT)
    {
        bool rt = false;
        switch (_dst_type)
        {
            case REDIS_DS:
            case REDIS_COUNTER_DS:
                rt = get_all_redis_position();
                break;
            case HBASE_DS:
                rt = get_all_hbase_position();
                break;
            default:
                g_logger.error("invalid dst_type %d", _dst_type);
                break;
        }

        if (!rt) {
            g_logger.error("get all position fail");
            return false;
        }
    }
    return true;
}
bool bus_config_t::get_prefix()
{
    _binlog_prefix[0] = '\0';
    std::size_t sz = _source.size();
    std::string tmp_prefix;
    for (uint32_t i = 0; i < sz; ++i)
    {
        data_source_t* ds = _source[i];
        const char *ip = ds->get_ip();
        const int port = ds->get_port();
        bus_mysql_t mconn(ip, port, _user, _pwd);
        if (!mconn.init(_charset))
        {
            g_logger.error("init [%s:%d] mysql conn fail", ip, port);
            return false;
        }
        if (!mconn.get_bin_prefix(tmp_prefix))
        {
            g_logger.error("[%s:%d] get binlog prefix fail", ip, port);
            return false;
        }

        if (_binlog_prefix[0] == '\0')
        {
            snprintf(_binlog_prefix, sizeof(_binlog_prefix), "%s", tmp_prefix.c_str());
        } else if (strcmp(_binlog_prefix, tmp_prefix.c_str())) {
            g_logger.error("[%s:%d] binlog prefix=[%s] diff [%s]",
                    ip, port, tmp_prefix.c_str(), _binlog_prefix);
            return false;
        }
    }
    if (_binlog_prefix[0] == '\0') {
        g_logger.error("binlog prefix empty");
        return false;
    }
    return true;
}
bool bus_config_t::check_binlog_config()
{
    std::size_t sz = _source.size();
    std::string open_flag, format_flag;
    for (uint32_t i = 0; i < sz; ++i)
    {
        data_source_t* ds = _source[i];
        const char *ip = ds->get_ip();
        const int port = ds->get_port();
        bus_mysql_t mconn(ip, port, _user, _pwd);
        if (!mconn.init(_charset))
        {
            g_logger.error("init [%s:%d] mysql conn fail", ip, port);
            return false;
        }
        if (!mconn.get_bin_open_flag(open_flag))
        {
            g_logger.error("[%s:%d] get binlog open flag fail", ip, port);
            return false;
        }

        if (open_flag != "ON") {
            g_logger.error("[%s:%d] binlog flag [%s] invalid", ip, port,
                    open_flag.c_str());
            return false;
        }

        if (!mconn.get_bin_format_flag(format_flag))
        {
            g_logger.error("[%s:%d] get binlog row flag fail", ip, port);
            return false;
        }

        if (format_flag != "ROW") {
            g_logger.error("[%s:%d] binlog format flag [%s] invalid", ip, port,
                    format_flag.c_str());
            return false;
        }
    }
    return true;
}
bool bus_config_t::_get_target_size()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if (!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    int ret = master.get_target_size(_metabss, _target_size);
    if (ret != MASTER_REDIS_SUCC)
    {
        g_logger.error("get target size fail");
        return false;
    }
    return true;
}
bool bus_config_t::get_charset_info()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if (!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    int ret = master.get_charset_info(_metabss, _charset, sizeof(_charset));
    if (ret != MASTER_REDIS_SUCC)
    {
        g_logger.error("get online flag fail");
        return false;
    }
    return true;
}
bool bus_config_t::get_user_info()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if(!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    int ret = master.get_user_info(_metabss, 
            _user, sizeof(_user), 
            _pwd, sizeof(_pwd));

    if (ret == MASTER_REDIS_FAIL)
    {
        g_logger.error("get master username/password fail");
        return false;
    }else if (ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("master username/password is not  exist");
        return false;
    }

    return true;
}
bool bus_config_t::get_source_type()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if(!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }

    int ret = master.get_all_source_type(_metabss, _source_type, _dst_type);
    if (ret == MASTER_REDIS_FAIL)
    {
        g_logger.error("get master transfer type fail");
        return false;
    }else if (ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("master transfer type is not  exist");
        return false;
    }
    return true;
}
bool bus_config_t::get_master_transfer_type()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if(!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }

    int ret = master.get_transfer_type(_metabss, _transfer_type);
    if (ret == MASTER_REDIS_FAIL)
    {
        g_logger.error("get master transfer type fail");
        return false;
    }else if (ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("master transfer type is not  exist");
        return false;
    }

    return true;
}
bool bus_config_t::get_master_source()
{
    clear_all_ds(_source);
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if (!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    
    int ret =  master.get_source(_metabss,
            _server->get_ip(), _server->get_port(), _source, _source_type);
    if (ret == MASTER_REDIS_FAIL)
    {
        g_logger.error("get master target datasource fail");
        return false;
    }

    if (ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("master target datasource is empty");
        return false;
    }

    return true;
}
bool bus_config_t::get_master_dst()
{
    clear_all_ds(_dst);
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if (!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    
    int ret =  master.get_dst(_metabss, _dst, _dst_type);
    if (ret == MASTER_REDIS_FAIL)
    {
        g_logger.error("get master target datasource fail");
        return false;
    }

    if (ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("master target datasource is empty");
        return false;
    }

    if (ret == MASTER_REDIS_DATA_FAIL)
    {
        g_logger.error("master target datasource data fail");
        return false;
    }

    return true;
}

bool bus_config_t::get_master_hbase_full_thread_num()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if (!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    
    int ret =  master.get_hbase_full_sender_thread_num(_metabss,
            _hbase_full_thread_num);
    if (ret == MASTER_REDIS_FAIL ||
            ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("get master hbase full sender thread num  fail");
        return false;
    }

    return true;
}

bool bus_config_t::get_master_mysql_full_check_pos()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if(!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }

    int ret = master.get_mysql_full_check_pos(_metabss,
            _mysql_full_check_pos);
    if (ret == MASTER_REDIS_FAIL ||
            ret == MASTER_REDIS_EMPTY)
    {
        g_logger.error("get master transfer type fail");
        return false;
    }

    return true;
}

bool bus_config_t::_get_online_flag()
{
    bus_master_t master(_meta->get_ip(), _meta->get_port());
    if (!master.init())
    {
        g_logger.error("connect to meta server fail");
        return false;
    }
    std::size_t sz = _dst.size();
    for (std::size_t i = 0; i < sz; ++i)
    {
        data_source_t *ds = _dst[i];
        int ret = master.get_ds_online_flag(_metabss, ds);
        if (ret != MASTER_REDIS_SUCC)
        {
            g_logger.error("get online flag fail");
            return false;
        }
    }
    return true;
}

bool bus_config_t::init_so_path(const char *bin_file)
{
    char *res = realpath(bin_file, _so_path);
    if (!res) {
        g_logger.error("bin file=%s realpath fail",
                bin_file);
        return false;
    }
    char *pdir = strrchr(_so_path, '/');
    snprintf(pdir + 1,
            sizeof(_so_path) - (pdir - _so_path) - 1,
            "%s/libprocess.so", _bss);
    return true;
}

bool bus_config_t::init_tmp_so()
{
    snprintf(_tmp_path, sizeof(_tmp_path),
            "%s/libprocess.XXXXXX", _pid_dirpath);
    int ret = mkstemp(_tmp_path);
    if (ret == -1)
    {
        g_logger.error("create tmp so file %s fail, error:%s",
                _tmp_path, strerror(errno));
        return false;
    }

    /* copy file to tmpfile */
    if(!copy_file(_so_path, _tmp_path))
    {
        g_logger.error("copy %s to %s so fail",
                _so_path, _tmp_path);
        return false;
    }
    return true;
}

bool bus_config_t::init_charset_table()
{
    for(std::vector<data_source_t*>::iterator it =  _source.begin();
            it != _source.end();
            ++it)
    {
        data_source_t *ds = *it;
        const char *ip = ds->get_ip();
        const int port = ds->get_port();
        if(!ds->construct_hash_table())
        {
            g_logger.error("ip=%s, port=%d,construct hash table fail", ip, port);
            return false;
        }
    }
    return true;
}

bool bus_config_t::update_source_position(uint32_t src_partnum)
{
    data_source_t *srcds = _source[src_partnum];
    mysql_position_t &srcpos = srcds->get_position();
    srcpos.reset_position();
    const char *srcip = srcds->get_ip();
    const int32_t srcport =  srcds->get_port();
    size_t tsz = _dst.size();
    for(std::size_t j = 0; j < tsz; j++)
    {
        data_source_t *dstds = _dst[j];
        if (dstds->get_online_flag() == 0)
        {
            g_logger.notice("[%s:%d] is offline", srcip, srcport);
            continue;
        }
        const char *dstip = dstds->get_ip();
        const int32_t dstport = dstds->get_port();
        if (_dst_type == REDIS_DS || _dst_type == REDIS_COUNTER_DS) {
            uint32_t index = src_partnum * _target_size + j;
            mysql_position_t *curpos = _all_pos[index];
            curpos->reset_position();
            bus_master_t master(dstip, dstport);
            if (!master.init())
            {
                g_logger.error("connect to [%s:%d] server fail",
                        dstip, dstport);
                return false;
            }
            int ret = MASTER_REDIS_FAIL;
            if (_dst_type == REDIS_DS) {
                ret = master.get_redis_position(_bss, srcds, curpos);
            } else if (_dst_type == REDIS_COUNTER_DS) {
                ret = master.get_rediscounter_position(_bss, srcds, curpos);
            }

            if(ret != MASTER_REDIS_SUCC)
            {
                g_logger.error("[%s:%d][%s:%d] get position fail",
                        srcip, srcport, dstip, dstport);
                return false;
            }
        } else if (_dst_type == HBASE_DS) {
            mysql_position_t *curpos = _all_pos[src_partnum];
            curpos->reset_position();
            char poskey[128];
            snprintf(poskey, sizeof(poskey), "%s_pos_%d", _bss, srcport);
            std::map<std::string, std::string> posmap;
            //key poskey, value会在get_hbase_positions中设置
            posmap[poskey] = poskey;
            if (!get_hbase_position(dstip, dstport, posmap)) {
                std::map<std::string, std::string>::iterator it = posmap.find(poskey);
                if (it == posmap.end()) {
                    g_logger.debug("can not find position [%s] from hbase", poskey);
                    return false;
                }
                std::string posvalue = it->second; 
                std::string filename, offset, rowoffset, cmdoffset; 
                if(!parse_position(posvalue, filename, offset, rowoffset, cmdoffset)) {
                    g_logger.error("[key=%s] [position=%s] is invalid", poskey, posvalue.c_str());
                    return false;
                }

                long pos_num, row_num, cmd_num;
                if (!str2num(offset.c_str(), pos_num) ||
                        !str2num(rowoffset.c_str(), row_num) ||
                        !str2num(cmdoffset.c_str(), cmd_num)) {
                    g_logger.error("position can't convert [%s] to int", posvalue.c_str());
                    return false;
                }
                srcds->set_position(filename.c_str(), pos_num, row_num, cmd_num, 1);
                curpos->set_position(filename.c_str(), pos_num, row_num, cmd_num);
                //成功则退出
                break;
            }
        }
    }
    if (srcpos.empty())
    {
        g_logger.error("[%s:%d] position is empty, invalid",
                srcip, srcport);
        return false;
    }
    g_logger.notice("[%d] source update pos=[%s:%ld:%ld:%ld]",
            srcport, srcpos.binlog_file, srcpos.binlog_offset, srcpos.rowindex, srcpos.cmdindex);
    return true;
}
bool bus_config_t::get_all_redis_position()
{
    clear_all_pos(_all_pos);

    std::size_t srcsz = _source.size();
    std::size_t dstsz = _dst.size();

    for (std::size_t i = 0; i < srcsz; i++)
    {
        data_source_t *srcds = _source[i];
        mysql_position_t &srcpos = srcds->get_position();
        srcpos.reset_position();
        const char *srcip = srcds->get_ip();
        const int32_t srcport =  srcds->get_port();
        for(std::size_t j = 0; j < dstsz; j++)
        {
            /* load position */
            mysql_position_t *curpos = new(std::nothrow) mysql_position_t();
            if (curpos == NULL) oom_error();
            _all_pos.push_back(curpos);

            data_source_t *dstds = _dst[j];
            if (dstds->get_online_flag() == 0)
            {
                g_logger.notice("[%s:%d] is offline", srcip, srcport);
                continue;
            }

            const char *dstip = dstds->get_ip();
            const int32_t dstport = dstds->get_port();
            bus_master_t master(dstip, dstport);
            if (!master.init())
            {
                g_logger.error("connect to [%s:%d] server fail",
                        dstip, dstport);
                return false;
            }
            int ret = MASTER_REDIS_FAIL;
            if (_dst_type == REDIS_DS) {
                ret = master.get_redis_position(_bss, srcds, curpos);
            } else if (_dst_type == REDIS_COUNTER_DS) {
                ret = master.get_rediscounter_position(_bss, srcds, curpos);
            }

            if(ret != MASTER_REDIS_SUCC)
            {
                g_logger.error("[%s:%d][%s:%d] get position fail",
                        srcip, srcport, dstip, dstport);
                return false;
            }
        }
        g_logger.debug("[%s:%d] min position:%s:%ld:%ld:%ld",
                srcip, srcport, srcpos.binlog_file, 
                srcpos.binlog_offset, srcpos.rowindex, srcpos.cmdindex);
        if (srcpos.empty())
        {
            g_logger.error("[%s:%d] position is empty, invalid",
                    srcip, srcport);
            return false;
        }
    }
    return true;
}

bool bus_config_t::get_all_hbase_position()
{
    clear_all_pos(_all_pos);

    std::size_t srcsz = _source.size();
    std::size_t dstsz = _dst.size();
    std::size_t j = 0;

    std::map<std::string, std::string> posmap;
    get_position_map(posmap);
    for(j = 0; j < dstsz; j++)
    {
        data_source_t *dstds = _dst[j];
        const char* ip = dstds->get_ip();
        const int32_t port = dstds->get_port();
        if (!get_hbase_position(ip, port, posmap)) {
            g_logger.debug("get hbase position from [%s:%d] succ", ip, port);
            continue; //去检查所有thriftserver在start trans时，是否能正常连接
        } else {
            g_logger.error("get hbase position from [%s:%d] fail", ip, port);
            return false;
        }
    }
    
    for (std::size_t i = 0; i < srcsz; i++)
    {
        char poskey[128];
        std::string strposkey;
        data_source_t *srcds = _source[i];
        mysql_position_t &srcpos = srcds->get_position();
        srcpos.reset_position();
        const char *srcip = srcds->get_ip();
        const int32_t srcport =  srcds->get_port();

        snprintf(poskey, sizeof(poskey), "%s_pos_%d", _bss, srcport);
        strposkey = poskey;
        std::map<std::string, std::string>::iterator it = posmap.find(strposkey);
        if (it == posmap.end()) {
            g_logger.debug("can not find position [%s] from hbase", poskey);
            return false;
        }
        std::string posvalue = it->second; 
        std::string filename, offset, rowoffset, cmdoffset; 
        if(!parse_position(posvalue, filename, offset, rowoffset, cmdoffset)) {
            g_logger.error("[key=%s] [position=%s] is invalid", poskey, posvalue.c_str());
            return false;
        }

        long pos_num, row_num, cmd_num;
        if (!str2num(offset.c_str(), pos_num) ||
                !str2num(rowoffset.c_str(), row_num) ||
                !str2num(cmdoffset.c_str(), cmd_num)) {
            g_logger.error("position can't convert [%s] to int", posvalue.c_str());
            return false;
        }

        mysql_position_t *curpos = new(std::nothrow) mysql_position_t();
        if (curpos == NULL) oom_error();
        _all_pos.push_back(curpos);

        curpos->set_position(filename.c_str(), pos_num, row_num, cmd_num);
        srcpos.set_position(filename.c_str(), pos_num, row_num, cmd_num);

        g_logger.debug("[%s:%d] sync position:%s:%ld:%ld:%ld",
                srcip, srcport, srcpos.binlog_file, 
                srcpos.binlog_offset, srcpos.rowindex, srcpos.cmdindex);
        if (srcpos.empty())
        {
            g_logger.error("[%s:%d] position is empty, invalid",
                    srcip, srcport);
            return false;
        }
    }
    return true;
}

bool bus_config_t::get_source_position(data_source_t *srcds)
{
    pthread_t pid = pthread_self();
    std::size_t dstsz = _dst.size();
    const char *srcip = srcds->get_ip();
    const int32_t srcport = srcds->get_port();
    mysql_position_t &srcpos = srcds->get_position();
    srcpos.reset_position();
    for(std::size_t j = 0; j < dstsz; j++)
    {
        data_source_t *dstds = _dst[j];
        const char *dstip = dstds->get_ip();
        const int32_t dstport = dstds->get_port();
        if (dstds->get_online_flag() == 0)
        {
            g_logger.notice("[%s:%d] is offline", dstip, dstport);
            continue;
        }
        if (_dst_type == REDIS_DS || _dst_type == REDIS_COUNTER_DS)
        {
            bus_master_t master(dstip, dstport);
            if (!master.init())
            {
                g_logger.error("connect to [ip=%s],[port=%d] server fail",
                        dstip, dstport);
                return false;
            }

            int ret = MASTER_REDIS_FAIL;
            if (_dst_type == REDIS_DS) {
                ret = master.get_redis_position(_bss, srcds, NULL);
            } else if (_dst_type == REDIS_COUNTER_DS) {
                ret = master.get_rediscounter_position(_bss, srcds, NULL);
            }

            if(ret != MASTER_REDIS_SUCC)
            {
                g_logger.error("[%s:%d][%s:%d] get position fail",
                        srcip, srcport, dstip, dstport);
                return false;
            }
        } else if (_dst_type == HBASE_DS) {
            char poskey[128];
            snprintf(poskey, sizeof(poskey), "%s_pos_%d", _bss, srcport);
            std::map<std::string, std::string> posmap;
            //key poskey, value会在get_hbase_positions中设置
            posmap[poskey] = poskey;
            if (!get_hbase_position(dstip, dstport, posmap)) {
                std::map<std::string, std::string>::iterator it = posmap.find(poskey);
                if (it == posmap.end()) {
                    g_logger.debug("can not find position [%s] from hbase", poskey);
                    return false;
                }
                std::string posvalue = it->second; 
                std::string filename, offset, rowoffset, cmdoffset; 
                if(!parse_position(posvalue, filename, offset, rowoffset, cmdoffset)) {
                    g_logger.error("[key=%s] [position=%s] is invalid", poskey, posvalue.c_str());
                    return false;
                }

                long pos_num, row_num, cmd_num;
                if (!str2num(offset.c_str(), pos_num) ||
                        !str2num(rowoffset.c_str(), row_num) ||
                        !str2num(cmdoffset.c_str(), cmd_num)) {
                    g_logger.error("position can't convert [%s] to int", posvalue.c_str());
                    return false;
                }
                srcds->set_position(filename.c_str(), pos_num, row_num, cmd_num, 1);
                break;
            }
        }
    }
    if (srcds->get_position().empty())
    {
        g_logger.error("[%s:%d][%x] position is empty",
                srcip, srcport, pid);
        return false;
    }
    return true;
}

// only used after get_all_source_position()
bool bus_config_t::get_position_map(std::map<std::string, std::string> &posmap)
{
    char key[128];
    char value[128];
    for (std::vector<data_source_t*>::iterator it = _source.begin();
           it != _source.end();
           ++it)
    {
       data_source_t *src = *it;
       //const char *src_ip = src->get_ip();
       const int src_port = src->get_port();
       mysql_position_t &srcpos = src->get_position();
       snprintf(key, sizeof(key), "%s_pos_%d", _bss, src_port);
       snprintf(value, sizeof(value), "%s:%ld:%ld:%ld",
               srcpos.binlog_file, srcpos.binlog_offset, srcpos.rowindex, srcpos.cmdindex);
       //std::string keystring = key;
       //std::string valuestring = value;
       posmap[key] = value;
    }

    return true;
}

std::vector<data_source_t*>& bus_config_t::get_source()
{
    return _source;
}

std::vector<data_source_t*>& bus_config_t::get_dst()
{
    return _dst;
}

std::vector<schema_t*>& bus_config_t::get_src_schema()
{
    return _source_schema;
}

std::vector<schema_t*>& bus_config_t::get_dst_schema()
{
    return _dst_schema;
}

}//namespace bus

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
