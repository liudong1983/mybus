/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_wrap.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/29 11:14:27
 * @version $Revision$
 * @brief 
 *  
 **/

#include "bus_wrap.h"
#include "mysql_full_extract.h"
#include "mysql_incr_extract.h"
#include "redis_increment_sender.h"
#include "redis_full_sender.h"
#include "hbase_full_sender.h"
#include "hbase_incr_sender.h"
#include "bus_fake.h"

namespace bus {
bool bus_t::reload_so(int &errorcode)
{
    if (_trans_stat != INIT)
    {
        errorcode = RELOAD_NOT_INIT;
        g_logger.error("reload fail, stat is not init");
        return false;
    }
    if (!_load_transform())
    {
        g_logger.error("reload load so fail");
        errorcode = RELOAD_FAIL;
        return false;
    }
    return true;
}
void bus_t::prepare_exit()
{
    /* 删除pidfile */
    char *pidfile = _config.get_pidfile();
    g_logger.notice("prepare exit, remove pidfile:%s", pidfile);
    unlink(pidfile);
    /* 删除so文件 */
    char *tmp_so_file = _config.get_tmp_so_path();
    unlink(tmp_so_file);
    /* server关闭 */
    _server.stop_server();
}
bool bus_t::shutdown(int &errorcode) 
{
    if (_trans_stat != INIT)
    {
        errorcode = SHUTDOWN_NOT_INIT;
        g_logger.error("shutdown fail, stat is not init");
        return false;
    }
    prepare_exit();
    _trans_stat = SHUTDOWN_WAIT;
    return true;
}
void bus_t::info(char *buf, size_t len)
{
    //获取统计信息
    uint64_t p_succ_count, p_fail_count;
    _stat.get_producer_count(p_succ_count, p_fail_count);

    uint64_t s_succ_count, s_fail_count;
    _stat.get_consumer_count(s_succ_count, s_fail_count);

    //获取状态信息
    transfer_state_t cur_state = _trans_stat;
    if (cur_state != INIT && !_stat.get_thread_state()) {
        cur_state = TRAN_FAIL;
    } else if (cur_state != INIT) {
        g_logger.reset_error_info();
    }
    uint32_t tasksz = _engine.get_task_size();
    snprintf(buf, len, 
            "bus_version:%s\r\n"
            "bus_release:%s\r\n"
            "so_version:%s\r\n"
            "bus_status:%s\r\n"
            "extract_succ_count:%lu\r\n"
            "so_ignore_count:%lu\r\n"
            "send_succ_count:%lu\r\n"
            "task_size:%u\r\n"
            "client_num:%lu\r\n"
            "error1:%s\r\n"
            "error2:%s\r\n"
            "error3:%s\r\n"
            "error4:%s\r\n"
            "error5:%s\r\n",
            VERSION,
            RELEASE,
            _so_version,
            transfer_state_msg[cur_state],
            p_succ_count,
            p_fail_count,
            s_succ_count,
            tasksz,
            bus_client_t::client_ct,
            g_logger.error_info1,
            g_logger.error_info2,
            g_logger.error_info3,
            g_logger.error_info4,
            g_logger.error_info5);
}
bool bus_t::_load_transform()
{
    if (_handle != NULL)
    {
        dlclose(_handle);
        _handle = NULL;
    }
    if (!_config.init_tmp_so())
    {
        g_logger.error("init tmp so fail");
        return false;
    }
    char *so_path = _config.get_tmp_so_path();
    _handle = dlopen(so_path, RTLD_NOW);
    if (_handle == NULL)
    {
        g_logger.error("open so=%s fail, error:%s", so_path, dlerror());
        return false;
    }

    _func = (transform) dlsym(_handle, "process");
    if (_func == NULL)
    {
        g_logger.error("load process fail, error:%s", dlerror());
        return false;
    }
    _init_func = (initso) dlsym(_handle, "init");
    if (_init_func == NULL)
    {
        g_logger.error("load init fail, error:%s", dlerror());
        return false;
    }

    _init_func(&g_logger, _so_version, sizeof(_so_version));
    return true;
}
bool bus_t::init(const int argc, char* const*argv)
{
    //解析命令行参数
    if (!_config.parse(argc, argv))
    {
        g_logger.error("parse command line argument fail");
        return false;
    }
    //加载so
    if (!_load_transform())
    {
        g_logger.error("load transform library fail");
        return false;
    }

    //初始化mysql library
    if (!bus_mysql_t::init_library())
    {
        g_logger.error("init mysql library fail");
        return false;
    }
    
    _trans_stat = INIT;
    return true;
}

bool bus_t::start_trans(int &errorcode)
{
    if (_trans_stat != INIT)
    {
        g_logger.error("transfer state is not INIT ok");
        errorcode = INIT_NOT_OK;
        return false;
    }

    g_logger.notice("\n\n----------databus start trans---------------");
    //获取任务分配信息
    if (!_config.get_master_info())
    {
        g_logger.error("get master info fail");
        errorcode = GET_META_INFO_FAIL;
        return false;
    }
    //创建生产者，消费者线程池
    if (!_init_pool())
    {
        g_logger.error("init thread pool fail");
        errorcode = INIT_THREAD_POOL_FAIL;
        return false;
    }
    //拆分任务
    if (!_split())
    {
        g_logger.error("split job fail");
        errorcode = SPLIT_JOB_FAIL;
        return false;
    }
    //清理统计信息
    _stat.clear();
    //启动引擎
    if (!_engine.start())
    {
        g_logger.error("start engine fail");
        errorcode = START_ENGINE_FAIL;
        _engine.destroy(); //bug resolved
        return false;
    }
    //设置状态
    transfer_type_t transfer_type = _config.get_transfer_type();
    if (transfer_type == FULL)
    {
        g_bus.set_transfer_stat(FULL_TRAN_BEGIN);
    } else if (transfer_type == INCREMENT) {
        g_bus.set_transfer_stat(INCR_TRAN_BEGIN);
    }
    return true;
}

void bus_t::stop_trans_and_exit()
{
    if (_trans_stat == INCR_TRAN_BEGIN)
    {
        _engine.stop();
        _trans_stat = STOP_AND_EXIT;
    }

    if (_trans_stat == INCR_TRAN_STOP_WAIT)
    {
        _trans_stat = STOP_AND_EXIT;
    }

    if (_trans_stat == INIT ||
            _trans_stat == FULL_TRAN_BEGIN ||
            _trans_stat == FULL_TRAN_WAIT_PRODUCER_EXIT ||
            _trans_stat == FULL_TRAN_WAIT_CONSUMOR_EXIT ||
            _trans_stat == FULL_TRAN_STOP_WAIT)
    {
        _trans_stat = SHUTDOWN_WAIT;
    }
    _stat.clear_thread_state();
    prepare_exit();
}

bool bus_t::stop_trans(int &errorcode)
{
    if (_trans_stat == INCR_TRAN_STOP_WAIT ||
            _trans_stat == FULL_TRAN_STOP_WAIT ||
            _trans_stat == INIT)
    {
        return true;
    }

    if (_trans_stat != INCR_TRAN_BEGIN &&
            _trans_stat != FULL_TRAN_BEGIN &&
            _trans_stat != FULL_TRAN_WAIT_PRODUCER_EXIT &&
            _trans_stat != FULL_TRAN_WAIT_CONSUMOR_EXIT &&
            _trans_stat != TRAN_FAIL)
    {
        errorcode = ENGINE_NOT_RUNNING;
        g_logger.error("stop trans fail, engine is not running");
        return false;
    }
    if (!is_init_ok())
    {
        errorcode = ENGINE_INIT;
        g_logger.error("stop trans fail, engine is initting");
        return false;
    }

    _engine.stop();
    /* 修改同步状态 */
    if (_trans_stat == INCR_TRAN_BEGIN)
    {
        _trans_stat = INCR_TRAN_STOP_WAIT;
    }else if(_trans_stat == FULL_TRAN_BEGIN ||
            /*修复 如果_trans_stat == FULL_TRAN_WAIT_PROUCER_EXIT && thread有fail的情况下
             * cron_func()中 get_thread_stat()不能检查通过，导致无法shutdown*/
            _trans_stat == FULL_TRAN_WAIT_PRODUCER_EXIT){
        _trans_stat = FULL_TRAN_STOP_WAIT;
    }
    return true;
}

bool bus_t::_init_redis_consumer_pool()
{
    std::vector<data_source_t*> _src = _config.get_source();
    std::vector<data_source_t*> _dst = _config.get_dst();
    std::size_t src_sz = _src.size();
    std::size_t dst_sz = _dst.size();
    transfer_type_t transfer_type = _config.get_transfer_type();

    for (std::size_t i = 0; i < src_sz; i++)
    {
        for (std::size_t j = 0; j < dst_sz; j++)
        {
            data_source_t *cur_dst_source = _dst[j];
            int online_flag = cur_dst_source->get_online_flag();
            const char *ip = cur_dst_source->get_ip();
            const int port = cur_dst_source->get_port();
            thread_pool_t *spool = NULL;
            if (transfer_type == FULL)
            {
                if (online_flag == 0) {
                    spool = new(std::nothrow)fake_pool_t(1, CONSUMER_QUEUE_SIZE, ip, port, i);
                } else {
                    spool = new(std::nothrow) redis_full_pool_t(1, CONSUMER_QUEUE_SIZE, ip, port, i);
                }
            }else if (transfer_type == INCREMENT)
            {
                if (online_flag == 0)
                {
                    spool = new(std::nothrow)fake_pool_t(1, CONSUMER_QUEUE_SIZE, ip, port, i);
                } else {
                    spool = new(std::nothrow)redis_increment_pool_t(1, CONSUMER_QUEUE_SIZE, ip, port, i);
                }
            }else {
                g_logger.error("unknown transfer type");
                return false;
            }
            if (spool == NULL) oom_error();
            _engine.add_consumor_pool(spool);
        }
    }
    return true;
}

bool bus_t::_init_hbase_consumer_pool()
{
    std::vector<data_source_t*> _src = _config.get_source();
    std::vector<data_source_t*> _dst = _config.get_dst();
    std::size_t src_sz = _src.size();
    std::size_t dst_sz = _dst.size();
    transfer_type_t transfer_type = _config.get_transfer_type();
    uint64_t thread_num = _config.get_hbase_full_thread_num();
    if (thread_num >= 10)
    {
        g_logger.error("hbase full sender thread num %d >= 10, too many", thread_num);
        return false;
    }

    for (std::size_t i = 0; i < src_sz; i++)
    {
        //对于hbase，一个上游对应一个sender，sender将数据发送到
        //thriftserver，将thriftserver按照取余方式分配给sender。
        std::size_t j = i%dst_sz;
        data_source_t *cur_dst_source = _dst[j];
        int online_flag = cur_dst_source->get_online_flag();
        const char *ip = cur_dst_source->get_ip();
        const int port = cur_dst_source->get_port();
        thread_pool_t *spool = NULL;
        if (transfer_type == FULL)
        {
            if (online_flag == 0) {
                spool = new(std::nothrow)fake_pool_t(1, CONSUMER_QUEUE_SIZE, ip, port, i);
            } else {
                //默认hbase的一个全量sender包含1个thread,  
                //如果使用多线程发送，请注意以下两点：
                //  1. full trans时，put/delete到hbase同一行时，多线程sender无法保证put和delete
                //  的顺序性，有可能先put或者先delete,如果有此种需求，请使用单线程模式
                //  2. 建议full trans时，尽量只用put 和 incr，不要put、delete交叉太多
                g_logger.debug("hbase full pool thread num is %d",  thread_num);
                spool = new(std::nothrow) hbase_full_pool_t(thread_num, CONSUMER_QUEUE_SIZE, ip, port, i, j);
            }
        }else if (transfer_type == INCREMENT)
        {
            if (online_flag == 0)
            {
                spool = new(std::nothrow)fake_pool_t(1, CONSUMER_QUEUE_SIZE, ip, port, i);
            } else {
                //hbase的一个增量sender只能包含1个thread
                spool = new(std::nothrow)hbase_incr_pool_t(1, CONSUMER_QUEUE_SIZE, ip, port, i, j);
            }
        }else {
            g_logger.error("unknown transfer type");
            return false;
        }
        if (spool == NULL) oom_error();
        _engine.add_consumor_pool(spool);
    }

    //用于初始化thrift_record
    _config.init_thrift_record();

    return true;
}

bool bus_t::_init_pool()
{
    std::vector<data_source_t*> _src = _config.get_source();
    std::vector<data_source_t*> _dst = _config.get_dst();

    std::size_t src_sz = _src.size();
    std::size_t dst_sz = _dst.size();
    //初始化engine参数
    _engine.set_dst_source_sz(dst_sz);

    source_t srctype = _config.get_srcds_type();
    source_t dsttype = _config.get_dstds_type();

    transfer_type_t transfer_type = _config.get_transfer_type();
    /* 创建生产者线程池 */
    thread_pool_t *ppool = NULL;
    if (transfer_type == FULL)
    {
        if (srctype == REDIS_DS) {
            //TODO
        } else if (srctype == MYSQL_DS) {
            ppool = new(std::nothrow) mysql_full_extract_t(src_sz, PRODUCER_QUEUE_SIZE);
        }
    } else if (transfer_type == INCREMENT) {
        if (srctype == MYSQL_DS)
        {
            ppool = new(std::nothrow) mysql_incr_extract_t(src_sz, PRODUCER_QUEUE_SIZE);
        }
    } else {
        g_logger.error("unknown transfer type");
        return false;
    }

    if (ppool == NULL) oom_error();
    _engine.set_producer_pool(ppool);

    bool rtn = false;
    /* 创建消费者线程池 */
    switch (dsttype)
    {
        case REDIS_DS:
        case REDIS_COUNTER_DS:
            rtn = _init_redis_consumer_pool();
            break;
        case HBASE_DS:
            rtn = _init_hbase_consumer_pool();
            break;
        default:
            g_logger.error("invalid dst type %d", dsttype);
            break;
    }

    return rtn;
}
bool bus_t::get_source_info(std::string &info)
{
    std::vector<data_source_t*> &up_ds = _config.get_source();
    std::vector<data_source_t*> &down_ds = _config.get_dst();
    if (_trans_stat == INIT && !_config.get_master_source())
    {
        g_logger.error("get master source fail");
        return false;
    }
    if (_trans_stat == INIT && !_config.get_master_dst())
    {
        g_logger.error("get master dst fail");
        return false;
    }
    if (_trans_stat == INIT && !_config.get_master_transfer_type())
    {
        g_logger.error("get master transfer type fail");
        return false;
    }
    transfer_type_t tran_type = _config.get_transfer_type();

    std::size_t sz = up_ds.size();
    char buffer[128];
    for (uint32_t i = 0; i < sz; ++i)
    {
        data_source_t *ds = up_ds[i];
        const char *ip = ds->get_ip();
        const int port = ds->get_port();
        snprintf(buffer, sizeof(buffer), "up:%s:%d\r\n", ip, port);
        info.append(buffer);
    }

    sz = down_ds.size();
    for (uint32_t i = 0; i < sz; ++i)
    {
        data_source_t *ds = down_ds[i];
        const char *ip = ds->get_ip();
        const int port = ds->get_port();
        snprintf(buffer, sizeof(buffer), "down:%s:%d\r\n", ip, port);
        info.append(buffer);
    }

    if (tran_type == FULL)
    {
        info.append("tran_type:full\r\n");
    } else if (tran_type == INCREMENT) {
        info.append("tran_type:incr\r\n");
    } else if (tran_type == TRANSFER_EMPTY) {
        info.append("tran_type:empty\r\n");
    } else {
        info.append("tran_type:unknown\r\n");
    }
    return true;
}
bool bus_t::get_match_info(std::string &info)
{
    source_t srctype = _config.get_srcds_type();
    const char *user_name = _config.get_user_name();
    const char *user_pwd = _config.get_user_pwd();
    const char *charset = "utf8";

    if (srctype == SOURCE_EMPTY && !_config.get_source_type())
    {
        g_logger.error("get source type fail");
        return false;
    }

    //在databus没有start trans时，去config server拉取最新schema信息
    //一旦databus start trans了，则不能去拉取最新，只能获取当前的
    if (_trans_stat == INIT)
    {
        if (!_config.get_source_schema())
        {
            g_logger.error("get source schema fail");
            return false;
        }

        if (!_config.init_schema_regex())
        {
            g_logger.error("init schema regex fail");
            return false;
        }
    }

    srctype = _config.get_srcds_type();
    if (srctype != MYSQL_DS)
    {
        return true;
    }

    std::vector<data_source_t*> &source_ds = _config.get_source();
    if (_trans_stat == INIT && !_config.get_master_source())
    {
        g_logger.error("get master source fail");
        return false;
    }

    if (user_name[0] == '\0' && !_config.get_user_info())
    {
        g_logger.error("get username/password fail");
        return false;
    }
    user_name = _config.get_user_name();
    user_pwd = _config.get_user_pwd();

    std::vector<schema_t*> &source_schema = _config.get_src_schema();
    std::vector<db_object_id> idvec;
    idvec.reserve(10240);
    char buff[1024];
    for (std::vector<data_source_t*>::iterator it = source_ds.begin();
            it != source_ds.end();
            ++it)
    {
        data_source_t *cursource = *it;
        const char *curip = cursource->get_ip();
        const int32_t curport = cursource->get_port();
        if(srctype == MYSQL_DS)
        {
            bus_mysql_t source_conn(curip, curport, user_name, user_pwd);
            if (!source_conn.init(charset))
            {
                g_logger.error("connect to mysql %s:%d  fail", curip, curport);
                return false;
            }

            for (std::vector<schema_t*>::iterator it = source_schema.begin();
                    it != source_schema.end();
                    ++it)
            {
                schema_t *cur_src_schema = *it;
                const char *src_schema_db = cur_src_schema->get_schema_db();
                const char *src_schema_name = cur_src_schema->get_schema_name();

                idvec.clear();
                if (!source_conn.get_match_all_table(cur_src_schema, idvec))
                {
                    g_logger.error("[%s:%d][%s:%s] get match all table fail", curip,
                            curport, src_schema_db, src_schema_name);
                    return false;
                }

                std::size_t sz = idvec.size();
                if (!sz) continue;
                for (uint32_t i = 0; i < sz; ++i)
                {
                    db_object_id &id = idvec[i];
                    snprintf(buff, sizeof(buff), "%d:%s.%s;", 
                            curport, id.schema_name.c_str(), id.table_name.c_str());
                    info.append(buff);
                }
                info.append("\r\n");
            }//for schema
        }
    }//for datasource
    return true;
}
bool bus_t::_split()
{
    source_t srctype = _config.get_srcds_type();
    const char *user_name = _config.get_user_name();
    const char *user_pwd = _config.get_user_pwd();
    const char *charset = _config.get_charset();
    transfer_type_t transfer_type = _config.get_transfer_type();

    std::vector<data_source_t*> source_ds = _config.get_source();
    std::vector<schema_t*> source_schema = _config.get_src_schema();
    uint32_t  src_ds_index = 0;
    std::vector<db_object_id> idvec;
    idvec.reserve(10240);
    for (std::vector<data_source_t*>::iterator it = source_ds.begin();
            it != source_ds.end();
            ++it)
    {
        data_source_t *cursource = *it;
        const char *curip = cursource->get_ip();
        const int32_t curport = cursource->get_port();
        if(srctype == MYSQL_DS && transfer_type == FULL)
        {
            bus_mysql_t source_conn(curip, curport, user_name, user_pwd);
            if (!source_conn.init(charset))
            {
                g_logger.error("connect to mysql %s:%d  fail", curip, curport);
                return false;
            }

            for (std::vector<schema_t*>::iterator it = source_schema.begin();
                    it != source_schema.end();
                    ++it)
            {
                schema_t *cur_src_schema = *it;
                const char *src_schema_db = cur_src_schema->get_schema_db();
                const char *src_schema_name = cur_src_schema->get_schema_name();

                idvec.clear();
                if (!source_conn.get_match_all_table(cur_src_schema, idvec))
                {
                    g_logger.error("[%s:%d][%s:%s] get match all table fail", curip,
                            curport, src_schema_db, src_schema_name);
                    return false;
                }

                std::size_t tbsz = idvec.size();
                for (std::size_t i = 0; i < tbsz; ++i)
                {
                    const char *cur_schema_db = idvec[i].schema_name.c_str();
                    const char *cur_schema_name = idvec[i].table_name.c_str();
                    bus_job_t *job = new(std::nothrow) bus_job_t(cursource, transfer_type,
                            cur_src_schema, cur_schema_db, cur_schema_name, src_ds_index);
                    if (job == NULL) oom_error();
                    g_logger.debug("split add job [%s:%d] [%s:%s:%s]",
                            curip, curport, 
                            cur_src_schema->get_schema_name(), cur_schema_db, cur_schema_name);
                    _engine.add_job(job);
                }
            }//for schema
        } else { 
                bus_job_t *job = new(std::nothrow) bus_job_t(cursource, transfer_type,
                        NULL, NULL, NULL, src_ds_index);
                if (job == NULL) oom_error();
                g_logger.debug("split add job [%s:%d] [NULL:NULL:NULL]", curip, curport);
                _engine.add_job(job);
        }
        ++src_ds_index;
    }//for datasource

    /* 添加结束哨兵 */
    if (transfer_type == FULL)
    {
        _engine.add_job(NULL);
    }
    return true;
}

void bus_t::cron_func()
{
    if (_trans_stat == FULL_TRAN_WAIT_PRODUCER_EXIT)
    {
        if (_engine.get_reader_stat() && _stat.get_thread_state())
        {
            _engine.add_end_row();
            _trans_stat = FULL_TRAN_WAIT_CONSUMOR_EXIT;
        }
    }

    if (_trans_stat == FULL_TRAN_WAIT_CONSUMOR_EXIT)
    {
        if (_engine.get_sender_stat())
        {
            _engine.destroy();
            _trans_stat = INIT;
            g_logger.reset_error_info();
        }
    }

    if (_trans_stat == INCR_TRAN_STOP_WAIT)
    {
        if (_engine.get_reader_stat() && _engine.get_sender_stat())
        {
            _engine.destroy();
            _trans_stat = INIT;
            g_logger.reset_error_info();
        }
    }

    if (_trans_stat == FULL_TRAN_STOP_WAIT)
    {
        if (_engine.get_reader_stat() && _engine.get_sender_stat())
        {
            _engine.destroy();
            _trans_stat = INIT;
            g_logger.reset_error_info();
        }
    }

    if (_trans_stat == STOP_AND_EXIT)
    {
        if (_engine.get_reader_stat() && _engine.get_sender_stat())
        {
            exit(0);
        }
    }

    if (_trans_stat == SHUTDOWN_WAIT)
    {
        //exit(0);
    }
}

}//namespace bus


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
