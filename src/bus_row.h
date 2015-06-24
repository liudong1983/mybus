/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_row.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/08 16:44:35
 * @version $Revision$
 * @brief 
 *  
 **/

#ifndef  __BUS_ROW_H_
#define  __BUS_ROW_H_
#include <assert.h>
#include <vector>
#include <map>
#include <string>
#include <stdexcept>
#include <exception>
#include <exception>
#include "bus_util.h"
#include "bus_position.h"
//#include "hbase_row.h"

namespace bus {
class schema_t;

#define HBASE_CMD_PUT  0
#define HBASE_CMD_GET  1
#define HBASE_CMD_DEL  2
#define HBASE_CMD_INCR 3 

#define HBASE_DEFAULT_TIME (-1)
#define HBASE_DEFAULT_DELETETYPE (-1)
#define USE_OLD_VALUE 0x01

class row_t;
class tobject_t;
//MAP2VECTOR for hbase_full/incr_sender
typedef std::map<std::string, std::vector<row_t*> > MAP2VECTOR;
extern std::vector<tobject_t*> NULL_VEC;

void dump_string(const char *p, char** dst);

//hbase.thrift2 (TColumn|TColumnValue|TColumnIncrement)
class tobject_t
{
    public:
        //TColumnValue
        tobject_t(char *cf, char *qu, char *value, uint64_t timestamp, uint64_t flag):
            _flag(flag), _timestamp(timestamp) {
                if (cf != NULL && qu != NULL && value != NULL) {
                    snprintf(_cf, sizeof(_cf), "%s", cf);
                    snprintf(_qu, sizeof(_qu), "%s", qu);
                    if (flag & USE_OLD_VALUE) {
                        _value = value;
                    } else {
                        dump_string(value, &_value);
                    }
                } else {
                    _cf[0] = '\0';
                    _qu[0] = '\0';
                    _value = NULL;
                }
            }
        //TColumn
        tobject_t(char *cf, char*qu, uint64_t timestamp):
             _value(NULL), _timestamp(timestamp) {
                if (cf != NULL) {
                    snprintf(_cf, sizeof(_cf), "%s", cf);
                    if (qu != NULL) {
                        snprintf(_qu, sizeof(_qu), "%s", qu);
                    } else  {
                        _qu[0] = '\0';
                    }
                } else {
                    _cf[0] = '\0';
                    _qu[0] = '\0';
                }
            }
        //TColumnIncrement
        tobject_t(char *cf, char*qu, int64_t amount):
            _value(NULL), _amount(amount) {
                if (cf != NULL && qu != NULL) {
                    snprintf(_cf, sizeof(_cf), "%s", cf);
                    snprintf(_qu, sizeof(_qu), "%s", qu);
                } else {
                    _cf[0] = '\0';
                    _qu[0] = '\0';
                }
            }

        ~tobject_t() {
            if (_value != NULL && (!(_flag & USE_OLD_VALUE))) {
                delete []_value;
                _value = NULL;
            }
        }

        const char* get_cf() const {
            return _cf;
        }
        
        const char* get_qu() const {
            return _qu;
        }

        char* get_value() const {
            return _value;
        }

        uint64_t get_timestamp() const {
            return _timestamp;
        }

        int64_t get_amount() const {
            return _amount;
        }

    private:
        char _cf[64];
        char _qu[64];
        char *_value;
        uint64_t _flag;
        uint64_t _timestamp;
        int64_t _amount;
};

class cmd_t 
{
    public:
        //for hbase _dstnum = 0;
        cmd_t():_dstnum(0) {}
        explicit cmd_t(long dstnum):_dstnum(dstnum) {}

        long get_cmd_dst() {
            return _dstnum;
        }
        void set_cmd_dst(long dstnum) {
            _dstnum = dstnum;
        }

        virtual void print(std::string &) const = 0;
        
        virtual char* get_redis_cmd() const { return NULL;}
        virtual char* get_redis_key() const { return NULL;}
        virtual char* get_redis_field() const { return NULL;}
        virtual char* get_redis_value() const { return NULL;}

        virtual uint64_t get_hbase_cmd() const { return -1;}
        virtual const char* get_hbase_table() const { return NULL;}
        virtual const char* get_hbase_row() const { return NULL;}
        virtual int64_t get_hbase_time() const { return 0;}
        virtual void set_hbase_time(uint64_t time) {
            g_logger.warn("time:%d", time); //avoid build wraning
        }
        //TODO:  如何不使用NULL_VEC_X返回空的vecotor，或者
        virtual const std::vector<tobject_t*>& get_hbase_vector() const {
            return NULL_VEC;
        }

        virtual ~cmd_t() = 0;
    private:
        long _dstnum;
        DISALLOW_COPY_AND_ASSIGN(cmd_t);
};

class hbase_cmd_t:public cmd_t 
{
    public:
        //PUT
        hbase_cmd_t(uint64_t cmd, char* table, char* row, uint64_t time);
        //DELETE INCREMENT
        hbase_cmd_t(uint64_t cmd, char* table, char* row);
        ~hbase_cmd_t();

        uint64_t get_hbase_cmd() const
        {
            return _cmd;
        }

        const char *get_hbase_table() const
        {
            return _table;
        }

        const char *get_hbase_row() const
        {
            return _row;
        }

        int64_t get_hbase_time() const
        {
            return _time;
        }

         //full trans add_row_copy(,)
        // may call this to set hbase timestamp
        void set_hbase_time(uint64_t time)
        {
            if (HBASE_DEFAULT_TIME == _time
                    && HBASE_CMD_PUT == _cmd) {
                _time = static_cast<int64_t>(time);
            }
        }

        // new tobject_t, then _tobject_vector.push_back(tobject_t*)
        void add_columnvalue(char* cf, char* qu, char* value, uint64_t time, uint64_t flag); //put
        void add_column(char* cf, char* qu, uint64_t time); //delete
        void add_increment(char* cf, char* qu, int64_t amount); //increment
            
        const std::vector<tobject_t*>& get_hbase_vector() const {
            return _tobject_vector;
        }

        void print(std::string &info) const;
    private:
        std::vector<tobject_t*> _tobject_vector;
        uint64_t _cmd;
        char _table[64];
        char _row[64];
        int64_t _time;
};

class redis_cmd_t:public cmd_t 
{
    public:
        redis_cmd_t():cmd_t(0),
            _cmd(NULL),
            _key(NULL),
            _field(NULL),
            _value(NULL){ 
        };
        redis_cmd_t(long dst, char* cmd, char* key,
                char* field, char* value);
        ~redis_cmd_t();

        char* get_redis_cmd() const
        {
            return _cmd;
        }

        char *get_redis_key() const
        {
            return _key;
        }

        char *get_redis_field() const
        {
            return _field;
        }

        char *get_redis_value() const
        {
            return _value;
        }

        void print(std::string &info) const;
    private:
        char* _cmd;
        char* _key;
        char* _field;
        char* _value;
};

class row_t
{
    public:
        row_t(){};
        explicit row_t(uint32_t sz)
        {
            _cols.reserve(sz);
            _oldcols.reserve(sz);
            _cmd = NULL;
        }
        row_t(const row_t& other);
        void dup_cmd(row_t* parent, long index);
        void dup_cmd_and_position(row_t* parent, long index);
        void push_back(const char *p, bool is_old);
        void push_back(const char *p, int sz, bool is_old);
        //for redis/redis_counter
        void push_cmd(long, char*, char*, char*, char*);

        //for hbase 
        //1. new hbase_cmd_t
        //2. push_back hbase_cmd_t to std::vector<cmd_t*> _zcols
        //3. return hbase_cmd_t
        hbase_cmd_t* get_hbase_put(char* table, char* row, uint64_t time);
        hbase_cmd_t* get_hbase_delete(char* table, char* row);
        hbase_cmd_t* get_hbase_increment(char* table, char* row);
        
        char* get_redis_cmd() const
        {
            return _cmd->get_redis_cmd(); 
        }

        char *get_redis_key() const
        {
            return _cmd->get_redis_key(); 
        }

        char *get_redis_field() const
        {
            return _cmd->get_redis_field(); 
        }

        char *get_redis_value() const
        {
            return _cmd->get_redis_value(); 
        }

        void set_hbase_time(uint64_t time)
        {
            return _cmd->set_hbase_time(time); 
        }
        bool get_value(uint32_t index, char **val_ptr)
        {
            uint32_t sz = _cols.size();
            if (index >= sz) {
                std::string info;
                this->print(info);
                g_logger.error("get value fail, index=%u,size=%u, data=%s",
                        index, sz, info.c_str());
                return false;
            }
            *val_ptr = _cols[index];
            return true;
        }

        bool get_value_byindex(uint32_t index, char **val_ptr)
        {
            uint32_t sz = _cols.size();
            if (index >= sz) {
                return false;
            }
            *val_ptr = _cols[index];
            return true;
        }

        bool get_value_byindex(uint32_t index, char **val_ptr, bus_log_t *solog)
        {
            uint32_t sz = _cols.size();
            if (index >= sz) {
                std::string info;
                this->print(info);
                solog->error("get value fail, index=%u,size=%u, data=%s",
                        index, sz, info.c_str());
                return false;
            }
            *val_ptr = _cols[index];
            return true;
        }

        bool get_value_byname(char* name, char **val_ptr)
        {
            assert(NULL != _pnamemap);
            std::map<std::string, int>::iterator it = (*_pnamemap).find(name);
            if (it == (*_pnamemap).end()) {
                return false;
            }

            uint32_t index = it->second;
            uint32_t sz = _cols.size();
            if (index >= sz) {
                return false;
            }
            *val_ptr = _cols[index];
            return true;
        }
        
        bool get_value_byname(char* name, char **val_ptr, bus_log_t *solog)
        {
            //*solog for _src_schema->get_column_index() need #include"bus_config.h"
            //then bad to bus_process.cc
            assert(NULL != _pnamemap);
            std::map<std::string, int>::iterator it = (*_pnamemap).find(name);
            if (it == (*_pnamemap).end()) {
                solog->error("namemap can not find %s", name);
                return false;
            }

            uint32_t index = it->second;
            uint32_t sz = _cols.size();
            if (index >= sz) {
                std::string info;
                this->print(info);
                solog->error("get value fail, index=%u,size=%u, data=%s",
                        index, sz, info.c_str());
                return false;
            }
            *val_ptr = _cols[index];
            return true;
        }
        
        bool get_old_value(uint32_t index, char **val_ptr)
        {
            uint32_t sz = _oldcols.size();
            if (index >= sz) {
                return false;
            }
            *val_ptr = _oldcols[index];
            return true;
        }

        bool get_old_value_byindex(uint32_t index, char **val_ptr)
        {
            uint32_t sz = _oldcols.size();
            if (index >= sz) {
                return false;
            }
            *val_ptr = _oldcols[index];
            return true;
        }

        bool get_old_value_byindex(uint32_t index, char **val_ptr, bus_log_t *solog)
        {
            uint32_t sz = _oldcols.size();
            if (index >= sz) {
                std::string info;
                this->print(info);
                solog->error("get old value fail, index=%u,size=%u, data=%s",
                        index, sz, info.c_str());
                return false;
            }
            *val_ptr = _oldcols[index];
            return true;
        }

        bool get_old_value_byname(char* name, char **val_ptr)
        {
            assert(NULL != _pnamemap);
            std::map<std::string, int>::iterator it = (*_pnamemap).find(name);
            if (it == (*_pnamemap).end()) {
                return false;
            }

            uint32_t index = it->second;
            uint32_t sz = _oldcols.size();
            if (index >= sz) {
                return false;
            }
            *val_ptr = _oldcols[index];
            return true;
        }

        bool get_old_value_byname(char* name, char **val_ptr, bus_log_t *solog)
        {
            assert(NULL != _pnamemap);
            std::map<std::string, int>::iterator it = (*_pnamemap).find(name);
            if (it == (*_pnamemap).end()) {
                solog->error("namemap can not find %s", name);
                return false;
            }

            uint32_t index = it->second;
            uint32_t sz = _oldcols.size();
            if (index >= sz) {
                std::string info;
                this->print(info);
                solog->error("get old value fail, index=%u,size=%u, data=%s",
                        index, sz, info.c_str());
                return false;
            }
            *val_ptr = _oldcols[index];
            return true;
        }


        std::vector<cmd_t*>& get_rcols()
        {
            return _zcols;
        }

        bool set_position(const char *filename, long offset, long row_index, long cmd_index)
        {
            return _rowpos.set_position(filename, offset, row_index, cmd_index);
        }

        mysql_position_t& get_position()
        {
            return _rowpos;
        }

        void set_db(const char *dbname)
        {
            snprintf(_dbname, sizeof(_dbname), "%s", dbname);
        }

        const char *get_db_name()
        {
            return _dbname;
        }

        void set_table(const char *tablename)
        {
            snprintf(_tablename, sizeof(_tablename), "%s", tablename);
        }

        const char* get_table() const
        {
            return _tablename;
        }

        void set_action(action_t action)
        {
            _action = action;
        }

        action_t get_action() const
        {
            return _action;
        }

        uint32_t size() const
        {
            return _cols.size();
        }

        long get_dst_partnum()
        {
            assert(NULL != _cmd);
            return _cmd->get_cmd_dst();
        }

        void set_dst_partnum(long dst)
        {
            assert(NULL != _cmd);
            _cmd->set_cmd_dst(dst);
        }

        long get_src_partnum()
        {
            return _source_partnum;
        }

        void set_src_partnum(const long partnum)
        {
            _source_partnum = partnum;
        }

        schema_t* get_src_schema()
        {
            return _src_schema;
        }

        void set_src_schema(schema_t *schema)
        {
            _src_schema = schema;
        }

        void set_column_pnamemap(std::map<std::string, int> *pnamemap)
        {
            _pnamemap = pnamemap;
        }

        long get_instance_num()
        {
           return  _instance_num;
        }
        void set_instance_num(long instance_num)
        {
            _instance_num = instance_num;
        }

        void set_schema_id(long schema_id)
        {
            _schema_id = schema_id;
        }

        long get_schema_id()
        {
            return _schema_id;
        }
        void print(std::string &info);
        int get_src_port()
        {
            return _source_port;
        }
        void set_src_port(const int port)
        {
            _source_port = port;
        }
        void set_dst_type(source_t dsttype)
        {
            _dst_type = dsttype;
        }
        source_t get_dst_type()
        {
            return _dst_type;
        }
        void set_position_cmdindex(long cmdindex)
        {
            return _rowpos.set_cmdindex(cmdindex);
        }

        const cmd_t *get_cmd() {
            return _cmd;
        }

        void set_cmd(cmd_t *cmd)
        {
            _cmd = cmd;
        }

        ~row_t();
    private:
        /* 行数据，每一个字符串的代表行中的一列 */
        std::vector<char*> _cols;
        std::vector<char*> _oldcols;
        std::vector<cmd_t*> _zcols;
        //unwillingness for get_value_byname() (need schema->get_name_index(),
        //need #include "bus_config.h", then bad to bus_process.cc)
        std::map<std::string, int> *_pnamemap;
        cmd_t* _cmd;
        /* 同步点 */
        mysql_position_t _rowpos;
        /*  表名 */
        char _dbname[64];
        char _tablename[64];
        /* 行的动作 */
        action_t _action;
        /*下游的类型*/
        source_t _dst_type;
        /* 下发切片号 */
        long _dst_partnum;
        /* 上游切片号 */
        long _source_partnum;
        int _source_port;
        /* 目标schema结构 */
        schema_t *_src_schema;
        /* 下游个数 */
        long _instance_num;
        /* schema id */
        long _schema_id;
        /* 其他目的地 */
        //std::vector<long> _other_dst;
};

void clear_batch_row(std::vector<row_t*> &batch_vec);
void clear_batch_vector(std::vector<void*> &batch_vec);
bool check_redis_row(row_t *row);
int64_t get_timestamp();

}//namespace bus
#endif  //__BUS_ROW_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
