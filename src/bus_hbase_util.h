/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_hbase_util.h
 * @author zhangbo6(zhangbo6@staff.sina.com.cn)
 * @date 2014/12/24 15:32:14
 * @version $Revision$
 * @brief 
 *  
 **/



#ifndef  __BUS_HBASE_UTIL_H_
#define  __BUS_HBASE_UTIL_H_
#include<stdio.h>
#include<stdlib.h>
#include<pthread.h>
#include<sys/time.h>

#include <iostream>
#include <utility>
#include <string>
#include <vector>
#include <map>
#include "THBaseService.h"
#include<thrift/config.h>
#include<thrift/transport/TSocket.h>
#include<thrift/transport/TBufferTransports.h>
#include<thrift/protocol/TBinaryProtocol.h>

#include "threadpool.h"
#include "bus_config.h"
#include "bus_wrap.h"
#include "bus_row.h"

namespace AT = apache::thrift;
namespace ATT = apache::thrift::transport;
namespace ATP = apache::thrift::protocol;
namespace AHHT = apache::hadoop::hbase::thrift2;

namespace bus{
extern const char *POSITION_TABLE_NAME;

class bus_hbase_t 
{
    public:
        bus_hbase_t(bool *stop_flag):
        _stop_flag(stop_flag),_closed(true),_client(NULL) {
            tput_vec.reserve(HBASE_BATCH_COUNT);
            tdelete_vec.reserve(HBASE_BATCH_COUNT);
            rtn_vec.reserve(HBASE_BATCH_COUNT);
        }

        // for get/put_hbase_position(), 只有put/get操作
        bus_hbase_t(const char* ip, int port, bool* stop_flag):
            _thrift_port(port),_stop_flag(stop_flag),
            _closed(true),_client(NULL) { //bug resolved
            snprintf(_thrift_ip, sizeof(_thrift_ip), ip);
            tput_vec.reserve(HBASE_BATCH_COUNT);
            //tdelete_vec.reserve(HBASE_BATCH_COUNT);
            //rtn_vec.reserve(HBASE_BATCH_COUNT);
        }

        ~bus_hbase_t() {
            if (NULL != _client) {
                delete _client;
                _client = NULL;
            }
        }
        /*
        const char* POSITION_TABLE_NAME = "databus";
        const char* POSITION_CF_NAME = "bus";
        const char* POSITION_QU_NAME = "position";
        */
        
        void set_thrift_ip_port(const char *ip, int port)
        {
            snprintf(_thrift_ip, sizeof(_thrift_ip), ip);
            _thrift_port = port;
        }

        int connect();
        void disconnect();
        int put_positions(const std::map<std::string, std::string> &cmds);
        int get_positions(std::map<std::string, std::string> &cmds);
        //int full_send(const std::string &table, const std::vector<row_t*> &puts);
        int full_send(const std::string &table, const std::vector<row_t*> &puts, uint32_t &processnum);
        //int incr_send(const std::string &table, const std::vector<row_t*> &rows, uint32_t &processnum);
        int send_to_hbase(const std::string &table, const std::vector<row_t*> &rows, uint32_t &processnum);
        int incr_set_position(const std::string &poskey, const std::string &posvalue);

        std::vector<AHHT::TColumnValue> columnValues;
        std::vector<AHHT::TPut> tput_vec;
        std::vector<AHHT::TColumnIncrement> columnIncrs;
        AHHT::TIncrement tincrement;
        std::vector<AHHT::TColumn> columns;
        std::vector<AHHT::TDelete> tdelete_vec;
        std::vector<AHHT::TDelete> rtn_vec;

        /*
        int put_positions(const char* table, const std::vector<hbase_cmd_t*> &cmds);
        int get_positions(const char* table, std::vector<hbase_cmd_t*> &cmds);
        int put(const std::string &table, const hbase_tput_t &put);
        int put_multiple(const std::string &table, const std::vector<hbase_tput_t> &puts);
        int get(hbase_tresult_t &rtn, const std::string &table, const hbase_tget_t &get);
        int get_multiple(std::vector<hbase_tresult_t> &rtns, const std::string &table, const std::vector<hbase_tget_t> &gets);
        int del(const std::string &table, const hbase_tdelete_t &del);
        int del_multiple(const std::string &table, const std::vector<hbase_tdelete_t> &dels);
        */
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_hbase_t);
        //_put_multiple(const std::string &table, const std::vector<AHHT::TPut> tput_vec),
        //all bus_hbase_t::tput_vec;
        int _put_multiple(const std::string &table);
        int _del_multiple(const std::string &table);
        char _thrift_ip[128];
        int _thrift_port;
        bool* _stop_flag;
        bool _closed;
        boost::shared_ptr<ATT::TSocket> _socket;
        boost::shared_ptr<ATT::TTransport> _transport;
        boost::shared_ptr<ATP::TProtocol> _protocol;
        AHHT::THBaseServiceClient *_client;
};

int set_hbase_position(const char* ip, const int port, const std::map<std::string, std::string> &positions);
int get_hbase_position(const char* ip, const int port, std::map<std::string, std::string> &positions);



}

#endif  //__BUS_HBASE_UTIL_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
