/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_hbase_util.cc
 * @author zhangbo6(zhangbo6@staff.sina.com.cn)
 * @date 2014/12/24 15:31:53
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_hbase_util.h"
//#include "hbase_row.h"
#include "hbase_types.h"

namespace bus{

const char *POSITION_TABLE_NAME = "databus";
const char *POSITION_CF_NAME = "bus";
const char *POSITION_QU_NAME = "position";

int bus_hbase_t::connect()
{
    if (!_closed)
        disconnect();

    try{
        boost::shared_ptr<ATT::TSocket> socket(new ATT::TSocket(_thrift_ip, _thrift_port));
        boost::shared_ptr<ATT::TTransport> transport(new ATT::TBufferedTransport(socket, 4*1024*1024, 4*1024*1024));
        boost::shared_ptr<ATP::TProtocol> protocol(new ATP::TBinaryProtocol(transport));

        _socket = socket;
        _socket->setRecvTimeout(5000);
        _socket->setSendTimeout(5000);
        _transport = transport;
        _protocol = protocol;
    } catch (std::exception &e) {
        g_logger.error("%s", e.what());
        oom_error();
    }
    
    try{
        _transport->open();
    } catch (AT::TException &e) {
        g_logger.warn("can not connect to thriftserver [%s:%d], %s",
                _thrift_ip, _thrift_port, e.what());
        return -1;
    }

    _client = new (std::nothrow)AHHT::THBaseServiceClient(_protocol);
    if (_client == NULL) oom_error();

    _closed = false;
    return 0;
}

void bus_hbase_t::disconnect()
{
    if (_closed) return;

    try{
        _transport->close();
    } catch (AT::TException &e) {
        g_logger.warn("can not succcess disconnect with thriftserver [%s:%d], %s",
                _thrift_ip, _thrift_port, e.what());
    }

    if (NULL != _client) {
        delete _client;
        _client = NULL;
    }
    _closed = true;
}

//return: 
//  0--sucess
//  -1--fatal error, maybe the table not exist
//  2--TTransportException TEAGAIN, maybe table or hbase isnot avaliable, try again)
//  3--TTransportException No more data to read, maybe thriftserver is down, try next thriftserver)
int bus_hbase_t::_put_multiple(const std::string &table)
{
    if (0 == tput_vec.size()) {g_logger.warn("tput_vec.size == 0"); return 0;}
    int ret = 0;

    try{
         _client->putMultiple(table, tput_vec);
    // illegal operation
    } catch (AT::TApplicationException &e) {
        ret = e.getType();
        g_logger.error("tput_vec.size():%d", tput_vec.size());
        g_logger.error("[%s:%d] [%s] [PUT] TApplicationException: %s:%d, stop", 
                _thrift_ip, _thrift_port, table.c_str(), e.what(), ret);
        ret = -1;
    } catch (ATT::TTransportException &e) {
        ret = e.getType(); //2 or 3
        g_logger.warn("[%s:%d] [%s] [PUT] TTransportException fail: %s:%d", 
                _thrift_ip, _thrift_port, table.c_str(), e.what(), ret);
    } catch (AT::TException &e) {
        g_logger.error("[%s:%d] [%s] [PUT] fatal error: %s, stop", 
                _thrift_ip, _thrift_port, table.c_str(), e.what());
        ret = -1;
    } 

    /*
    g_logger.debug("[%s:%d] [%s] [PUT], stop", 
        _thrift_ip, _thrift_port, table.c_str());
        */
    tput_vec.clear();

    return ret;
}

int bus_hbase_t::_del_multiple(const std::string &table)
{
    if (0 == tdelete_vec.size()) {g_logger.warn("tdelete_vec.size == 0"); return 0;}
    int ret = 0;

    try{
        rtn_vec.clear();
         _client->deleteMultiple(rtn_vec, table, tdelete_vec);
    // illegal operation
    } catch (AT::TApplicationException &e) {
        ret = e.getType();
        g_logger.error("[%s:%d] [%s] [DELETE] TApplicationException: %s:%d, stop", 
                _thrift_ip, _thrift_port, table.c_str(), e.what(), ret);
        ret = -1;
    } catch (ATT::TTransportException &e) {
        ret =  e.getType();
        g_logger.warn("[%s:%d] [%s] [DELETE] TTransportException: %s:%d", 
                _thrift_ip, _thrift_port, table.c_str(), e.what(), ret);
    } catch (AT::TException &e) {
        g_logger.error("[%s:%d] [%s] [DELETE] fatal error: %s, stop", 
                _thrift_ip, _thrift_port, table.c_str(), e.what());
        ret = -1;
    }

    tdelete_vec.clear();
    return ret;
}

int bus_hbase_t::send_to_hbase(const std::string &table, const std::vector<row_t*> &rows, uint32_t &processnum)
{
    AHHT::TColumnValue columnValue;
    AHHT::TPut tput;

    AHHT::TColumn column;
    AHHT::TDelete tdelete;

    AHHT::TColumnIncrement tcolumnincr;
    AHHT::TResult tresult;

    uint64_t cmd;
    row_t* hrow;

    tput_vec.clear();
    tdelete_vec.clear();
    int ret = 0;
    uint32_t i = 0;
    uint32_t finishcount = processnum;

    if (rows.size() == 0) return 0;
    uint64_t cmd_flag = HBASE_CMD_PUT;
    for (std::vector<row_t*>::const_iterator it = rows.begin();
            it != rows.end() && !(*_stop_flag);
            ++it, ++i) {
        if (i <= processnum && 0 != processnum) continue;
        hrow = *it;
        const cmd_t* hcmd = hrow->get_cmd();
        cmd = hcmd->get_hbase_cmd();
step:
        if (cmd == cmd_flag) {
            if (cmd_flag == HBASE_CMD_PUT) {
                //构建TPut
                columnValues.clear();
                tput.__set_row(hcmd->get_hbase_row());
                if (hcmd->get_hbase_time()) {
                    tput.__set_timestamp(hcmd->get_hbase_time());
                }
                const std::vector<tobject_t*> tcv_vec = hcmd->get_hbase_vector();
                for (std::vector<tobject_t*>::const_iterator it = tcv_vec.begin();
                        it != tcv_vec.end();
                        ++it) {
                    AHHT::TColumnValue tcv;
                    tcv.__set_family((*it)->get_cf());
                    tcv.__set_qualifier((*it)->get_qu());
                    tcv.__set_value((*it)->get_value());
                    if ((*it)->get_timestamp()) {
                        tcv.__set_timestamp((*it)->get_timestamp());
                    }
                    columnValues.push_back(tcv);
                }
                tput.__set_columnValues(columnValues);
                tput_vec.push_back(tput);
                continue;
            } else if (cmd_flag == HBASE_CMD_DEL) {
                columns.clear();
                tdelete.__set_row(hcmd->get_hbase_row());
                //对于上游为mysql来说，如果删除的话，就应该是直接删除某一行,
                //而不会有删除某一个cf或者qualifier的需求。
                /*
                const std::vector<tobject_t*> tcv_vec = hcmd->get_hbase_vector();
                for (std::vector<tobject_t*>::const_iterator it = tcv_vec.begin();
                        it != tcv_vec.end();
                        ++it) {
                    AHHT::TColumn tc;
                    tc.__set_family((*it)->get_cf());
                    tc.__set_qualifier((*it)->get_qu());
                    columns.push_back(tc);
                }
                tdelete.__set_columns(columns);
                */
                tdelete_vec.push_back(tdelete);
                continue;
            } else if (cmd_flag == HBASE_CMD_INCR) {
                columnIncrs.clear();
                const std::vector<tobject_t*> tcv_vec = hcmd->get_hbase_vector();
                for (std::vector<tobject_t*>::const_iterator it = tcv_vec.begin();
                        it != tcv_vec.end();
                        ++it) {
                    AHHT::TColumnIncrement tci;
                    tci.__set_family((*it)->get_cf());
                    tci.__set_qualifier((*it)->get_qu());
                    tci.__set_amount((*it)->get_amount());
                    columnIncrs.push_back(tci);
                }
                tincrement.__set_row(hcmd->get_hbase_row());
                tincrement.__set_columns(columnIncrs);
                try{
                    //increment没有批量的接口
                     _client->increment(tresult, table, tincrement);
                // illegal operation
                } catch (AT::TApplicationException &e) {
                    ret = e.getType();
                    g_logger.error("[%s:%d] [%s] [INCREMENT] TApplicationException: %s:%d, stop", 
                            _thrift_ip, _thrift_port, table.c_str(), e.what(), ret);
                    ret = -1; goto error_out;
                // thriftserver is closed
                } catch (ATT::TTransportException &e) {
                    ret = e.getType();
                    g_logger.warn("[%s:%d] [%s] [INCREMENT] TTransportException: %s:%d", 
                            _thrift_ip, _thrift_port, table.c_str(), e.what(), ret);
                    goto error_out;
                //disable table
                } catch (AHHT::TIOError &e) {
                    //ret = e.getType();
                    g_logger.warn("[%s:%d] [%s] [INCREMENT] TIOError: %s", 
                            _thrift_ip, _thrift_port, table.c_str(), e.what());
                    ret = 2; goto error_out;
                } catch (AHHT::TIllegalArgument &e) {
                    g_logger.warn("[%s:%d] [%s] [INCREMENT] TIllegalArgument: %s", 
                            _thrift_ip, _thrift_port, table.c_str(), e.what());
                    ret = -1; goto error_out;
                } catch (AT::TException &e) {
                    g_logger.error("[%s:%d] [%s] [INCREMENT] %s %s fatal error: %s, stop", 
                            _thrift_ip, _thrift_port, 
                            table.c_str(),
                            hcmd->get_hbase_row(),
                            e.what());
                    ret = -1; goto error_out;
                }
                finishcount = i;
                continue;
            }
        } else {
            //发送
            if (cmd_flag == HBASE_CMD_PUT) {
                //g_logger.debug("PUT %d:%d", rows.size(), i);
                if (tput_vec.size() == 0) { cmd_flag = cmd; goto step;}
                ret = _put_multiple(table);
                if (ret) {
                    goto error_out;
                }
                tput_vec.clear();
            } else if (cmd_flag == HBASE_CMD_DEL) {
                if (tdelete_vec.size() == 0) { cmd_flag = cmd; goto step;}
                ret = _del_multiple(table);
                if (ret) {
                    goto error_out;
                }
                tdelete_vec.clear();
            }

            finishcount = i - 1;
            cmd_flag = cmd;
            goto step;
        }
    }

    if (cmd_flag == HBASE_CMD_PUT) {
        ret = _put_multiple(table);
        if (ret) {
            goto error_out;
        }
    } else if (cmd_flag == HBASE_CMD_DEL) {
        ret = _del_multiple(table);
        if (ret) {
            goto error_out;
        }
    }

    processnum = 0;
    return 0;

error_out:
    processnum = finishcount;
    return ret;
}

int bus_hbase_t::incr_set_position(const std::string &poskey, const std::string &posvalue)
{
    AHHT::TColumnValue columnValue;
    AHHT::TPut tput;
    tput_vec.clear();

    columnValues.clear();
    tput.__set_row(poskey);
    columnValue.__set_family(POSITION_CF_NAME);
    columnValue.__set_qualifier(POSITION_QU_NAME);
    columnValue.__set_value(posvalue);
    columnValues.push_back(columnValue);
    tput.__set_columnValues(columnValues);
    tput_vec.push_back(tput);

    int ret = _put_multiple(POSITION_TABLE_NAME);
    if (ret) {
        g_logger.warn("[%s:%d] [%s] put position failed", 
                _thrift_ip, _thrift_port, POSITION_TABLE_NAME);
        return ret;
    }

    return 0;
}

int bus_hbase_t::put_positions(const std::map<std::string, std::string> &positions)
{
    AHHT::TColumnValue columnValue;
    AHHT::TPut tput;

    tput_vec.clear();
    for (std::map<std::string, std::string>::const_iterator it = positions.begin();
            it != positions.end();
            ++it) {
        columnValues.clear();
        
        tput.__set_row((it->first).c_str());
        columnValue.__set_family(POSITION_CF_NAME);
        columnValue.__set_qualifier(POSITION_QU_NAME);
        columnValue.__set_value((it->second).c_str());
        columnValues.push_back(columnValue);
        tput.__set_columnValues(columnValues);
        tput_vec.push_back(tput);
    }

    try{
         _client->putMultiple(POSITION_TABLE_NAME, tput_vec);
    } catch (ATT::TTransportException &e) {
        g_logger.warn("[%s:%d] [%s] [PUT] TTransportException fail: %s:%d", 
                _thrift_ip, _thrift_port, POSITION_TABLE_NAME, e.what(), e.getType());
        return -1;
    } catch (AT::TException &e) {
        g_logger.error("[%s:%d] [%s] [PUT] fatal error: %s, stop", 
                _thrift_ip, _thrift_port, POSITION_TABLE_NAME, e.what());
        return -1;
    } 
        
    return 0;
}

int bus_hbase_t::get_positions(std::map<std::string, std::string> &positions)
{
    AHHT::TColumn column;
    std::vector<AHHT::TColumn> columns;
    AHHT::TGet tget;
    std::vector<AHHT::TGet> tget_vec;
    AHHT::TResult tresult;
    std::vector<AHHT::TResult> tresults;

    //char *hrow;

    for (std::map<std::string, std::string>::iterator it = positions.begin();
            it != positions.end();
            ++it) {
        columns.clear();
        
        tget.__set_row((it->first).c_str());
        column.__set_family(POSITION_CF_NAME);
        column.__set_qualifier(POSITION_QU_NAME);
        columns.push_back(column);
        tget.__set_columns(columns);

        tget_vec.push_back(tget);
    }

    try{
         _client->getMultiple(tresults, POSITION_TABLE_NAME, tget_vec);
    } catch (ATT::TTransportException &e) {
        g_logger.warn("[%s:%d] [%s] [GET] TTransportException fail: %s:%d", 
                _thrift_ip, _thrift_port, POSITION_TABLE_NAME, e.what(), e.getType());
        return -1;
    } catch (AT::TException &e) {
        g_logger.error("[%s:%d] [%s] [GET] fatal error: %s, stop", 
                _thrift_ip, _thrift_port, POSITION_TABLE_NAME, e.what());
        return -1;
    } 

    for (std::vector<AHHT::TResult>::iterator it = tresults.begin();
            it != tresults.end();
            ++it) {
        tresult = *it;
        std::string srow = tresult.row;
        std::vector<AHHT::TColumnValue> &cv_vec = tresult.columnValues;
        if (cv_vec.size() == 0) {
            g_logger.warn("can not get pos %s, size is 0", tresult.row.c_str());
            break;
        }
        positions[srow] = cv_vec[0].value;
    }

    return 0;
}

int set_hbase_position(const char* ip, const int port, const std::map<std::string, std::string> &positions) 
{
    bool stop_flag = false;
    bus_hbase_t *hbclient = new (std::nothrow)bus_hbase_t(ip, port, &stop_flag);
    if (hbclient == NULL) oom_error();
    
    int rt = hbclient->connect();
    if (rt) {
        g_logger.warn("can not connect to [%s:%d]", ip, port);
        delete hbclient;
        return -1;
    }
    
    rt = hbclient->put_positions(positions);
    //g_logger.debug("%d", rt);
    //if (hbclient->put_positions(positions)) {
    if (rt) {
        g_logger.warn("set position to [%s:%d] fail", ip, port);
        delete hbclient;
        return -1;
    }

    hbclient->disconnect();
    delete hbclient;

    return 0;
}

int get_hbase_position(const char* ip, const int port, std::map<std::string, std::string> &positions) 
{
    bool stop_flag = false;
    bus_hbase_t *hbclient = new (std::nothrow)bus_hbase_t(ip, port, &stop_flag);
    if (hbclient == NULL) oom_error();
    
    int rt = hbclient->connect();
    if (rt) {
        g_logger.warn("can not connect to [%s:%d]", ip, port);
        delete hbclient;
        return -1;
    }
    
    if (hbclient->get_positions(positions)) {
        g_logger.warn("set position to [%s:%d] fail", ip, port);
        delete hbclient;
        return -1;
    }

    hbclient->disconnect();
    delete hbclient;

    return 0;
}



}





















/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
