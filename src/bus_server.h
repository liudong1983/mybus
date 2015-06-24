/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_server.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/29 11:09:20
 * @version $Revision$
 * @brief 
 *  
 **/


#ifndef  __BUS_SERVER_H_
#define  __BUS_SERVER_H_

#include <stdio.h>
#include <sys/socket.h>
#include <assert.h>
#include <time.h>
#include <netinet/in.h>
#include <sys/types.h> 
#include <sys/epoll.h> 
#include <arpa/inet.h> 
#include <signal.h> 
#include <unistd.h> 
#include <errno.h>
#include <vector>
#include <string>
#include "bus_util.h"

namespace bus {
#define MAX_EPOLL_SIZE 4096
#define REPLY_UNKNOWN_COMMAND 0
#define REPLY_EMPTY_COMMAND 1
#define REPLY_OK 2
#define REPLY_TRANS_FAIL 3
#define REPLY_MATCH_FAIL 4
#define BUFF_SIZE 1024*4
#define LOOP_TIME 100 //ms
#define MAX_IDLE_TIME 86400
#define SHUTDOWN_CMD 1

class bus_client_t
{
    public:
        bus_client_t(const char *_ip, const int32_t _port, int _fd):port(_port),
        fd(_fd), mask(0), flag(0), argc(0), interact_time(0)
        {
            snprintf(ip, sizeof(ip), "%s", _ip);
            querybuf.reserve(BUFF_SIZE);
            replybuf.reserve(BUFF_SIZE);
            ++client_ct;
            this->outbuf[0] = '\0';
            this->inbuf[0] = '\0';
        }
        ~bus_client_t()
        {
            if (fd != -1)
            {
                close(fd);
                fd = -1;
            }
            --client_ct;
        }
        void reset_argv()
        {
            argc = 0;
            argv.clear();
        }
        int32_t read_socket();
        int32_t process_simple_protocol();
        int32_t process_redis_protocol();
        int32_t write_socket();
    public:
        char inbuf[1024];
        char outbuf[1024];
        std::string replybuf;
        std::string querybuf;
        int32_t port;
        char ip[32];
        int32_t fd;
        int32_t mask;
        int32_t flag; //now for shutdown
        long argc;
        std::vector<std::string> argv;
        time_t interact_time;
        static uint64_t client_ct;
};
/**
 * @brief service接口
 */
class bus_server_t
{
    public:
        bus_server_t();
        ~bus_server_t() {}
        void server_cron();
        void start_server(int32_t port);
        void stop_server();
    private:
        bool _accept_client();
        void _free_client(bus_client_t *c);
        bool _add_event(bus_client_t *myclient, int event);
        bool _del_event(bus_client_t *myclient, int event);
        bool _add_reply(bus_client_t *myclient, const char *s);
        bool _add_reply_bulk(bus_client_t *myclient, const char *s);
        bool _add_error(bus_client_t *myclient, const char *s);
        void _process_query(int fd);
        void _process_response(int fd);
        void _parse_cmd(bus_client_t *myclient);
        void _process_cmd(bus_client_t *myclient);
        void _close_idle_connection();
        void _start_command();
        
        int32_t _port;
        int32_t _epollfd;
        int32_t _acceptfd;
        bool _stop_flag;
        struct epoll_event _events[MAX_EPOLL_SIZE];
        bus_client_t* _client_list[MAX_EPOLL_SIZE];
        uint64_t _cron_count;
};
}//namespace bus
#endif  //__BUS_SERVER_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
