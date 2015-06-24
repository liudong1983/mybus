/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_server.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/29 16:56:20
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_server.h"
#include "bus_wrap.h"
namespace bus {
static char *infomsg[] = {"unknown command",
    "empty command",
    "ok",
    "start trans fail",
    "get match info fail"};
uint64_t bus_client_t::client_ct = 0;
int32_t bus_client_t::read_socket()
{
    int64_t sz = read(this->fd, inbuf, sizeof(inbuf));
    if(sz == -1)
    {
        g_logger.error("read data from ip=%s, port=%d fd=%d, fail,error:%s", ip, port, fd, strerror(errno));
        return -1;
    }else if(sz == 0)
    {
        g_logger.debug("read socket eof from ip=%s, port=%d fd=%d, peer close fd", ip, port, fd);
        return 1;
    }

    this->inbuf[sz] = '\0';
    this->querybuf.append(this->inbuf);
    return 0;
}

int32_t bus_client_t::process_redis_protocol()
{
    std::string::size_type newline = this->querybuf.find_first_of("\r\n");
    if(newline == std::string::npos)
    {
        return 1;
    }
    //读取参数个数
    std::string str_argc = this->querybuf.substr(1, newline - 1);
    if (!str2num(str_argc.c_str(), argc)) {
        g_logger.error("redis protocl error, argument number=%s is invalid", str_argc.c_str());
        return -1;
    }
    //逐个参数进行读取
    for(int i = 0; i< argc; i++)
    {
        std::string::size_type pos1 = this->querybuf.find_first_of("\r\n", newline + 2);
        if (pos1 == std::string::npos)
        {
            this->argc = 0;
            this->argv.clear();
            return 1;
        }
        if (pos1 <= newline + 2 || this->querybuf[newline + 2] != '$')
        {
            return -1;
        }
        std::string str_argv_len = this->querybuf.substr(newline + 2, pos1 - newline - 2);
        std::string::size_type pos2 = this->querybuf.find_first_of("\r\n", pos1 + 2);
        if (pos2 == std::string::npos)
        {
            this->argc = 0;
            this->argv.clear();
            return 1;
        }

        if (pos2 <= pos1 + 2)
        {
            return -1;
        }
        std::string str_argv = this->querybuf.substr(pos1 + 2, pos2 - pos1 - 2);
        this->argv.push_back(str_argv);
        newline = pos2;
    }

    this->querybuf.erase(0, newline + 2);
    return 0;
}
int32_t bus_client_t::process_simple_protocol()
{
    std::string::size_type newline = this->querybuf.find_first_of("\r\n");
    if(newline == std::string::npos)
    {
        return 1;
    }
    std::string query = this->querybuf.substr(0, newline);
    g_logger.debug("query=%s, buffer=%s", query.c_str(), this->querybuf.c_str());
    //querybuf pos move
    this->querybuf.erase(0, newline + 2);

    //parse argment
    this->argc = 0;
    this->argv.clear();
    char delimiter = ' ';
    std::string::size_type begin = query.find_first_not_of(delimiter);
    while(begin != std::string::npos)
    {
        std::string::size_type end = query.find_first_of(delimiter, begin);
        if(end == std::string::npos)
        {
            std::string parm = query.substr(begin);
            this->argv.push_back(parm);
            this->argc++;
            return 0;
        }
        std::string parm = query.substr(begin, end - begin);
        this->argv.push_back(parm);
        this->argc++;
        begin = query.find_first_not_of(delimiter, end + 1);
    }

    return 0;
}
int32_t bus_client_t::write_socket()
{
    size_t len = strlen(this->outbuf);
    if (len != 0) {
        ssize_t sz = write(this->fd, outbuf, len);
        if(sz <= 0 || sz != (ssize_t)len)
        {
            g_logger.error("write data ip=%s, port=%d, fd=%d, fail,error:%s",
                    ip, port, fd, strerror(errno));
            return -1;
        }
    }
    len = replybuf.size();
    if (len != 0) {
        ssize_t sz = write(this->fd, replybuf.c_str(), len);
        if(sz <= 0 || sz != (ssize_t)len)
        {
            g_logger.error("write data ip=%s, port=%d, fd=%d, fail,error:%s",
                    ip, port, fd, strerror(errno));
            return -1;
        }
    }
    this->outbuf[0] = '\0';
    this->replybuf.clear();
    return 0;
}
/********************************************************************/
void bus_server_t::_free_client(bus_client_t *c)
{
    assert(c != NULL);
    int index = c->fd;
    this->_del_event(c, c->mask);
    assert(_client_list[index] != NULL);
    delete _client_list[index];
    _client_list[index] = NULL;
}
bool bus_server_t::_add_event(bus_client_t *myclient, int event)
{
    int op = myclient->mask == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    struct epoll_event e;
    e.events = myclient->mask | event;
    e.data.u64 = 0;
    e.data.fd = myclient->fd;
    int ret = epoll_ctl(this->_epollfd, op, myclient->fd, &e);
    if(ret != 0)
    {
        g_logger.error("epoll op add/mod fail,error:%s", strerror(errno));
        return false;
    }
    myclient->mask = e.events;
    return true;
}

bool bus_server_t::_del_event(bus_client_t *myclient, int event)
{
    event = myclient->mask & ~event;
    int op = event == 0 ? EPOLL_CTL_DEL : EPOLL_CTL_MOD;
    struct epoll_event e;
    e.events = event;
    e.data.u64 = 0;
    e.data.fd = myclient->fd;
    int ret = epoll_ctl(this->_epollfd, op, myclient->fd, &e);
    if(ret != 0)
    {
        g_logger.error("epoll fd=%d del event=%d fail, error:%s", myclient->fd, event, strerror(errno));
        return false;
    }
    myclient->mask = e.events;
    return true;
}
bool bus_server_t::_add_reply(bus_client_t *c, const char *s)
{
    snprintf(c->outbuf, sizeof(c->outbuf), "+%s\r\n", s);
    return this->_add_event(c, EPOLLOUT);
}
bool bus_server_t::_add_reply_bulk(bus_client_t *c, const char *s)
{
    char tmpbuf[128];
    uint64_t len = strlen(s);
    snprintf(tmpbuf, sizeof(tmpbuf), "$%lu\r\n", len);
    c->replybuf.append(tmpbuf);
    c->replybuf.append(s);
    c->replybuf.append("\r\n");
    return this->_add_event(c, EPOLLOUT);
}

bool bus_server_t::_add_error(bus_client_t *c, const char *s)
{
    snprintf(c->outbuf, sizeof(c->outbuf), "-ERR %s\r\n", s);
    return this->_add_event(c, EPOLLOUT);
}

bool bus_server_t::_accept_client()
{
    sockaddr client_addr;
    socklen_t client_addr_len = 0;

    int client_fd = accept(this->_acceptfd, (sockaddr*)&client_addr, &client_addr_len);
    if(client_fd == -1)
    {
        g_logger.error("accept client socket fail,error:%s", strerror(errno));
        return false;
    }
    
    if (client_fd >= MAX_EPOLL_SIZE)
    {
        g_logger.error("client fd=%d exceed max fd=%d", client_fd, MAX_EPOLL_SIZE);
        return false;
    }
    sockaddr_in* client_addr_in = (sockaddr_in*)(&client_addr);
    char *ip = inet_ntoa(client_addr_in->sin_addr);
    int port = ntohs(client_addr_in->sin_port);
    g_logger.debug("connect with client %s:%d", ip, port);
    bus_client_t* myclient = new(std::nothrow) bus_client_t(ip, port, client_fd);
    if (myclient == NULL) oom_error();
    this->_client_list[myclient->fd] = myclient;
    return this->_add_event(myclient, EPOLLIN);
}


void bus_server_t::_process_query(int fd)
{
    bus_client_t *myclient = this->_client_list[fd];
    assert(myclient != NULL);
    /* 从socket中读数据 */
    int32_t ret = myclient->read_socket();
    if(ret == -1)
    {
        g_logger.error("read socket fail,error:%s", strerror(errno));
        this->_free_client(myclient);
        return;
    } else if (ret == 1) {
        g_logger.debug("peer close socket", myclient->ip, myclient->port);
        this->_free_client(myclient);
        return;
    }
    
    myclient->interact_time = time(NULL);
    /* 将读到的数据按照协议进行解析，并进行处理 */
    _parse_cmd(myclient);
}
void bus_server_t::_parse_cmd(bus_client_t *myclient)
{
    int ret = -1;
    while(myclient->querybuf.size() != 0)
    {
        if(myclient->querybuf[0] == '*')
        {
            ret = myclient->process_redis_protocol();
        }  else {
            ret = myclient->process_simple_protocol();
        }

        if(ret == -1)
        {
            g_logger.error("process protocol error");
            this->_free_client(myclient);
            return;
        }else if(ret == 1)
        {
            return;
        } else if (ret == 0)
        {
            _process_cmd(myclient);
            myclient->reset_argv();
        }
    }
}

void bus_server_t::_close_idle_connection()
{
    time_t now = time_t();
    for (uint32_t i = 0; i < MAX_EPOLL_SIZE; i++)
    {
        if (_client_list[i] != NULL)
        {
           if (now - _client_list[i]->interact_time > MAX_IDLE_TIME)
           {
               g_logger.notice("client ip=%s, port=%d exceed max idle time, close",
                       _client_list[i]->ip, _client_list[i]->port);
               _free_client(_client_list[i]);
               _client_list[i] = NULL;
           }
        }
    }
}

void bus_server_t::server_cron()
{
    ++_cron_count;
    if (!(_cron_count % 36000))
    {
        _close_idle_connection();
    }

    if (!(_cron_count % 10))
    {
        g_bus.cron_func();
    }

    // 1分钟
    if (!(_cron_count % 600))
    {
        g_bus.get_stat().reset_all_thrift_flag();
    }
}

bus_server_t::bus_server_t():_stop_flag(false), _cron_count(0) 
{
    bzero(_events, sizeof(struct epoll_event) * MAX_EPOLL_SIZE);
    for (uint32_t i = 0; i < MAX_EPOLL_SIZE; i++)
    {
        _client_list[i] = NULL;
    }
}

void bus_server_t::_process_cmd(bus_client_t *myclient)
{
    if(myclient->argc == 0)
    {
        g_logger.debug("empty command");
        this->_add_error(myclient, infomsg[REPLY_EMPTY_COMMAND]);
        return;
    }

    int errorcode;
    if(myclient->argc == 1 && 
            myclient->argv[0] == "ping") {
        this->_add_reply(myclient, "pong");
    } else if(myclient->argc == 2 && 
            myclient->argv[0] == "start" && myclient->argv[1] == "trans") {
        g_logger.notice("recv start trans command");
        g_logger.reset_error_info();
        if(!g_bus.start_trans(errorcode)) {
            g_logger.notice("trans fail,error:%s", start_error_msg[errorcode]);
            this->_add_error(myclient, start_error_msg[errorcode]);
        } else {
            this->_add_reply(myclient, infomsg[REPLY_OK]);
        }
    } else if (myclient->argc == 2 && 
            myclient->argv[0] == "stop" && myclient->argv[1] == "trans") {
        g_logger.notice("recv stop trans command");
        g_logger.reset_error_info();
        if (!g_bus.stop_trans(errorcode))
        {
            g_logger.notice("stop trans fail,error:%s", start_error_msg[errorcode]);
            this->_add_error(myclient, start_error_msg[errorcode]);
        } else {
            this->_add_reply(myclient, infomsg[REPLY_OK]);
        }
    } else if (myclient->argc == 1 && 
            myclient->argv[0] == "shutdown") {
        g_logger.notice("recv shutdown command");
        g_logger.reset_error_info();
        if (!g_bus.shutdown(errorcode)) {
            g_logger.notice("shutdown fail,error:%s", start_error_msg[errorcode]);
            this->_add_error(myclient, start_error_msg[errorcode]);
        } else {
            myclient->flag |= SHUTDOWN_CMD;
            this->_add_reply(myclient, infomsg[REPLY_OK]);
        }
    } else if(myclient->argc == 1 &&
            myclient->argv[0] == "info") {
        g_logger.debug("recv info command");
        char buf[10240];
        g_bus.info(buf, 10240);
        this->_add_reply_bulk(myclient, buf);
    } else if(myclient->argc == 3 &&
            myclient->argv[0] == "set" && myclient->argv[1] == "loglevel") {
        g_logger.notice("recv set loglevel command");
        int32_t ret = 0;
        if (myclient->argv[2] == "debug") {
            ret = LOG_DEBUG;
        } else if (myclient->argv[2] == "info") {
            ret = LOG_VERBOSE;
        } else if (myclient->argv[2] == "notice") {
            ret = LOG_NOTICE;
        } else if (myclient->argv[2] == "warn") {
            ret = LOG_WARNING;
        } else if (myclient->argv[2] == "error") {
            ret = LOG_ERROR;
        } else {
            this->_add_error(myclient, "loglevel is invalid");
            return;
        }
        g_logger.set_loglevel(ret);
        this->_add_reply(myclient, infomsg[REPLY_OK]);
    } else if (myclient->argc == 3 &&
            myclient->argv[0] == "set" && myclient->argv[1] == "posfreq")
    {
        const char *freq = myclient->argv[2].c_str();
        g_logger.notice("recv command: set posfreq %s", freq);
        int64_t ifreq = atoi(freq);
        if (ifreq <= 0) {
            this->_add_reply(myclient, "invalid argument");
        } else {
            g_bus.set_posfreq(static_cast<uint64_t>(ifreq));
            this->_add_reply(myclient, infomsg[REPLY_OK]);
        }
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "checkpos")
    {
        const char *checkpos = myclient->argv[1].c_str();
        g_logger.notice("recv command: checkpos %s", checkpos);
        if (!strcmp("true", checkpos))
            g_bus.set_mysql_full_check_pos(true);
        else
            g_bus.set_mysql_full_check_pos(false);
        this->_add_reply(myclient, infomsg[REPLY_OK]);
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "reload" && myclient->argv[1] == "so")
    {
        g_logger.notice("recv reload so command");
        int errorcode;
        if (!g_bus.reload_so(errorcode))
        {
            g_logger.notice("reload so fail");
            this->_add_error(myclient, "reload so fail");
            return;
        }
        this->_add_reply(myclient, infomsg[REPLY_OK]);
    } else if (myclient->argc == 4 &&
            myclient->argv[0] == "add" && myclient->argv[1] == "black") {
        g_logger.notice("recv add black command");
        bool ret = g_bus.add_blacklist(myclient->argv[2].c_str(), 
                myclient->argv[3].c_str());
        if (!ret) {
            this->_add_error(myclient, start_error_msg[0]);
        } else {
            this->_add_reply(myclient, infomsg[REPLY_OK]);
        }
    } else if (myclient->argc == 4 && 
            myclient->argv[0] == "del" && myclient->argv[1] == "black") {
        g_logger.notice("recv del black command");
        bool ret = g_bus.del_blacklist(myclient->argv[2].c_str(),
                myclient->argv[3].c_str());
        if (!ret) {
            this->_add_error(myclient, start_error_msg[0]);
        } else {
            this->_add_reply(myclient, infomsg[REPLY_OK]);
        }
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "clear" && myclient->argv[1] == "black") {
        g_logger.notice("recv clear black command");
        bool ret = g_bus.clear_blacklist();
        if (!ret) {
            this->_add_error(myclient, start_error_msg[0]);
        } else {
            this->_add_reply(myclient, infomsg[REPLY_OK]);
        }
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "info" && myclient->argv[1] == "matchschema") {
        g_logger.notice("recv get matchinfo command");
        std::string info;
        info.reserve(10240);
        if (!g_bus.get_match_info(info))
        {
            g_logger.error("get match info fail");
            this->_add_error(myclient, infomsg[REPLY_MATCH_FAIL]);
            return;
        }
        this->_add_reply_bulk(myclient, info.c_str());
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "get" && myclient->argv[1] == "blacklist") {
        g_logger.debug("recv get blacklist command");
        std::string info;
        info.reserve(1024);
        bus_config_t &config = g_bus.get_config();
        config.get_blacklist(info);
        this->_add_reply_bulk(myclient, info.c_str());
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "info" && myclient->argv[1] == "source") {
        g_logger.debug("recv info source command");
        std::string info;
        info.reserve(1024);
        g_bus.get_source_info(info);
        this->_add_reply_bulk(myclient, info.c_str());
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "info" && myclient->argv[1] == "thread") {
        g_logger.debug("recv info thread command");
        std::string info;
        info.reserve(1024);
        g_bus.get_thread_info(info);
        this->_add_reply_bulk(myclient, info.c_str());
    } else if (myclient->argc == 2 &&
            myclient->argv[0] == "info" && myclient->argv[1] == "stat") {
        g_logger.debug("recv info stat command");
        std::string info;
        info.reserve(1024);
        g_bus.get_thread_stat_info(info);
        this->_add_reply_bulk(myclient, info.c_str());
    } else {
        g_logger.notice("command=%s,unknown command", myclient->argv[0].c_str());
        this->_add_error(myclient, infomsg[REPLY_UNKNOWN_COMMAND]);
    }
}

void bus_server_t::_process_response(int fd)
{
    bus_client_t *myclient = this->_client_list[fd];
    assert(myclient != NULL);
    int ret = myclient->write_socket();
    if(ret != 0)
    {
        g_logger.error("write data fail");
        this->_free_client(myclient);
        return;
    }

    this->_del_event(myclient, EPOLLOUT);
    if (SHUTDOWN_CMD == myclient->flag) _stop_flag = true;
}

void bus_server_t::start_server(int32_t port)
{
    this->_port = port;
    sockaddr_in server_addr;
    memset(&server_addr, '\0', sizeof(sockaddr_in));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(this->_port);
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    //create accept socket
    this->_acceptfd = socket(AF_INET, SOCK_STREAM, 0);
    if(this->_acceptfd == -1){
        g_logger.error("create accept fd fail, error:%s", strerror(errno));
        fatal_error();
    }
    int mode = 1;
    int32_t ret = setsockopt(this->_acceptfd, SOL_SOCKET, SO_REUSEADDR, (void*)&mode, sizeof(int));
    if (ret != 0)
    {
	g_logger.error("set socket resuse fail,error:%s", strerror(errno));
	fatal_error();
    }
    //bind address
    ret = bind(this->_acceptfd, (sockaddr*)&server_addr, sizeof(server_addr));
    if(ret != 0){
        g_logger.error("bind address fail,error:%s", strerror(errno));
        fatal_error();
    }
    //create epoll fd
    this->_epollfd = epoll_create(MAX_EPOLL_SIZE);
    struct epoll_event accept_event;
    accept_event.events = EPOLLIN;
    accept_event.data.u64 = 0;
    accept_event.data.fd = this->_acceptfd;
    ret = epoll_ctl(this->_epollfd, EPOLL_CTL_ADD, this->_acceptfd, &accept_event);
    if(ret == -1){
        g_logger.error("add accept fd to epoll fail,error:%s", strerror(errno));
        fatal_error();
    }
    //listen accept fd
    ret = listen(this->_acceptfd, 10);
    if(ret == -1){
        g_logger.error("listen accept fd fail,error:%s", strerror(errno));
        fatal_error();
    }
    
    g_logger.notice("start server port=%d succ", port);
    //begin accept a connection
    while(!_stop_flag)
    {
        int fdnum = epoll_wait(this->_epollfd, this->_events, sizeof(_events)/sizeof(struct epoll_event), LOOP_TIME);
        if (fdnum == -1)
        {
            if (errno != EINTR) {
                g_logger.error("epoll wait fail,error:%s", strerror(errno));
            }
            fdnum = 0;
        }

        for(int i = 0; i < fdnum; i++)
        {
            if(this->_events[i].data.fd == this->_acceptfd)
            {
                this->_accept_client();
            }else if(this->_events[i].events & EPOLLIN)
            {
                int fd = this->_events[i].data.fd;
                _process_query(fd);
            }else if(this->_events[i].events & EPOLLOUT)
            {
                int fd = this->_events[i].data.fd;
                _process_response(fd);
            }
        }//for
        server_cron();
    }//while

    for(int i = 0; i < MAX_EPOLL_SIZE; i++)
    {
        if(_client_list[i] != NULL)
        {
            this->_free_client(_client_list[i]);
            _client_list[i] = NULL;
        }
    }
    if(this->_acceptfd != -1)
    {
        close(this->_acceptfd);
        this->_acceptfd = -1;
    }

    if(this->_epollfd != -1)
    {
        close(this->_epollfd);
        this->_epollfd = -1;
    }
}
void bus_server_t::stop_server()
{
    //_stop_flag = true;
    /* 快速重启 */
    if(this->_acceptfd != -1)
    {
        close(this->_acceptfd);
        this->_acceptfd = -1;
    }
}

}//namespace bus
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
