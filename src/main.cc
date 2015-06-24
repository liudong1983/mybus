/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
/**
 * @file main.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/15 13:19:15
 * @version $Revision$
 * @brief main流程定义
 *  
 **/
#include <stdio.h>
#include <signal.h>
#include <sys/wait.h>
#include <fcntl.h>
#include "bus_util.h"
#include "bus_config.h"
#include "bus_wrap.h"
#include "threadpool.h"
#include "hiredis.h"
#include "bus_server.h"
#include "execinfo.h"
void sig_term_handler(int sig)
{
    BUS_NOTUSED(sig);
    bus::g_logger.notice("recv sigterm signal, the progress will exit");
    bus::g_bus.stop_trans_and_exit();
}

void setup_signal_handler()
{
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    /* sigterm handler */
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = sig_term_handler;
    sigaction(SIGTERM, &act, NULL);
}
void deameon()
{
    int fd;
    /* 父进程退出 */
    if (fork() != 0) {
        sleep(1);
        int status = 0;
        waitpid(-1, &status, WNOHANG);
        exit(WEXITSTATUS(status)); /* parent exits */
    }
    /* 创建新会话，并成为leader */
    setsid();

    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
    /* 写入pid */
    char * pidfile = bus::g_bus.get_config().get_pidfile();
    assert(pidfile != NULL);
    if (access(pidfile, F_OK) == 0)
    {
        bus::g_logger.error("other process is running, exit:%s exist", pidfile);
        exit(-1);
    }
    FILE *fp = fopen(pidfile, "w");
    if (fp)
    {
        fprintf(fp, "%d\n", getpid());
        fclose(fp);
    }
}

void usage()
{
    fprintf(stderr, "Usage: bus --prefix name --port busport --logdir /log/dir --loglevel 0 --meta meta_ip:meta_port\n" \
                    "Example: bus --prefix dev_yf --port 31000 --logdir /tmp --loglevel 0 --meta 127.0.0.1:8866\n" \
                    "DESCRIPTION:\n" \
                    "   -x name, --prefix name\n" \
                    "        name of your business to use databus\n" \
                    "   -p port, --port port\n" \
                    "       port of databus to listen\n" \
                    "   -d /log/dir, --logdir /log/dir\n" \
                    "       path to /log/dir\n" \
                    "   -l N, --loglevel N\n" \
                    "       N: 0(debug) 1(verbose) 2(notice) 3(warning) 4(error)\n" \
                    "   -m meta_ip:meta_port, --meta meta_ip:meta_port\n" \
                    "   -m meta_ip:meta_port, --meta meta_ip:meta_port\n" \
                    "       the ip:port of meta server(config server)\n");
    exit(1);
}
int main(int argc, char* argv[])
{
    if (argc < 7 || !(argc%2))
    {
        usage();
    }

    //bus::g_logger.set_logname("./bus_init.log");
    bus::g_logger.set_loglevel(LOG_NOTICE);
    //解析命令行参数
    if (!bus::g_bus.init(argc, argv))
    {
        bus::g_logger.error("parse command line arguments fail");
        exit(1);
    }
    bus::g_logger.notice("parse command line arguments succ");
    //设置deameon
    deameon();
    //设置信号
    setup_signal_handler();
    //启动server
    int port = bus::g_bus.get_config().get_local_server()->get_port();
    bus::g_bus.get_server().start_server(port);
    return 0;
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
