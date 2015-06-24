/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_util.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/29 23:32:46
 * @version $Revision$
 * @brief 
 *  
 **/
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "bus_util.h"
#include "bus_wrap.h"

namespace bus {
bus_t g_bus;
char *data_type_str[] = {"int64", "uint64", "int32", "uint32", "string"};
char *source_type_str[] = {"hbase", "mysql", "redis", "rediscounter", "bus"};
char* start_error_msg[] = {"fail, init state is not ok",
    "fail, read info from meta server fail",
    "fail, init thread pool fail",
    "fail, split job fail",
    "fail, start engine fail",
    "fail, engine is not running",
    "fail, engine is running, first stop trans",
    "fail, stat is not INIT_OK",
    "fail, reload so fail",
    "fail, engine is initting, wait"};

char *transfer_state_msg[] = {"INIT_OK",
    "FULL_TRAN",
    "FULL_TRAN_WAIT_EXIT",
    "FULL_TRAN_WAIT_EXIT",
    "FULL_TRAN_STOP_WAIT",
    "INCR_TRAN_INIT",
    "INCR_TRAN",
    "INCR_TRAN_STOP_WAIT",
    "SHUTDOWN_WAIT",
    "SHUTDOWN_EXIT",
    "TRAN_FAIL"};

char *thread_msg[] = {"init",
    "running with ok",
    "running with error",
    "exit with ok",
    "exit with error"
};

void fatal_error()
{
    g_logger.error("fatal error, exit");
    g_bus.prepare_exit();
    exit(2);
}
bool str2num(const char *str, long &num)
{
    char *endptr;
    num = strtol(str, &endptr, 10);
    if (*str != '\0' && *endptr == '\0')
        return true;
    return false;
}
bool get_ifip(const char *ifname, char *ip, uint32_t len)
{
     struct ifaddrs *ifaddr, *ifitem;
     struct sockaddr_in *sin = NULL;

     if(getifaddrs(&ifaddr) == -1) 
     {
         g_logger.error("get if addrs fail");
         return false;
     }   
    
     for(ifitem = ifaddr; ifitem != NULL; ifitem = ifitem->ifa_next)
     {   
         if(ifitem->ifa_addr == NULL) continue;
         int family = ifitem->ifa_addr->sa_family; 
    
         if(family != AF_INET)
         {    
            continue;
         }   
         sin = (struct sockaddr_in*)ifitem->ifa_addr;
         if (!strcmp(ifitem->ifa_name, ifname)) {
             snprintf(ip, len, inet_ntoa(sin->sin_addr));
             freeifaddrs(ifaddr);
             return true;
         }   
     }        
     freeifaddrs(ifaddr);
     return false;
}
bool get_inner_ip(char *ip_str, uint32_t len)
{
    if (get_ifip("eth1", ip_str, len))
    {
        return true;
    }
    if (get_ifip("eth0", ip_str, len))
    {
        return true;
    }
    if (get_ifip("eth2", ip_str, len))
    {
        return true;
    }
    if (get_ifip("eth3", ip_str, len))
    {
        return true;
    }
    if (get_ifip("eth4", ip_str, len))
    {
        return true;
    }
    return false;
}
bool copy_file(const char* source, const char* dst)
{
    assert(source != NULL && dst != NULL);
    int rfd = open(source, O_RDONLY);
    if (rfd == -1)
    {
        g_logger.error("%s so file open fail,error:%s", source, strerror(errno));
        return false;
    }

    int wfd = open(dst, O_WRONLY|O_CREAT|O_APPEND);
    if (wfd == -1)
    {
        g_logger.error("%s so file open fail,error:%s", dst, strerror(errno));
        return false;
    }
    char buf[1024];
    int rcount;
    int wcount;
    while ((rcount = read(rfd, buf, sizeof(buf))) != 0)
    {
        if (rcount == -1)
        {
            g_logger.error("read %s so file fail,error:%s", source, strerror(errno));
            return false;
        }

        wcount = write(wfd, buf, rcount);
        if (wcount != rcount)
        {
            g_logger.error("write %s so file fail", dst);
            return false;
        }
    }

    close(rfd);
    close(wfd);
    return true;
}

int translate_data_type(const char *type)
{
    int sz = sizeof(data_type_str) / sizeof(char*);
    for (int i = 0; i < sz; ++i)
    {
        if (!strcasecmp(data_type_str[i], type))
        {
            return i;
        }
    }

    return BUS_FAIL;
}

int translate_source_type(const char *type)
{
    int sz = sizeof(source_type_str) / sizeof(char*);
    for (int i = 0; i < sz; ++i)
    {
        if (!strcasecmp(source_type_str[i], type))
        {
            return i;
        }
    }
    return BUS_FAIL;
}
}//namespace bus
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
