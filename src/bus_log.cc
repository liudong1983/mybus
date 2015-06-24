/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_log.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/27 12:18:29
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_log.h"
namespace bus {
bus_log_t g_logger;
void oom_error()
{
    g_logger.error("allocate memory fail, exit");
    exit(1);
}
}



/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
