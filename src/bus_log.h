/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_log.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/27 11:34:41
 * @version $Revision$
 * @brief 
 *  
 **/
#ifndef  __BUS_LOG_H_
#define  __BUS_LOG_H_
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<stdint.h>
#include<stdarg.h>
#include<time.h>
#include<string.h>
#include<errno.h>
#include<pthread.h>
#include<arpa/inet.h>
#include<ifaddrs.h>
#include<pthread.h>

namespace bus {
/* Log levels */
#define LOG_DEBUG 0
#define LOG_VERBOSE 1
#define LOG_NOTICE 2
#define LOG_WARNING 3
#define LOG_ERROR 4

#define PROCESS_FATAL -1
#define PROCESS_OK 0
#define PROCESS_IGNORE 1
/* log print */
#define debug(arg2, ...) \
		log(LOG_DEBUG, "[%x] [%s:%s] [%d] "arg2, pthread_self(), __FILE__, __func__, __LINE__, ##__VA_ARGS__)
#define notice(arg2, ...) \
		log(LOG_NOTICE, "[%x] [%s:%s] [%d] "arg2, pthread_self(), __FILE__, __func__, __LINE__, ##__VA_ARGS__)
#define warn(arg2, ...) \
		log(LOG_WARNING, "[%x] [%s:%s] [%d] "arg2, pthread_self(), __FILE__, __func__, __LINE__, ##__VA_ARGS__)
#define error(arg2, ...) \
		log(LOG_ERROR, "[%x] [%s:%s] [%d] "arg2, pthread_self(), __FILE__, __func__, __LINE__, ##__VA_ARGS__)
#define MAX_LOGMSG_LEN    4096
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&); \
    void operator=(const TypeName &)
enum action_t {INSERT, UPDATE, DEL};
void oom_error();
/* class defifnition */
class lock_t
{
    public:
        explicit lock_t(pthread_mutex_t* mutex):_mutex(mutex)
        {
            pthread_mutex_lock(_mutex);
        }
        ~lock_t()
        {
            pthread_mutex_unlock(_mutex);
        }
    private:
        pthread_mutex_t* _mutex;
};

class bus_log_t
{
    public:
        bus_log_t():_fp(NULL) {
            pthread_mutex_init(&_mutex, NULL);
            _logpath[0] = '\0';
        }

        void set_loglevel(const int32_t verbosity)
        {
            _verbosity = verbosity;
        }

        void set_logname(const char *filename)
        {
            snprintf(_logpath, sizeof(_logpath), "%s", filename);
        }

        void log(int level, const char *fmt, ...) 
        {
            if ((level & 0xff) < _verbosity) return;
            va_list ap;
            char msg[MAX_LOGMSG_LEN];

            va_start(ap, fmt);
            vsnprintf(msg, sizeof(msg), fmt, ap);
            va_end(ap);

            lock_t mylock(&_mutex);           
            lograw(level,msg);
        }
        void lograw(int level, const char *msg) 
        {
            //const char *c[] = {"DEBUG ", "INFO  ", "NOTICE", "WARN  ", "ERROR "};
            const char *c[] = {"[DEBUG] ", "[INFO]  ", "[NOTICE]", "[WARN]  ", "[ERROR] "};
            time_t now = time(NULL);
            char buf[64];

            level &= 0xff; /* clear flags */
            if (level < this->_verbosity) return;
            if (level == LOG_ERROR) {
                snprintf(error_info5, sizeof(error_info5), error_info4);
                snprintf(error_info4, sizeof(error_info5), error_info3);
                snprintf(error_info3, sizeof(error_info5), error_info2);
                snprintf(error_info2, sizeof(error_info5), error_info1);
                snprintf(error_info1, sizeof(error_info1), msg);
            }
            strftime(buf, sizeof(buf), "%d %b %H:%M:%S", localtime(&now));
            if (NULL == _fp && '\0' != _logpath[0]) {
                _fp =  fopen(this->_logpath, "a");
                if (!_fp) {
                    //fprintf(stdout, "[%d] %s [%s] %s\n", static_cast<int>(getpid()), buf, c[level], msg);
                    fprintf(stdout, "[%d] %s %s %s\n", static_cast<int>(getpid()), buf, c[level], msg);
                    fprintf(stdout, "[%d] %s [ERROR] open %s fail:%s\n", static_cast<int>(getpid()), buf, _logpath, strerror(errno));
                    return;
                }
            }

            if (NULL == _fp) {
                //fprintf(stdout, "[%d] %s [%s] %s\n", static_cast<int>(getpid()), buf, c[level], msg);
                fprintf(stdout, "[%d] %s %s %s\n", static_cast<int>(getpid()), buf, c[level], msg);
                fflush(stdout);
            } else {
                //fprintf(_fp, "[%d] %s [%s] %s\n", static_cast<int>(getpid()), buf, c[level], msg);
                fprintf(_fp, "[%d] %s %s %s\n", static_cast<int>(getpid()), buf, c[level], msg);
                fflush(_fp);
            }
        }

        void reset_error_info()
        {
            lock_t mylock(&_mutex);
            error_info1[0] = '\0';
            error_info2[0] = '\0';
            error_info3[0] = '\0';
            error_info4[0] = '\0';
            error_info5[0] = '\0';
        }
        ~bus_log_t()
        {
            pthread_mutex_destroy(&_mutex);
            if (NULL != _fp) fclose(_fp);
        }
    public:
        char error_info1[256];
        char error_info2[256];
        char error_info3[256];
        char error_info4[256];
        char error_info5[256];
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_log_t);
        FILE *_fp;
        char _logpath[128];
        int32_t _verbosity;
        pthread_mutex_t _mutex;
};
extern bus_log_t g_logger;
} //namespace bus


#endif  //__BUS_LOG_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
