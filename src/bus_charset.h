/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_charset.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/15 07:30:30
 * @version $Revision$
 * @brief 
 *  
 **/



#ifndef  __BUS_CHARSET_H_
#define  __BUS_CHARSET_H_
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <iconv.h>
#include "bus_util.h"
#include "bus_row.h"

namespace bus {
#define MAX_CHARSET_BUFFER 65535*3
class bus_mem_block_t
{
    public:
        bus_mem_block_t():size(0), block(NULL) {}
        ~bus_mem_block_t()
        {
            if (block != NULL)
            {
                delete [] block;
                block = NULL;
                size = 0;
            }
        }
        void assign(char *buf, uint32_t sz);
        char at(uint32_t index)
        {
            assert(index < size);
            return *(block + index);
        }
        bool get_bit(uint32_t pos);
        uint32_t get_bitset_count();
        uint32_t get_bitset_count(uint32_t sz);
    private:
        DISALLOW_COPY_AND_ASSIGN(bus_mem_block_t);
    public:
        uint32_t size;
        char *block;

};

class convert_charset_t {
    public:
        convert_charset_t():_cfd((iconv_t)-1) 
        {
            _from_charset[0] = '\0';
            _to_charset[0] = '\0';
        }
        ~convert_charset_t()
        {
            if (_cfd != (iconv_t)-1)
            {
                iconv_close(_cfd);
                _cfd = (iconv_t)-1;
            }
        }
        bool assign_charset(const char* from_code, const char* to_code);
        bool convert_charset(char *inbuffer, size_t len, row_t *row, bool is_old);
    private:
        char _from_charset[20];
        char _to_charset[20];
        iconv_t _cfd;
        char _out_buf[MAX_CHARSET_BUFFER];
};
}//namespace bus

#endif  //__BUS_CHARSET_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
