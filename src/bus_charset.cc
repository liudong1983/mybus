/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_charset.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/15 08:08:09
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_charset.h"
namespace bus {
void bus_mem_block_t::assign(char *buf, uint32_t sz)
{
    //释放原有block
    if (block != NULL)
    {
        delete [] block;
        block = NULL;
        size = 0;
    }
    //分配新的block
    block = new(std::nothrow) char[sz];
    if (block == NULL) oom_error();
    //拷贝内存
    memcpy(block, buf, sz);
    size = sz;
}
bool bus_mem_block_t::get_bit(uint32_t pos)
{
    uint32_t mchar = pos >> 3;
    uint32_t nbit = pos & 0x07;
    assert(mchar < size);
    return ((block[mchar] >> nbit) & 0x01) == 0x01 ? true : false;
}
uint32_t bus_mem_block_t::get_bitset_count()
{
    uint32_t ret = 0;
    for (std::size_t i = 0; i < size; i++)
    {
        uint8_t p = *(block + i);
        while (p != 0)
        {
            if (p & 0x01) ++ret;
            p = p >> 1;
        }
    }
    return ret;
}
uint32_t bus_mem_block_t::get_bitset_count(uint32_t sz)
{
    uint32_t ret = 0;
    assert(sz <= size * 8);
    for (uint32_t i = 0; i < sz; i++)
    {
        if (get_bit(i)) ++ret;
    }
    return ret;
}
/**********************************************************/
bool convert_charset_t::assign_charset(const char* from_code, const char* to_code)
{
    assert(from_code != NULL && to_code != NULL);
    if (!strcasecmp(from_code, _from_charset) &&
            !strcasecmp(to_code, _to_charset))
    {
        return true;
    }

    if (_cfd != (iconv_t)-1)
    {
        iconv_close(_cfd);
        _cfd = (iconv_t)-1;
    }
    snprintf(_from_charset, sizeof(_from_charset), from_code);
    snprintf(_to_charset, sizeof(_to_charset), to_code);
    if (strcasecmp(_from_charset, _to_charset))
    {
        _cfd = iconv_open(_to_charset, _from_charset);
        if (_cfd == (iconv_t)-1)
        {
            g_logger.error("open %s to %s iconv fail,error:%s",
                    _from_charset, _to_charset, strerror(errno));
            return false;
        }
    }
    return true;
}
bool convert_charset_t::convert_charset(char *inbuffer, size_t len, row_t *row, bool is_old)
{
    if (_cfd == (iconv_t)-1)
    {
        if (_from_charset[0] != '\0' &&
                !strcasecmp(_from_charset, _to_charset))
        {
            row->push_back(inbuffer, len, is_old);
            return true;
        } else {
            g_logger.notice("charset descriptor don't prepare ok");
            row->push_back(inbuffer, len, is_old);
            return false;
        }
    }
    char *inbuffer1 = inbuffer;
    char *outbuffer1 = _out_buf;
    size_t inlen1 = len;
    size_t outlen1 = MAX_CHARSET_BUFFER;

    size_t ret = iconv(_cfd, &inbuffer1, &inlen1, &outbuffer1, &outlen1);
    if (ret == (size_t)-1)
    {
        if (errno == E2BIG)
        {
            g_logger.error("impossible case, out buffer room is not enough,error:%s", strerror(errno));

        } else if (errno == EILSEQ)
        {
            g_logger.error("input buffer contain invalid multibyte,error:%s", strerror(errno));
        } else if (errno == EINVAL)
        {
            g_logger.error("incomplete multibyte,error:%s", strerror(errno));
        }
        row->push_back(inbuffer, len, is_old);
        return false;
    }
    size_t output_len = MAX_CHARSET_BUFFER - outlen1;
    row->push_back(_out_buf, output_len, is_old);
    return true;
}

}//namespace bus

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
