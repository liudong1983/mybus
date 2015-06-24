/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_event.cc
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/05/15 15:41:13
 * @version $Revision$
 * @brief 
 *  
 **/
#include "bus_event.h"
#include "mydecimal.h"
namespace bus {
/*
 * @brief 解析table map event中meta数据
 */
uint16_t read_meta(bus_mem_block_t &buf, uint32_t &pos, uint8_t datatype)
{
    uint16_t meta = 0;
    if (datatype == MYSQL_TYPE_FLOAT ||
            datatype == MYSQL_TYPE_DOUBLE ||
            datatype == MYSQL_TYPE_BLOB ||
            datatype == MYSQL_TYPE_GEOMETRY){
        meta = (uint16_t)(uint8_t)buf.at(pos);
        pos += 1;
    } else if(datatype == MYSQL_TYPE_NEWDECIMAL ||
            datatype == MYSQL_TYPE_VAR_STRING ||
            datatype == MYSQL_TYPE_STRING) {
        meta = ((uint16_t)(uint8_t)buf.at(pos) << 8) | ((uint16_t)(uint8_t)buf.at(pos + 1));
        pos += 2;
    } else if (datatype == MYSQL_TYPE_VARCHAR) {
        meta = uint2korr(buf.block + pos);
        pos += 2;
    }else if (datatype == MYSQL_TYPE_BIT) {
        meta = ((uint16_t)(uint8_t)buf.at(pos + 1) << 8) | ((uint16_t)(uint8_t)buf.at(pos));
        pos += 2;
    }else if (datatype == MYSQL_TYPE_DECIMAL ||
            datatype == MYSQL_TYPE_TINY ||
            datatype == MYSQL_TYPE_SHORT ||
            datatype == MYSQL_TYPE_LONG ||
            datatype == MYSQL_TYPE_NULL ||
            datatype == MYSQL_TYPE_TIMESTAMP ||
            datatype == MYSQL_TYPE_LONGLONG ||
            datatype == MYSQL_TYPE_INT24 ||
            datatype == MYSQL_TYPE_DATE ||
            datatype == MYSQL_TYPE_TIME ||
            datatype == MYSQL_TYPE_DATETIME ||
            datatype == MYSQL_TYPE_YEAR) {
    }else {
        g_logger.error("[datatype=%d] can't exist in the binlog");
    }

    return meta;
}
/*
 * @brief 解析压缩过的整型数据 
 */
bool unpack_int(char *p, uint32_t &npos, uint64_t &value){
    uint8_t type = p[0];
    if(type < 0xfb){
        npos++;
        value = type;
    }else if(type == 0xfc){
        npos += 2;
        value = uint2korr(p);
    }else if(type == 0xfd){
        npos += 3;
        value = uint3korr(p);
    }else if(type == 0xfe){
        npos += 8;
        value = uint8korr(p);
    }else {
        g_logger.error("unkown integer type");
        return false;
    }

    return true;
}
/*
 * 解析event中的column数据
 */
bool parse_column_value(char *buf,
        uint8_t type,
        uint16_t meta,
        uint32_t &pos,
        row_t *row,
        bool is_old,
        convert_charset_t &convert,
        column_t *col)
{
    uint32_t length= 0;
    //转换类型，将MYSQL_TYPE_STRING类型转化为具体的类型
    if (type == MYSQL_TYPE_STRING)
    {
        if (meta >= 256)
        {
            uint8_t byte0= meta >> 8;
            uint8_t byte1= meta & 0xFF;

            if ((byte0 & 0x30) != 0x30)
            {
                /* a long CHAR() field: see #37426 */
                length= byte1 | (((byte0 & 0x30) ^ 0x30) << 4);
                type= byte0 | 0x30;
            }
            else
            {
                switch (byte0)
                {
                    case MYSQL_TYPE_SET:
                    case MYSQL_TYPE_ENUM:
                    case MYSQL_TYPE_STRING:
                        type = byte0;
                        length = byte1;
                        break;
                    default:
                        g_logger.error("don't know how to handle column type=%d meta=%d, byte0=%d, byte1=%d", 
                                type, meta, byte0, byte1);
                        return false;
                }
            }
        } else {
            length= meta;
        }
    }

    //根据具体的类型, 以及元数据进行转化
    int col_unsigned = -1;
    if (col != NULL) col_unsigned = col->get_column_type();
    char value[256];
    switch(type)
    {
        case MYSQL_TYPE_LONG: //int32
            {
                if (col_unsigned == 1) {
                    uint32_t i = uint4korr(buf); 
                    snprintf(value, sizeof(value), "%u", i);
                } else {
                    int32_t i = sint4korr(buf); 
                    snprintf(value, sizeof(value), "%d", i);
                }
                pos += 4;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_TINY:
            {
                if (col_unsigned == 1) {
                    uint8_t c = (uint8_t)(*buf);
                    snprintf(value, sizeof(value), "%hhu", c);
                } else {
                    int8_t c = (int8_t)(*buf);
                    snprintf(value, sizeof(value), "%hhd", c);
                }
                pos += 1;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_SHORT:
            {
                if (col_unsigned == 1) {
                    uint16_t s = uint2korr(buf);
                    snprintf(value, sizeof(value), "%hu", s);
                } else {
                    int16_t s = sint2korr(buf);
                    snprintf(value, sizeof(value), "%hd", s);
                }
                pos += 2;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_LONGLONG:
            {
                if (col_unsigned == 1) {
                    int64_t ll = uint8korr(buf);
                    snprintf(value, sizeof(value), "%lu", ll);
                } else {
                    uint64_t ll = sint8korr(buf);
                    snprintf(value, sizeof(value), "%ld", ll);
                }
                pos += 8;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_INT24:
            {
                int32_t i;
                if (col_unsigned == 1) {
                    i = uint3korr(buf); 
                } else {
                    i = sint3korr(buf); 
                }
                pos += 3;
                snprintf(value, sizeof(value), "%d", i);
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_TIMESTAMP:
            {
                uint32_t i = uint4korr(buf);
                pos += 4;
                snprintf(value, sizeof(value), "%u", i);
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_DATETIME:
            {
                uint64_t ll = uint8korr(buf);
                uint32_t d = ll / 1000000;
                uint32_t t = ll % 1000000;
                snprintf(value, sizeof(value),
                        "%04d-%02d-%02d %02d:%02d:%02d",
                        d / 10000, (d % 10000) / 100, d % 100,
                        t / 10000, (t % 10000) / 100, t % 100);
                pos += 8;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_TIME:
            {
                uint32_t i32= uint3korr(buf);
                snprintf(value,
                        sizeof(value),
                        "%02d:%02d:%02d",
                        i32 / 10000, (i32 % 10000) / 100, i32 % 100);
                pos += 3;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_NEWDATE:
            {
                uint32_t tmp= uint3korr(buf);
                int part;
                char sbuf[11];
                char *spos= &sbuf[10];  // start from '\0' to the beginning

                /* Copied from field.cc */
                *spos--=0;                 // End NULL
                part=(int) (tmp & 31);
                *spos--= (char) ('0'+part%10);
                *spos--= (char) ('0'+part/10);
                *spos--= ':';
                part=(int) (tmp >> 5 & 15);
                *spos--= (char) ('0'+part%10);
                *spos--= (char) ('0'+part/10);
                *spos--= ':';
                part=(int) (tmp >> 9);
                *spos--= (char) ('0'+part%10); part/=10;
                *spos--= (char) ('0'+part%10); part/=10;
                *spos--= (char) ('0'+part%10); part/=10;
                *spos=   (char) ('0'+part);
                
                snprintf(value, sizeof(value), "%s", sbuf);
                pos += 3;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_DATE:
            {
                uint32_t i32 = uint3korr(buf);
                snprintf(value,
                        sizeof(value),
                        "%04d-%02d-%02d",
                       (int32_t)(i32 / (16L * 32L)), (int32_t)(i32 / 32L % 16L), (int32_t)(i32 % 32L));
                pos += 3;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_YEAR:
            {
                uint32_t i = (uint32_t)(uint8_t)*buf;
                snprintf(value, sizeof(value), "%04d", i + 1900);
                pos += 1;
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_ENUM:
            {
                switch (length) {
                    case 1:
                        snprintf(value, sizeof(value), "%d", (int32_t)*buf);
                        pos += 1;
                        row->push_back(value, is_old);
                        break;
                    case 2:
                        {
                            int32_t i32 = uint2korr(buf);
                            snprintf(value, sizeof(value) ,"%d", i32);
                            pos += 2;
                            row->push_back(value, is_old);
                        }
                        break;
                    default:
                        g_logger.error("unknown enum packlen=%d", length);
                        return false;
                }
            }
            break;
        case MYSQL_TYPE_SET:
            {
                pos += length;
                row->push_back(buf, length, is_old);
            }
            break;
        case MYSQL_TYPE_BLOB:
            switch (meta) 
            {
                case 1:     //TINYBLOB/TINYTEXT
                    {
                        length = (uint8_t)(*buf);
                        pos += length + 1;
                        row->push_back(buf + 1, length, is_old);
                    }
                    break;
                case 2:     //BLOB/TEXT
                    {
                        length = uint2korr(buf);
                        pos += length + 2;
                        row->push_back(buf + 2, length, is_old);
                    }
                    break;
                case 3:     //MEDIUMBLOB/MEDIUMTEXT
                    {
                        length = uint3korr(buf);
                        pos += length + 3;
                        row->push_back(buf + 3, length, is_old);
                    }
                    break;
                case 4:     //LONGBLOB/LONGTEXT
                    {
                        length = uint4korr(buf);
                        pos += length + 4;
                        row->push_back(buf + 4, length, is_old);
                    }
                    break;
                default:
                    g_logger.error("unknown blob type=%d", meta);
                    return false;
            }
            break;
        case MYSQL_TYPE_VARCHAR:
        case MYSQL_TYPE_VAR_STRING:
            {
                length = meta;
                if (length < 256){
                    length = (uint8_t)(*buf);
                    pos += 1 + length;
                    if (!convert.convert_charset(buf + 1, length, row, is_old))
                    {
                        g_logger.error("parse string column value fail, convert_charset fail");
                        return true;
                    }
                    //row->push_back(buf + 1, length, is_old);
                } else {
                    length = uint2korr(buf);
                    pos += 2 + length;
                    if (!convert.convert_charset(buf + 2, length, row, is_old))
                    {
                        g_logger.error("parse string column value fail, convert_charset fail");
                        return true;
                    }
                    //row->push_back(buf + 2, length, is_old);
                }
            }
            break;
        case MYSQL_TYPE_STRING:
            {
                if (length < 256){
                    length = (uint8_t)(*buf);
                    pos += 1 + length;
                    //row->push_back(buf + 1, length, is_old);
                    if (!convert.convert_charset(buf + 1, length, row, is_old))
                    {
                        g_logger.error("parse string column value fail, convert_charset fail");
                        return true;
                    }
                }else{
                    length = uint2korr(buf);
                    pos += 2 + length;
                    //row->push_back(buf + 2, length, is_old);
                    if (!convert.convert_charset(buf + 2, length, row, is_old))
                    {
                        g_logger.error("parse string column value fail, convert_charset fail");
                        return true;
                    }
                }
            }
            break;
        case MYSQL_TYPE_BIT:
            {
                uint32_t nBits= ((meta >> 8) * 8) + (meta & 0xFF);
                length= (nBits + 7) / 8;
                pos += length;
                row->push_back(buf, length, is_old);
            }
            break;
        case MYSQL_TYPE_FLOAT:
            {
                float fl;
                float4get(fl, buf);
                pos += 4;
                snprintf(value, sizeof(value), "%f", (double)fl);
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_DOUBLE:
            {
                double dbl;
                float8get(dbl,buf);
                pos += 8;
                snprintf(value, sizeof(value), "%f", dbl);
                row->push_back(value, is_old);
            }
            break;
        case MYSQL_TYPE_NEWDECIMAL:
            {
                uint8_t precision= meta >> 8;
                uint8_t decimals= meta & 0xFF;
                int bin_size = my_decimal_get_binary_size(precision, decimals);
                my_decimal dec;
                binary2my_decimal(E_DEC_FATAL_ERROR, (unsigned char*) buf, &dec, precision, decimals);
                int i, end;
                char buff[512], *spos;
                spos = buff;
                spos += sprintf(buff, "%s", dec.sign() ? "-" : "");
                end= ROUND_UP(dec.frac) + ROUND_UP(dec.intg)-1;
                for (i=0; i < end; i++)
                    spos += sprintf(spos, "%09d.", dec.buf[i]);
                spos += sprintf(spos, "%09d", dec.buf[i]);
                row->push_back(buff, is_old);
                pos += bin_size;
            }
            break;
        default:
            g_logger.error("don't know how to handle type =%d, meta=%d column value",
                    type, meta);
            return false;
    }
    return true;
}
/**************************************************/
int32_t bus_repl_t::start_repl(bool &stop_flag)
{
    int32_t ret = dump_cmd.write_packet(fd);
    if (ret != 0) {
        g_logger.error("write dump packe fail");
        return 2;
    }
    
    while(!stop_flag)
    {
        ret = pack.read_packet(fd);
        if (ret != 0) {
            /* read error, or eof */
            return 2;
        }

        ret = pack.parse_packet();
        if (ret == -1) {
            /* parse packet fail */
            return -1;
        } else if (ret == 1) {
            /* stop signal */
            return 1;
        } else if(ret == 2) {
            /* eof */
            return 2;
        }
    }
    return 1;
}
/**********************************************************/
bus_packet_t::bus_packet_t(uint32_t statid,
        const char *filename,
        uint64_t offset,
        uint64_t rowindex,
        uint64_t cmdindex,
        data_source_t *src_source,
        uint32_t src_index):
    _bodylen(0),_seqid(0),
    event_pack(statid, src_source, src_index, filename,
            offset, rowindex, cmdindex)
{
    _body = (char*)malloc(PACKET_NORMAL_SIZE);
    if (_body == NULL) oom_error();
    _body_mem_sz = PACKET_NORMAL_SIZE;
}
bus_packet_t::~bus_packet_t()
{
    if (_body != NULL)
    {
        free(_body);
        _body = NULL;
    }
    _body_mem_sz = 0;
}
void bus_packet_t::_add_mem(uint32_t sz)
{
    if (_bodylen + sz > _body_mem_sz)
    {
        char *temp = (char *)realloc(_body, _bodylen + sz);
        if (temp == NULL) oom_error();
        _body = temp;
        _body_mem_sz = _bodylen + sz;
    }
}
int bus_packet_t::read_packet(int fd)
{
    int cur_payload_sz = 0xffffff;
    _bodylen = 0;
    int bytes_ct, ret;
    while (cur_payload_sz == 0xffffff)
    {
        /* 读取packet head */
        char buf[4];
        bytes_ct = 0;
        while (bytes_ct != 4) {
            ret = read(fd, buf + bytes_ct, 4 - bytes_ct);
            if (ret == 0) {
                g_logger.notice("read socket eof");
                return -1;
            } else if (ret == -1) {
                g_logger.error("read packet head fail, error:%s", strerror(errno));
                return -1;
            } else {
                bytes_ct += ret;
            }
        }

        cur_payload_sz = uint3korr(buf);
        /* 分配空间 */
        _add_mem(cur_payload_sz);
        /*读取body */
        bytes_ct = 0;
        while(bytes_ct != cur_payload_sz) {
            ret = read(fd, _body + _bodylen + bytes_ct, cur_payload_sz - bytes_ct);
            if (ret == 0) {
                g_logger.notice("read socket eof");
                return -1;
            } else if (ret == -1) {
                g_logger.error("read packet body fail, error:%s", strerror(errno));
                return -1;
            } else {
                //g_logger.notice("sz=%d,count=%d", cur_payload_sz, ret);
                bytes_ct += ret;
            }
        }
        _bodylen += cur_payload_sz;
    }
    return 0;
}
int32_t bus_packet_t::parse_packet()
{
    uint32_t pos = 0;
    uint8_t pack_type = _body[0];
    pos += 1;
    
    int ret;
    if (pack_type == 0x00)
    {
        ret = event_pack.parse_event(_body, pos, _bodylen);
        if (ret == -1) {
            g_logger.error("parse event fail");
            return -1;
        } else if (ret == 1) {
            g_logger.notice("recv stop signal");
            return 1;
        }
        if (pos != _bodylen) {
            g_logger.error("parse event fail, pos %u != bodylen %u",
                    pos, _bodylen);
            return -1;
        }
        assert(pos == _bodylen);
    } else if (pack_type == 0xff) {
        ret = error_pack.parse(_body, pos, _bodylen); 
        if (ret != 0)
        {
            g_logger.error("parse error packet fail");
            return -1;
        }
        error_pack.print_info();
        assert(pos == _bodylen);
        return -1;
    } else if (pack_type == 0xfe) {
        g_logger.notice("read eof packet");
        return 2;
    }
    return 0;
}
/***********************************************************/
int32_t bus_error_packet_t::parse(char *buf, uint32_t &pos, uint32_t pack_len)
{
    _errcode = uint2korr(buf + pos);
    pos += 2;
    memcpy(_state, buf + pos, sizeof(_state));
    pos += sizeof(_state);
    
    uint32_t left = pack_len - pos;
    _info.assign(buf + pos, left);
    pos += left;
    return 0;
}
/***********************************************************/
int32_t bus_dump_cmd_t::write_packet(int fd)
{
    char buf[1024];
    uint32_t bodysz = 0;
    uint32_t offset = 4;
    buf[offset] = _cmd;
    bodysz += 1;
    int4store(buf + offset + bodysz, _binlog_pos);
    bodysz += 4;
    int2store(buf + offset + bodysz, _flags);
    bodysz += 2;
    int4store(buf + offset + bodysz, _server_id);
    bodysz += 4;

    std::size_t binlog_filename_len = _binlog_filename.size();
    memcpy(buf + offset + bodysz, _binlog_filename.c_str(), binlog_filename_len);
    bodysz += binlog_filename_len;
    
    /* set seq id */
    buf[offset - 1] = 0x00;

    int3store(buf, bodysz);

    int ret = write(fd, buf, bodysz + 4);
    if (ret != int(bodysz + 4))
    {
        g_logger.error("write dump cmd packet fail, error:%s", strerror(errno));
        return -1;
    }
    return 0;
}
/**********************************************************/
void bus_event_head_t::parse_event_head(char *buf, uint32_t &pos)
{
    time_stamp = uint4korr(buf + pos);
    pos += 4;
    event_type = buf[pos];
    pos += 1;
    server_id = uint4korr(buf + pos);
    pos += 4;
    event_sz = uint4korr(buf + pos);
    pos += 4;
    log_pos = uint4korr(buf + pos);
    pos += 4;
    flags = uint2korr(buf + pos);
    pos += 2;
}
/**********************************************************/
bool bus_event_t::init_event_schema_charset()
{
    //找到和当前map_event 匹配的schema
    bus_config_t &config = g_bus.get_config();
    std::string& schema_name = map_event.get_schema_name();
    std::string& table_name = map_event.get_table_name();
    map_schema = config.get_match_schema(schema_name.c_str(),
            table_name.c_str());
    if (map_schema == NULL)
    {
        map_charset = NULL;
        return true;
    }
    char key_charset[64];
    snprintf(key_charset, sizeof(key_charset), "%s.%s", schema_name.c_str(),
            table_name.c_str());
    map_charset = src_source->get_table_charset(key_charset);
    if (map_charset == NULL)
    {
        std::vector<db_object_id> dbvec;
        db_object_id id(schema_name.c_str(), table_name.c_str());
        dbvec.push_back(id);
        if (!src_source->check_struct(map_schema, dbvec))
        {
            g_logger.error("%s.%s check table struct fail",
                    schema_name.c_str(), table_name.c_str());
            return false;
        }

        if (!src_source->construct_hash_table())
        {
            g_logger.error("%s.%s construct hash table fail",
                    schema_name.c_str(), table_name.c_str());
            map_charset = NULL;
            return false;
        }
        map_charset = src_source->get_table_charset(key_charset);
    }
    if (map_charset == NULL)
    {
        g_logger.error("%s.%s get charset fail",
                schema_name.c_str(), table_name.c_str());
        return false;
    }

    const char *target_charset = config.get_charset();
    if (map_charset != NULL && target_charset != NULL)
    {
        if (!map_convert.assign_charset(map_charset->c_str(), target_charset))
        {
            g_logger.error("assign %s to %s fail", map_charset->c_str(),
                    target_charset);
            return false;
        }
    }
    return true;
}
int32_t bus_event_t::parse_event(char *buf, uint32_t &pos, uint32_t packlen)
{
    assert(packlen >= 19);
    this->pack_len = packlen;
    event_head.parse_event_head(buf, pos);
    int ret = -1;
    switch(event_head.event_type)
    {
        case 0x00:
            g_logger.debug("unknown event");
            return -1;
            break;
        case 0x04:
            g_logger.debug("rotate event");
            if (rotate_event.parse_rotate_event_body(buf, pos, *this))
            {
                g_logger.error("parse rotate event error");
                return -1;
            }
            break;
        case 0x0f:
            g_logger.debug("format description event");
            if(format_event.parse_format_event_body(buf, pos, *this))
            {
                g_logger.error("parse format event error");
                return -1;
            }
            break;
        case 0x13:
            {
                std::string &binlog_name = rotate_event.get_binlog_filename();
                uint64_t offset = event_head.get_event_pos();
                g_logger.debug("table map event, [%s:%lu]", binlog_name.c_str(),
                        offset);
                if (map_event.parse_map_event_body(buf, pos, *this))
                {
                    g_logger.error("parse map event error");
                    return -1;
                }
                map_position.set_position(binlog_name.c_str(), offset, 0, 0);
                if (!init_event_schema_charset())
                {
                    g_logger.error("init map event charset fail");
                    return -1;
                }
            }
            break;
        case 0x17:
            {
                g_logger.debug("write rows event v1, [%s:%lu:%lu]", 
                        map_position.binlog_file, map_position.binlog_offset,
                        map_position.rowindex);
                row_event.init(0x17, 1);
                ret = row_event.parse_row_event_body(buf, pos, *this);
                if (ret == -1)
                {
                    g_logger.error("parse write event fail");
                    return -1;
                } else if (ret == 1)
                {
                    g_logger.notice("write row event, recv stop signal");
                    return 1;
                }
            }
            break;
        case 0x18:
            {
                g_logger.debug("update rows event v1, [%s:%lu:%lu]", 
                        map_position.binlog_file, map_position.binlog_offset,
                        map_position.rowindex);
                row_event.init(0x18, 1);
                ret = row_event.parse_row_event_body(buf, pos, *this);
                if (ret == -1)
                {
                    g_logger.error("parse write event fail");
                    return -1;
                } else if (ret == 1)
                {
                    g_logger.notice("update row  event, recv stop signal");
                    return 1;
                }
            }
            break;
        case 0x19:
            {
                g_logger.debug("delete rows event v1, [%s:%lu:%lu]", 
                        map_position.binlog_file, map_position.binlog_offset,
                        map_position.rowindex);
                row_event.init(0x19, 1);
                ret = row_event.parse_row_event_body(buf, pos, *this);
                if (ret == -1)
                {
                    g_logger.error("parse delete event fail");
                    return -1;
                } else if (ret == 1)
                {
                    g_logger.notice("delete row event, recv stop signal");
                    return 1;
                }
            }
            break;
        default:
            //g_logger.debug("other event:%hhu", event_head.event_type);
            pos = packlen;
    }
    return 0;
}
/***********************************************************/
int32_t bus_format_event_t::parse_format_event_body(char *buf, 
        uint32_t &pos, bus_event_t &cur_event)
{
    _binlog_ver = uint2korr(buf + pos);
    pos += 2;
    
    uint32_t sz = sizeof(_serv_ver) - 1;
    strncpy(_serv_ver, buf + pos, sz);
    _serv_ver[sz] = '\0';
    pos += sz;

    _time_stamp = uint4korr(buf + pos);
    pos += 4;

    _head_len = buf[pos];
    pos += 1;

    int left = cur_event.pack_len - pos;
    _post_head_len_str.assign(buf + pos, left);
    pos += left;

    return 0;
}
/***********************************************************/
int32_t bus_rotate_event_t::parse_rotate_event_body(char *buf,
        uint32_t &pos, bus_event_t &cur_event)
{
    _next_pos = uint8korr(buf + pos);
    pos += 8;
    int left = cur_event.pack_len - pos;
    _next_binlog_filename.assign(buf + pos, left);
    pos += left;
    return 0;
}
/***********************************************************/
int32_t bus_table_map_t::parse_map_event_body(char *buf,
        uint32_t &pos, bus_event_t &cur_event)
{
    uint8_t post_head_len = cur_event.get_event_posthead_len(_cmd);
    if (post_head_len == 8)
    {
        _table_id = uint6korr(buf + pos);
        pos += 6;
    } else {
        _table_id = uint4korr(buf + pos);
        pos += 4;
    }
    _flags = uint2korr(buf + pos);
    pos += 2;

    _schema_name_length = buf[pos];
    pos += 1;
    _schema_name.assign(buf + pos, _schema_name_length);
    pos += _schema_name_length;
    pos += 1;

    _table_name_length = buf[pos];
    pos +=1;
    _table_name.assign(buf + pos, _table_name_length);
    pos += _table_name_length;
    pos += 1;

    if(!unpack_int(buf + pos, pos, _column_count))
    {
        g_logger.error("read column count fail");
        return -1;
    }
    _column_type_def.assign(buf + pos, _column_count);
    pos += _column_count;
    //读取column_meta
    if (!unpack_int(buf + pos, pos, _column_meta_length))
    {
        g_logger.error("read column meta length fail");
        return -1;
    }
    _column_meta_def.assign(buf + pos, _column_meta_length);
    pos += _column_meta_length;

    //解析column_meta
    _meta_vec.clear();
    uint32_t meta_pos = 0;
    for (uint32_t i = 0; i < _column_count; ++i)
    {
        uint16_t meta = read_meta(_column_meta_def, meta_pos, (uint8_t)_column_type_def.at(i));
        _meta_vec.push_back(meta);
    }
    assert(meta_pos == _column_meta_length);

    uint64_t null_bitmap_length = (_column_count + 7) >> 3;
    _null_bitmap.assign(buf + pos, null_bitmap_length);
    pos += null_bitmap_length;
    
    return 0;
}
/***********************************************************/
int32_t bus_row_event_t::parse_row_event_body(char *buf,
        uint32_t &pos,
        bus_event_t &cur_event)
{
    bus_engine_t &engine = g_bus.get_engine();
    bus_stat_t &stat = g_bus.get_stat();
    bus_config_t &config = g_bus.get_config();
    source_t dst_type = config.get_dstds_type();
    std::vector<mysql_position_t*> &all_pos = config.get_all_pos();
    long target_size = config.get_target_size();
    uint8_t post_head_len = cur_event.get_event_posthead_len(_cmd);
    if (post_head_len == 6)
    {
        _table_id = uint4korr(buf + pos);
        pos += 4;
    } else {
        _table_id = uint6korr(buf + pos);
        pos += 6;
    }

    _flags = uint2korr(buf + pos);
    pos += 2;

    if (_ver == 2)
    {
        _extra_data_length = uint2korr(buf + pos);
        pos += 2;
        _extra_data.assign(buf + pos, _extra_data_length);
        pos += _extra_data_length;
    }

    if(!unpack_int(buf + pos, pos, _column_count))
    {
        g_logger.error("parse column count fail");
        return -1;
    }
    
    uint64_t _present_bitmap_len = (_column_count + 7) >> 3;
    //读取present bitmap
    _present_bitmap1.assign(buf + pos, _present_bitmap_len);
    pos += _present_bitmap_len;
    //解析present bitmap
    uint32_t present_ct1 = _present_bitmap1.get_bitset_count(_column_count);
    uint32_t null_bitmap1_size = (present_ct1 + 7) >> 3;
    
    uint32_t null_bitmap2_size = 0;
    if (_cmd == 0x18)
    {
        _present_bitmap2.assign(buf + pos, _present_bitmap_len);
        pos += _present_bitmap_len;
        uint32_t present_ct2 = _present_bitmap2.get_bitset_count(_column_count);
        null_bitmap2_size = (present_ct2 + 7) >> 3;
    }
    
    schema_t *match_schema = cur_event.map_schema;
    if (match_schema == NULL)
    {
        //g_logger.debug("[%s.%s] don't match, ignore row",
        //        cur_event.get_map_schema_name().c_str(),
        //        cur_event.get_map_table_name().c_str());
        pos =  cur_event.pack_len;
        return 0;
    }
    long schema_id = match_schema->get_schema_id();

    std::string *pcharset = cur_event.map_charset;
    if (pcharset == NULL)
    {
        g_logger.error("[%s.%s] get charset fail",
                cur_event.get_map_schema_name().c_str(),
                cur_event.get_map_table_name().c_str());
        return -1;
    }
    uint64_t row_ct = cur_event.map_position.rowindex;
    const int src_port = cur_event.src_source->get_port();
    while(pos < cur_event.pack_len)
    {
        row_ct = cur_event.map_position.rowindex;
        row_t *row = new(std::nothrow)row_t(_column_count);
        if (row == NULL) oom_error();
        row->set_position(cur_event.map_position.binlog_file,
                cur_event.map_position.binlog_offset,
                cur_event.map_position.rowindex,
                cur_event.map_position.cmdindex);
        row->set_table(cur_event.get_map_table_name().c_str());
        row->set_src_port(src_port);
        row->set_src_partnum(cur_event.src_partnum);
        row->set_instance_num(target_size);
        row->set_src_schema(match_schema);
        row->set_column_pnamemap(match_schema->get_column_namemap());
        row->set_db(cur_event.get_map_schema_name().c_str());
        row->set_schema_id(schema_id);
        row->set_dst_type(dst_type);

        if (_cmd == 0x17) {
            row->set_action(INSERT);
            if (!parse_row(buf, pos, null_bitmap1_size, row, _present_bitmap1, false, cur_event))
            {
                g_logger.error("parse [%s:%lu:%lu] row fail",
                        cur_event.map_position.binlog_file,
                        cur_event.map_position.binlog_offset,
                        row_ct);
                return -1;
            }
        } else if (_cmd == 0x19) {
            row->set_action(DEL);
            if (!parse_row(buf, pos, null_bitmap1_size, row, _present_bitmap1, false, cur_event))
            {
                g_logger.error("parse [%s:%lu:%lu] row fail",
                        cur_event.map_position.binlog_file,
                        cur_event.map_position.binlog_offset,
                        row_ct);
                return -1;
            }

        }else if(_cmd == 0x18) {
            row->set_action(UPDATE);
            if (!parse_row(buf, pos, null_bitmap1_size, row, _present_bitmap1, true, cur_event))
            {
                g_logger.error("parse [%s:%lu:%lu] row fail",
                        cur_event.map_position.binlog_file,
                        cur_event.map_position.binlog_offset,
                        row_ct);
                return -1;
            }

            if (!parse_row(buf, pos, null_bitmap2_size, row, _present_bitmap2, false, cur_event)) {
                g_logger.error("parse [%s:%lu:%lu] row fail",
                        cur_event.map_position.binlog_file,
                        cur_event.map_position.binlog_offset,
                        row_ct);
                return -1;
            }
        } else {
            g_logger.error("unsupport command=%hhu", _cmd);
            return -1;
        }

        cur_event.map_position.incr_rowindex();
        /* 调用处理逻辑对row进行处理 */
        int ret = g_bus.process(row);
        if (ret == PROCESS_FATAL) {
            std::string row_info;
            row->print(row_info);
            g_logger.error("process [%s:%lu:%lu] row fatal, rows=%s",
                        cur_event.map_position.binlog_file,
                        cur_event.map_position.binlog_offset,
                        row_ct, row_info.c_str());
            delete row;
            return -1;
        } else if (ret == PROCESS_IGNORE) {
            std::string row_info;
            row->print(row_info);
            g_logger.debug("process [%s:%lu:%lu] row fail, rows=%s",
                        cur_event.map_position.binlog_file,
                        cur_event.map_position.binlog_offset,
                        row_ct, row_info.c_str());
            delete row;
            stat.incr_producer_error_count(cur_event.statid, 1);
            continue;
        }
        //如果add失败，则说明线程池被关闭，stop
        ret = engine.add_row_copy(row, all_pos, cur_event.statid);
        if (ret == ROW_NUM_INVALID) {
            std::string row_info;
            row->print(row_info);
            g_logger.error("add row fail, partnum invalid, row=%s", row_info.c_str());
            delete row;
            row = NULL;
            return -1;
        } else if (ret == ROW_ADD_STOP) {
            g_logger.notice("recv stop signal, add row stop");
            delete row;
            row = NULL;
            return 1;
        } else if (ret == ROW_IGNORE_OK) {
            delete row;
            row = NULL;
        }
        /* //bug resloved
        } else {
            stat.incr_producer_succ_count(cur_event.statid, 1);
        }
        */
    }
    return 0;
}

bool bus_row_event_t::parse_row(char *buf,
        uint32_t &pos,
        uint32_t null_bitmap_size,
        row_t *row,
        bus_mem_block_t &present_bitmap,
        bool is_old,
        bus_event_t &cur_event)
{
    bus_table_map_t& map = cur_event.map_event;
    convert_charset_t& convert = cur_event.map_convert;

    _null_bitmap.assign(buf + pos, null_bitmap_size);
    pos += null_bitmap_size;
    /* 构造临时行数据结构，存储解析后的数据 */
    row_t *temp_row = new(std::nothrow) row_t(_column_count);
    if (temp_row == NULL) oom_error();
    
    schema_t *curschema = cur_event.map_schema;
    uint32_t present_pos = 0;
    for (uint32_t i = 0; i < _column_count; ++i)
    {
        if (present_bitmap.get_bit(i)) {
            bool null_col = _null_bitmap.get_bit(present_pos);
            if (null_col) {
                temp_row->push_back(NULL, false);
            } else {
                uint8_t type = map.get_column_type(i);
                uint16_t meta = map.get_column_meta(i);
                column_t *col = curschema->get_column_byseq(i);
                if (!parse_column_value(buf + pos, type, meta, pos, temp_row, false,
                            convert, col))
                {
                    g_logger.error("parse %d column value fail", i);
                    delete temp_row;
                    temp_row = NULL;
                    return false;
                }
            }
            ++present_pos;
        } else {
            temp_row->push_back(NULL, false);
        }
    }
    
    /* 将解析后的数据按照顺序放入目标行 */
    std::vector<column_t*>& cols = curschema->get_columns();
    char *ptr;
    for(std::vector<column_t*>::iterator it = cols.begin();
            it != cols.end();
            ++it)
    {
        column_t *curcolumn = *it;
        int32_t curseq = curcolumn->get_column_seq();
        if (!temp_row->get_value(curseq, &ptr))
        {
            g_logger.error("get value sequence %d fail", curseq);
            delete temp_row;
            temp_row = NULL;
            return false;
        }
        row->push_back(ptr, is_old);
    }
    
    /* 释放临时行数据结构占用的内存 */
    if (temp_row != NULL)
    {
        delete temp_row;
        temp_row = NULL;
    }
    return true;
}

}//namespace bus
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
