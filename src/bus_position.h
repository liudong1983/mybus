/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file bus_position.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/06/23 12:34:46
 * @version $Revision$
 * @brief 
 *  
 **/
#ifndef  __BUS_POSITION_H_
#define  __BUS_POSITION_H_
#include<string>
#include "bus_log.h"
namespace bus {

class mysql_position_t
{
    public:
        mysql_position_t()
        {
            binlog_file[0] = '\0';
        }
        mysql_position_t(const mysql_position_t &other)
        {
            if (other.binlog_file[0] != '\0') {
                snprintf(binlog_file, sizeof(binlog_file), "%s", other.binlog_file);
                file_seq = other.file_seq;
                binlog_offset = other.binlog_offset;
                rowindex = other.rowindex;
                cmdindex = other.cmdindex;
            } else {
                binlog_file[0] = '\0';
            }
        }

        bool empty()
        {
            return binlog_file[0] == '\0' ? true : false;
        }
        
        void reset_position()
        {
            /*
            pthread_t pid = pthread_self();
            g_logger.debug("[%ld] reset position", pid);
            */
            binlog_file[0] = '\0';
        }

        bool set_position(const char *fileindex, const char *offset,
                const char *rowseq, const char*cmdseq, const char *prefix)
        {
            long fileseq_num, fileoffset_num, rowseq_num, cmdseq_num;
            if (!str2num(fileindex, fileseq_num) ||
                    !str2num(offset, fileoffset_num) ||
                    !str2num(rowseq, rowseq_num) ||
                    !str2num(cmdseq, cmdseq_num))
            {
                g_logger.error("[%s:%s:%s] is invalid", fileindex, offset, rowseq);
                return false;
            }
            file_seq = fileseq_num;
            binlog_offset = fileoffset_num;
            rowindex = rowseq_num;
            cmdindex = cmdseq_num;
            snprintf(binlog_file, sizeof(binlog_file), "%s.%06ld", prefix, file_seq);
            return true;
        }


        bool set_position(const char *binfile, long offset, long rowseq, long cmdseq)
        {
            snprintf(binlog_file, sizeof(binlog_file), binfile);
            binlog_offset = offset;
            rowindex = rowseq;
            cmdindex = cmdseq;

            std::string filename(binlog_file);
            std::size_t sz = filename.size();
            std::size_t sep_pos = filename.rfind('.');
            if (sep_pos == std::string::npos)
            {
                g_logger.error("[binlog_filename=%s] is invalid", binlog_file);
                return false;
            }

            int64_t seq_start = -1;
            for (std::size_t index = sep_pos + 1; index < sz; ++index)
            {
                if (filename[index] < '0' || filename[index] > '9')
                {
                    g_logger.error("[binlog_filename=%s] is invalid", binlog_file);
                    return false;
                }
                if (filename[index] != '0' && seq_start == -1) seq_start = index;
            }
            if (seq_start == -1)
            {
                file_seq = 0;
                return true;
            }

            std::string seq = filename.substr(seq_start, sz - seq_start);
            bool ret = str2num(seq.c_str(), file_seq);
            if (!ret)
            {
                g_logger.error("[binlog_filename=%s] is invalid", binlog_file);
                return false;
            }
            return true;
        }

        void incr_rowindex()
        {
            ++rowindex;
        }
        
        void set_cmdindex(long index)
        {
            cmdindex = index;
        }
        int compare_position(mysql_position_t &other)
        {
            return compare_position(other.binlog_file, other.binlog_offset, other.rowindex, other.cmdindex);
        }
        int compare_position(const char *filename, long offset, long rowseq, long cmdseq)
        {
            int ret = strcasecmp(binlog_file, filename);
            if (ret > 0)
            {
                return 1;
            } else if (ret == 0) 
            {
                if (binlog_offset > offset)
                {
                    return 1;
                } else if (binlog_offset == offset)
                {
                    if (rowindex > rowseq)
                    {
                        return 1;
                    } else if (rowindex == rowseq){
                        //return 0;
                        if (cmdindex > cmdseq)
                        {
                            return 1;
                        } else if (cmdindex == cmdseq) {
                            return 0;
                        }
                    }
                }
            }
            return -1;
        }
    public:
        char binlog_file[64];
        long binlog_offset;
        long rowindex;
        //cmd index add by zhangbo6, for 1->n->m
        long cmdindex;
        long file_seq;
};
}//namespace bus

#endif  //__BUS_POSITION_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
