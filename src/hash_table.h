/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file hash_table.h
 * @author liudong2(liudong2@staff.sina.com.cn)
 * @date 2014/04/21 19:57:42
 * @version $Revision$
 * @brief 
 *  
 **/



#ifndef  __HASH_TABLE_H_
#define  __HASH_TABLE_H_
#include <stdio.h>
#include <string.h>
#include <vector>

namespace bus{

extern void oom_error();

static uint32_t hashcode(const char *buf)
{
    int len = static_cast<int>(strlen(buf));
    uint32_t hash = 5381;

    while (len--)
        hash = ((hash << 5) + hash) + (*buf++); /* hash * 33 + c */

    return hash;
}

/* 类型T必须支持拷贝构造函数 */
template<class T>
class val_t
{
    public:
        val_t(const char *key, const T &val):_val(val)
        {
            snprintf(_key, sizeof(_key), "%s", key);
        }

        T& get_val()
        {
            return _val;
        }

        const char* get_key() const
        {
            return _key;
        }

    private:
        char _key[64];
        T _val;
};

/* 不支持多线程 */
template<class T>
class hashtable_t
{
    public:
        typedef std::vector<val_t<T>*> buck_t;
        typedef typename std::vector<val_t<T>*>::iterator buck_iterator;
        explicit hashtable_t(uint32_t n)
        {
            /* 分配n个指针的空间，并且置空 */
            _bucks = new(std::nothrow) buck_t*[n];
            if (_bucks == NULL) oom_error();

            for (uint32_t i = 0; i < n; ++i)
            {
                _bucks[i] = NULL;
            }
            _sz = n;
        }

        ~hashtable_t()
        {
            for (uint32_t i = 0; i < _sz; i++)
            {
                if (_bucks[i] != NULL)
                {
                    /* 释放vector成员所指向的对象 */
                    for (buck_iterator it = _bucks[i]->begin(); 
                            it != _bucks[i]->end();
                            ++it)
                    {
                        if (*it != NULL)
                        {
                            delete *it;
                            *it = NULL;
                        }
                    }
                    /* 释放std::vector本身 */
                    delete _bucks[i];
                    _bucks[i] = NULL;
                }
            }
            /* 释放指针空间 */
            delete [] _bucks;
            _bucks = NULL;
        }
        
        void set_value(const char *key, const T& value)
        {
            /* 生成新对象 */
            val_t<T> *myval = new(std::nothrow) val_t<T>(key, value);
            if (myval == NULL) oom_error();
            
            /* 计算buck */
            uint32_t code = hashcode(key) % _sz;
            /* 放入对应buck之中 */
            if (_bucks[code] == NULL)
            {
                _bucks[code] = new(std::nothrow) buck_t();
                if (_bucks[code] == NULL) oom_error();

                _bucks[code]->push_back(myval);
            }else
            {
                _bucks[code]->push_back(myval);
            }
        }

        T* get_value(const char *key)
        {
            /* 计算对应buck */
            uint32_t code = hashcode(key) % _sz;
            buck_t *curbuck = _bucks[code];
            if(curbuck == NULL) return false;

            for (buck_iterator it = curbuck->begin();
                    it != curbuck->end();
                    ++it)
            {
                val_t<T> *curval = *it;
                assert(curval != NULL);
                if (!strcmp(key, curval->get_key()))
                {
                    T* ret = &(curval->get_val());
                    return ret;
                }
            }
            return NULL;
        }

    private:
        /* 指向buck_t* 的指针数组 */
        buck_t **_bucks;
        uint32_t _sz;
};
} //namespace bus
#endif  //__HASH_TABLE_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
