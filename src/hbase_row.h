/***************************************************************************
 * 
 * Copyright (c) 2014 Sina.com, Inc. All Rights Reserved
 * 
 **************************************************************************/
 
 
 
/**
 * @file hbase_row.h
 * @author zhangbo6(zhangbo6@staff.sina.com.cn)
 * @date 2014/12/29 18:35:57
 * @version $Revision$
 * @brief 
 *  
 **/

//maybe use this to 
//class hbase_cmd_t {
//  public:
//      std::string table;
//      tput_t *tput;
//      tdelete_t *tdelete;
//      uint32_t type;
//      //....
//      };

#ifndef  __HBASE_ROW_H_
#define  __HBASE_ROW_H_

namespace bus {

struct tdeletetype {
  enum type {
    DELETE_COLUMN = 0,
    DELETE_COLUMNS = 1
  };
};

class tcolumn_t {
 public:

  tcolumn_t() : family(), qualifier(), timestamp(0) {
  }

  ~tcolumn_t() {}

  std::string family;
  std::string qualifier;
  int64_t timestamp;

  void __set_family(const std::string& val) {
    family = val;
  }

  void __set_qualifier(const std::string& val) {
    qualifier = val;
  }

  void __set_timestamp(const int64_t val) {
    timestamp = val;
  }

};

class tcolumnvalue_t {
 public:

  tcolumnvalue_t() : family(), qualifier(), value(), timestamp(0) {
  }

  ~tcolumnvalue_t() {}

  std::string family; // char[128] 98%
  std::string qualifier; //char[128]
  std::string value; //char *, int flag
  int64_t timestamp;

  void __set_family(const std::string& val) {
    family = val;
  }

  void __set_qualifier(const std::string& val) {
    qualifier = val;
  }

  void __set_value(const std::string& val) {
    value = val;
  }

  void __set_timestamp(const int64_t val) {
    timestamp = val;
  }
};

class tput_t {
 public:

  tput_t() : row(), timestamp(0), writeToWal(true) {
  }

  ~tput_t() {}

  std::string row; //char[128]
  std::vector<tcolumnvalue_t>  columnValues;
  int64_t timestamp;
  bool writeToWal;
  std::map<std::string, std::string>  attributes;

  void __set_row(const std::string& val) {
    row = val;
  }

  void __set_columnValues(const std::vector<tcolumnvalue_t> & val) {
    columnValues = val;
  }

  void __set_timestamp(const int64_t val) {
    timestamp = val;
  }

  void __set_writeToWal(const bool val) {
    writeToWal = val;
  }

  void __set_attributes(const std::map<std::string, std::string> & val) {
    attributes = val;
  }

};

class tdelete_t {
 public:

  tdelete_t() : row(), timestamp(0), deleteType((tdeletetype::type)1), writeToWal(true) {
    deleteType = (tdeletetype::type)1;

  }

  ~tdelete_t() {}

  std::string row;
  std::vector<tcolumn_t>  columns;
  int64_t timestamp;
  tdeletetype::type deleteType;
  bool writeToWal;
  std::map<std::string, std::string>  attributes;

  void __set_row(const std::string& val) {
    row = val;
  }

  void __set_columns(const std::vector<tcolumn_t> & val) {
    columns = val;
  }

  void __set_timestamp(const int64_t val) {
    timestamp = val;
  }

  void __set_deleteType(const tdeletetype::type val) {
    deleteType = val;
  }

  void __set_writeToWal(const bool val) {
    writeToWal = val;
  }

  void __set_attributes(const std::map<std::string, std::string> & val) {
    attributes = val;
  }
};

}//namespace bus




#endif  //__HBASE_ROW_H_

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
