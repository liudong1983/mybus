#ifndef HBASE_TYPES_H
#define HBASE_TYPES_H

namespace bus {

struct TDeleteType {
  enum type {
    DELETE_COLUMN = 0,
    DELETE_COLUMNS = 1
  };
};

class TColumn {
 public:

  TColumn() : family(), qualifier(), timestamp(0) {
  }

  ~TColumn() {}

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

class TColumnValue {
 public:

  TColumnValue() : family(), qualifier(), value(), timestamp(0) {
  }

  ~TColumnValue() {}

  std::string family;
  std::string qualifier;
  std::string value;
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

class TPut {
 public:

  TPut() : row(), timestamp(0), writeToWal(true) {
  }

  ~TPut() {}

  std::string row;
  std::vector<TColumnValue>  columnValues;
  int64_t timestamp;
  bool writeToWal;
  std::map<std::string, std::string>  attributes;

  void __set_row(const std::string& val) {
    row = val;
  }

  void __set_columnValues(const std::vector<TColumnValue> & val) {
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

class TDelete {
 public:

  TDelete() : row(), timestamp(0), deleteType((TDeleteType::type)1), writeToWal(true) {
    deleteType = (TDeleteType::type)1;

  }

  ~TDelete() {}

  std::string row;
  std::vector<TColumn>  columns;
  int64_t timestamp;
  TDeleteType::type deleteType;
  bool writeToWal;
  std::map<std::string, std::string>  attributes;

  void __set_row(const std::string& val) {
    row = val;
  }

  void __set_columns(const std::vector<TColumn> & val) {
    columns = val;
  }

  void __set_timestamp(const int64_t val) {
    timestamp = val;
  }

  void __set_deleteType(const TDeleteType::type val) {
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
#endif
