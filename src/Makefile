CXX:= g++
CXXFLAGS:= -Wall -W -D__STDC_LIMIT_MACROS -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H
LDFLAGS:=
DEBUGFLAGS:= -g3 -gdwarf-2 -ggdb
OPTFLAG:= -O2
AR:= ar
ARFLAGS:= rcs

progname:= bus
libarchive := libbus.a
busobj:= bus_config.o threadpool.o \
	data_source.o bus_engine.o bus_wrap.o  \
	bus_util.o bus_row.o mysql_full_extract.o mysql_incr_extract.o \
	redis_full_sender.o bus_event.o redis_increment_sender.o \
	bus_server.o bus_stat.o bus_charset.o bus_redis_util.o bus_fake.o \
	bus_log.o hbase_types.o hbase_constants.o THBaseService.o \
	bus_hbase_util.o hbase_full_sender.o hbase_incr_sender.o
mainobj:= main.o

boost_dir:= ../include/boost
thrift_dir:= ../include
thrift_lib_dir:= ../lib/thrift
thrift_gen_dir:= ../dep/gen-cpp
thrift_gen_src:= ${thrift_gen_dir}/hbase_types.cpp ${thrift_gen_dir}/hbase_constants.cpp ${thrift_gen_dir}/THBaseService.cpp

tinydir:= ../dep/tinyxml
regexdir:= ../dep/regex
tinysrc:= $(tinydir)/*.cpp
regexsrc:= $(regexdir)/*.c
includedir:=  -I ../include/mysql -I ${thrift_dir} -I ${boost_dir} -I${thrift_gen_dir} \
	-I ../dep/tinyxml -I ../include/redis
libdir:= -L ../lib/mysql  -L ../lib/redis -L ${thrift_lib_dir}
libs:= -Wl,-dn -lhiredis -lmysqlclient_r -Wl,-dy -lcrypt -lz -lrt -lssl -lm -lpthread -ldl -lthrift
macros:= -DVERSION=\"${VERSION}\"  -DRELEASE=\"${RELEASE}\" -DNDEBUG
all: $(progname) $(libarchive)
#deps

hbase_types.o: ${thrift_gen_dir}/hbase_types.cpp ${thrift_gen_dir}/hbase_types.h
hbase_constants.o: ${thrift_gen_dir}/hbase_constants.cpp ${thrift_gen_dir}/hbase_constants.h
THBaseService.o: ${thrift_gen_dir}/THBaseService.cpp ${thrift_gen_dir}/THBaseService.h\
	${thrift_gen_dir}/hbase_types.h ${thrift_gen_dir}/hbase_constants.h

bus_hbase_util.o: bus_hbase_util.cc bus_hbase_util.h bus_config.h\
	${thrift_gen_dir}/hbase_types.h ${thrift_gen_dir}/hbase_types.h ${thrift_gen_dir}/THBaseService.h \
  ../include/redis/hiredis.h bus_util.h bus_log.h threadpool.h bus_row.h \
  bus_config.h data_source.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h \
  bus_server.h bus_stat.h
	g++ -c  $(CXXFLAGS) $(macros) $(DEBUGFLAGS) $(includedir) $< ${thrift_gen_src}

hbase_full_sender.o: hbase_full_sender.cc hbase_full_sender.h \
  ../include/redis/hiredis.h bus_util.h bus_log.h threadpool.h bus_row.h \
  bus_config.h data_source.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h \
  bus_server.h bus_stat.h bus_hbase_util.h

hbase_incr_sender.o: hbase_incr_sender.cc hbase_incr_sender.h \
  ../include/redis/hiredis.h bus_util.h bus_log.h threadpool.h bus_row.h \
  bus_config.h data_source.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h \
  bus_server.h bus_stat.h bus_hbase_util.h

bus_charset.o: bus_charset.cc bus_charset.h bus_util.h bus_log.h \
  bus_row.h
bus_config.o: bus_config.cc bus_config.h bus_util.h bus_log.h \
  data_source.h ../include/redis/hiredis.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h \
  bus_row.h threadpool.h bus_server.h bus_stat.h
bus_engine.o: bus_engine.cc bus_engine.h bus_util.h bus_log.h bus_row.h \
  threadpool.h ../include/redis/hiredis.h bus_config.h data_source.h \
  ../include/mysql/mysql.h ../include/mysql/mysql_version.h \
  ../include/mysql/mysql_com.h ../include/mysql/mysql_time.h \
  ../include/mysql/my_list.h ../include/mysql/typelib.h \
  ../include/mysql/my_alloc.h hash_table.h bus_position.h \
  ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_server.h bus_stat.h
bus_event.o: bus_event.cc bus_event.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h bus_util.h \
  bus_log.h bus_row.h bus_config.h data_source.h \
  ../include/redis/hiredis.h hash_table.h bus_position.h \
  ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h threadpool.h \
  bus_server.h bus_stat.h bus_charset.h ../include/mysql/mydecimal.h \
  ../include/mysql/myconvert.h
bus_fake.o: bus_fake.cc bus_fake.h threadpool.h \
  ../include/redis/hiredis.h bus_util.h bus_log.h bus_row.h bus_wrap.h \
  bus_config.h data_source.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_engine.h bus_server.h \
  bus_stat.h
bus_log.o: bus_log.cc bus_log.h
bus_redis_util.o: bus_redis_util.cc bus_redis_util.h \
  ../include/redis/hiredis.h bus_util.h bus_log.h threadpool.h bus_row.h \
  bus_config.h data_source.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h \
  bus_server.h bus_stat.h
bus_row.o: bus_row.cc bus_row.h bus_util.h bus_log.h
bus_server.o: bus_server.cc bus_server.h bus_util.h bus_log.h bus_wrap.h \
  bus_config.h data_source.h ../include/redis/hiredis.h \
  ../include/mysql/mysql.h ../include/mysql/mysql_version.h \
  ../include/mysql/mysql_com.h ../include/mysql/mysql_time.h \
  ../include/mysql/my_list.h ../include/mysql/typelib.h \
  ../include/mysql/my_alloc.h hash_table.h bus_position.h \
  ../dep/tinyxml/tinyxml2.h bus_engine.h bus_row.h threadpool.h \
  bus_stat.h
bus_stat.o: bus_stat.cc bus_stat.h bus_util.h bus_log.h bus_position.h \
  threadpool.h ../include/redis/hiredis.h bus_row.h bus_wrap.h \
  bus_config.h data_source.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  ../dep/tinyxml/tinyxml2.h bus_engine.h bus_server.h
bus_util.o: bus_util.cc bus_util.h bus_log.h bus_wrap.h bus_config.h \
  data_source.h ../include/redis/hiredis.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_engine.h bus_row.h \
  threadpool.h bus_server.h bus_stat.h
bus_wrap.o: bus_wrap.cc bus_wrap.h bus_config.h bus_util.h bus_log.h \
  data_source.h ../include/redis/hiredis.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_engine.h bus_row.h \
  threadpool.h bus_server.h bus_stat.h mysql_full_extract.h \
  mysql_incr_extract.h redis_increment_sender.h bus_redis_util.h \
  bus_hbase_util.h redis_full_sender.h bus_fake.h
data_source.o: data_source.cc data_source.h ../include/redis/hiredis.h \
  ../include/mysql/mysql.h ../include/mysql/mysql_version.h \
  ../include/mysql/mysql_com.h ../include/mysql/mysql_time.h \
  ../include/mysql/my_list.h ../include/mysql/typelib.h \
  ../include/mysql/my_alloc.h bus_util.h bus_log.h hash_table.h \
  bus_position.h bus_wrap.h bus_config.h ../dep/tinyxml/tinyxml2.h \
  bus_engine.h bus_row.h threadpool.h bus_server.h bus_stat.h
main.o: main.cc bus_util.h bus_log.h bus_config.h data_source.h \
  ../include/redis/hiredis.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h \
  bus_row.h threadpool.h bus_server.h bus_stat.h
mysql_full_extract.o: mysql_full_extract.cc mysql_full_extract.h \
  bus_util.h bus_log.h bus_wrap.h bus_config.h data_source.h \
  ../include/redis/hiredis.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_engine.h bus_row.h \
  threadpool.h bus_server.h bus_stat.h
mysql_incr_extract.o: mysql_incr_extract.cc bus_event.h \
  ../include/mysql/mysql.h ../include/mysql/mysql_version.h \
  ../include/mysql/mysql_com.h ../include/mysql/mysql_time.h \
  ../include/mysql/my_list.h ../include/mysql/typelib.h \
  ../include/mysql/my_alloc.h bus_util.h bus_log.h bus_row.h bus_config.h \
  data_source.h ../include/redis/hiredis.h hash_table.h bus_position.h \
  ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h threadpool.h \
  bus_server.h bus_stat.h bus_charset.h mysql_incr_extract.h
redis_full_sender.o: redis_full_sender.cc redis_full_sender.h \
  ../include/redis/hiredis.h bus_util.h bus_log.h threadpool.h bus_row.h \
  bus_config.h data_source.h ../include/mysql/mysql.h \
  ../include/mysql/mysql_version.h ../include/mysql/mysql_com.h \
  ../include/mysql/mysql_time.h ../include/mysql/my_list.h \
  ../include/mysql/typelib.h ../include/mysql/my_alloc.h hash_table.h \
  bus_position.h ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h \
  bus_server.h bus_stat.h bus_redis_util.h
redis_increment_sender.o: redis_increment_sender.cc \
  redis_increment_sender.h ../include/redis/hiredis.h bus_util.h \
  bus_log.h threadpool.h bus_row.h bus_config.h data_source.h \
  ../include/mysql/mysql.h ../include/mysql/mysql_version.h \
  ../include/mysql/mysql_com.h ../include/mysql/mysql_time.h \
  ../include/mysql/my_list.h ../include/mysql/typelib.h \
  ../include/mysql/my_alloc.h hash_table.h bus_position.h \
  ../dep/tinyxml/tinyxml2.h bus_wrap.h bus_engine.h bus_server.h \
  bus_stat.h bus_redis_util.h
threadpool.o: threadpool.cc threadpool.h ../include/redis/hiredis.h \
  bus_util.h bus_log.h bus_row.h bus_wrap.h bus_config.h data_source.h \
  ../include/mysql/mysql.h ../include/mysql/mysql_version.h \
  ../include/mysql/mysql_com.h ../include/mysql/mysql_time.h \
  ../include/mysql/my_list.h ../include/mysql/typelib.h \
  ../include/mysql/my_alloc.h hash_table.h bus_position.h \
  ../dep/tinyxml/tinyxml2.h bus_engine.h bus_server.h bus_stat.h
$(progname): $(busobj) $(mainobj) $(tinysrc)
	$(CXX) -o $@ $^ $(CXXFLAGS) $(LDFLAGS) $(OPTFLAG) $(macros) $(DEBUGFLAGS) $(includedir) $(libdir) $(libs)
$(libarchive):  $(busobj)
	$(AR) $(ARFLAGS) $(libarchive) $^
%.o: %.cc
	$(CXX) -c $(CXXFLAGS) $(macros) $(DEBUGFLAGS) $(includedir) $<
%.o: %.cpp
	$(CXX) -c $(CXXFLAGS) $(macros) $(DEBUGFLAGS) $(includedir) $<
dep:
	@$(CXX) -MM *.cc ${includedir} 
clean:
	rm -rf $(progname)  *.o *.a *.log *.so core.* libprocess*
