CXX:= g++
CXXFLAGS:= -Wall -W -fPIC -shared
LDFLAGS:=
DEBUGFLAGS:= -g -ggdb
OPTFLAG:= -O0
includedir:= -I ../../src
progname:= libprocess.so
srcfile:= bus_process.cc ../../src/bus_row.cc ../../src/bus_log.cc
macros:= -DVERSION=\"${VERSION}\"  -DRELEASE=\"${RELEASE}\" -DNDEBUG
all: $(progname)

$(progname): $(srcfile)
	$(CXX) -o $@ $^ $(CXXFLAGS) $(LDFLAGS) $(OPTFLAG) $(macros) $(DEBUGFLAGS) $(includedir)
clean:
	rm -rf $(progname)  *.o *.a *.log *.so
