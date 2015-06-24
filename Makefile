all : bussrc
bussrc:
	cd src && make "VERSION=${VERSION}" "RELEASE=${RELEASE}"
	cd myso/huihua && make "VERSION=${VERSION}" "RELEASE=${RELEASE}"
	cd myso/msg && make "VERSION=${VERSION}" "RELEASE=${RELEASE}"
	cd myso/bowen && make "VERSION=${VERSION}" "RELEASE=${RELEASE}"
clean:
	cd src && make clean
	cd myso/huihua && make clean
	cd myso/msg && make clean
	cd myso/bowen && make clean
	cd ../../
install:
	install -D -m 755 src/bus $(RPM_INSTALL_ROOT)/usr/local/databus/bus
	install -D -m 755 myso/huihua/libprocess.so $(RPM_INSTALL_ROOT)/usr/local/databus/huihua/libprocess.so
	install -D -m 755 myso/msg/libprocess.so $(RPM_INSTALL_ROOT)/usr/local/databus/msg/libprocess.so
	install -D -m 755 myso/bowen/libprocess.so $(RPM_INSTALL_ROOT)/usr/local/databus/bowen/libprocess.so
