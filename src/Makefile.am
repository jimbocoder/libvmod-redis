AM_CPPFLAGS = @VMOD_INCLUDES@

vmoddir = @VMOD_DIR@
vmod_LTLIBRARIES = libvmod_redis.la

libvmod_redis_la_LDFLAGS = -module -export-dynamic -version-info 1:0:0
libvmod_redis_la_LIBADD = -lhiredis

libvmod_redis_la_SOURCES = \
	vcc_if.c \
	vcc_if.h \
	vmod_redis.c

vcc_if.c vcc_if.h: @VMODTOOL@ $(top_srcdir)/src/vmod_redis.vcc
	@VMODTOOL@ $(top_srcdir)/src/vmod_redis.vcc

VMOD_TESTS = tests/*.vtc
.PHONY: $(VMOD_TESTS)

tests/*.vtc:
	@VARNISHTEST@ -Dvarnishd=@VARNISHD@ -Dvmod_topbuild=$(abs_top_builddir) $@

check: $(VMOD_TESTS)

EXTRA_DIST = \
	vmod_redis.vcc \
	$(VMOD_TESTS)

CLEANFILES = $(builddir)/vcc_if.c $(builddir)/vcc_if.h \
	$(builddir)/vmod_redis.man.rst \
	$(builddir)/vmod_redis.rst
