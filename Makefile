include $(GOROOT)/src/Make.inc

TARG=gozk

CGOFILES=\
	gozk.go\

CGO_OFILES=\
	helpers.o\

ifdef ZKROOT
LIBDIR=$(ZKROOT)/src/c/.libs
LD_LIBRARY_PATH:=$(LIBDIR):$(LD_LIBRARY_PATH)
CGO_CFLAGS+=-I$(ZKROOT)/src/c/include -I$(ZKROOT)/src/c/generated
CGO_LDFLAGS+=-L$(LIBDIR)
else
LIBDIR=/usr/lib
CGO_CFLAGS+=-I/usr/include/zookeeper
endif

ifndef STATIC
CGO_LDFLAGS+=-lzookeeper_mt
else
CGO_LDFLAGS+=-lm -lpthread
CGO_OFILES+=$(wildcard _lib/*.o)

all: package
_lib:
	@mkdir -p _lib
	cd _lib && ar x $(LIBDIR)/libzookeeper_mt.a

_cgo_defun.c: _lib
endif

include $(GOROOT)/src/Make.pkg

_cgo_defun.c: helpers.c
