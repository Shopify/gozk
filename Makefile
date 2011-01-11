include $(GOROOT)/src/Make.inc

TARG=gozk

CGOFILES=\
	gozk.go\

CGO_OFILES=\
	helpers.o\

CGO_LDFLAGS+=-lzookeeper_mt

#CGO_LDFLAGS=-lm -lpthread
#CGO_OFILES=helpers.o $(shell ls asd/*.o)

include $(GOROOT)/src/Make.pkg

_cgo_defun.c: helpers.c
