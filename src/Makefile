include $(GOROOT)/src/Make.inc

TARG=gozk

CGOFILES=\
	gozk.go\

CGO_LDFLAGS+=-lzookeeper_mt

include $(GOROOT)/src/Make.pkg

_cgo_defun.c: helpers.c
