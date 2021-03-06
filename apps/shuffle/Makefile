# Makefile for shuffle
ROOT_PATH=../..
include $(ROOT_PATH)/build/shared.mk

librt_libs = $(ROOT_PATH)/bindings/cc/librt++.a
INC += -I$(ROOT_PATH)/bindings/cc

shuffle_src = $(wildcard *.cc)
shuffle_obj = $(shuffle_src:.cc=.o)

lib_shim = $(ROOT_PATH)/shim/libshim.a -ldl

src = $(shuffle_src)
obj = $(src:.cc=.o)
dep = $(obj:.o=.d)

# must be first
all: shuffle_node

shuffle_node: $(shuffle_obj) $(librt_libs) $(RUNTIME_DEPS)
	$(LDXX) -o $@ $(LDFLAGS) $(lib_shim) $(obj) $(librt_libs) $(RUNTIME_LIBS)

ifneq ($(MAKECMDGOALS),clean)
-include $(dep)   # include all dep files in the makefile
endif

# rule to generate a dep file by using the C preprocessor
# (see man cpp for details on the -MM and -MT options)
%.d: %.cc
	@$(CXX) $(CXXFLAGS) $< -MM -MT $(@:.d=.o) >$@
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

.PHONY: clean
clean:
	rm -f $(obj) $(dep) shuffle
