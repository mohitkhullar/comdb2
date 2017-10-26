#######################################################
# Makefile for backward compatibility with test suite #
#######################################################
.PHONY: test_tools
test_tools: compat_install
	@cd build && $(MAKE) -s -j blob bound cdb2_client cdb2api_caller cdb2bind comdb2_blobtest comdb2_sqltest crle hatest insert_lots_mt leakcheck localrep overflow_blobtest ptrantest recom selectv serial sicountbug sirace simple_ssl stepper utf8
	@ln -f build/tests/tools/blob tests/bloballoc.test/blob
	@ln -f build/tests/tools/bound tests/tools/bound
	@ln -f build/tests/tools/cdb2_client tests/cdb2api_so.test/cdb2_client
	@ln -f build/tests/tools/cdb2api_caller tests/tools/cdb2api_caller
	@ln -f build/tests/tools/cdb2bind tests/cdb2bind.test/cdb2bind
	@ln -f build/tests/tools/comdb2_blobtest tests/blob_size_limit.test/comdb2_blobtest
	@ln -f build/tests/tools/comdb2_sqltest tests/sqlite.test/comdb2_sqltest
	@ln -f build/tests/tools/crle tests/tools/crle
	@ln -f build/tests/tools/hatest tests/tools/hatest
	@ln -f build/tests/tools/insert_lots_mt  tests/insert_lots.test/insert_lots_mt
	@ln -f build/tests/tools/leakcheck tests/leakcheck.test/leakcheck
	@ln -f build/tests/tools/localrep tests/tools/localrep
	@ln -f build/tests/tools/overflow_blobtest tests/tools/overflow_blobtest
	@ln -f build/tests/tools/ptrantest tests/tools/ptrantest
	@ln -f build/tests/tools/recom tests/tools/recom
	@ln -f build/tests/tools/selectv tests/tools/selectv
	@ln -f build/tests/tools/serial tests/tools/serial
	@ln -f build/tests/tools/sicountbug tests/sicountbug.test/sicountbug
	@ln -f build/tests/tools/sirace tests/sirace.test/sirace
	@ln -f build/tests/tools/simple_ssl tests/simple_ssl.test/simple_ssl
	@ln -f build/tests/tools/stepper tests/tools/stepper
	@ln -f build/tests/tools/utf8 tests/tools/utf8

.PHONY: compat_install
compat_install: all
	@ln -f build/db/comdb2 cdb2_dump
	@ln -f build/db/comdb2 cdb2_printlog
	@ln -f build/db/comdb2 cdb2_stat
	@ln -f build/db/comdb2 cdb2_verify
	@ln -f build/db/comdb2 comdb2
	@ln -f build/tools/cdb2_sqlreplay/cdb2_sqlreplay cdb2_sqlreplay
	@ln -f build/tools/cdb2sockpool/cdb2sockpool cdb2sockpool
	@ln -f build/tools/cdb2sql/cdb2sql cdb2sql
	@ln -f build/tools/comdb2ar/comdb2ar comdb2ar
	@ln -f build/tools/pmux/pmux pmux

.PHONY: all
all: build
	@cd build && $(MAKE) -s -j

CMAKE3 := $(shell command -v cmake3 2> /dev/null)
build:
ifndef CMAKE3
	@mkdir build && cd build && cmake ..
else
	@mkdir build && cd build && cmake3 ..
endif

.PHONY: clean
clean: build
	@cd build && $(MAKE) -s -j clean

.PHONY: deb-current
deb-current: package

.PHONY: rpm-current
rpm-current: package

.PHONY: package
package: all
	@cd build && $(MAKE) -s -j package

.PHONY: install
install: all
	@cd build && $(MAKE) -s -j install

.PHONY: test
test: test_tools
	$(MAKE) -C tests
