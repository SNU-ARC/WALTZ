# ZenFS utility makefile

TARGET = zenfs

CC ?= gcc
CXX ?= g++

SPDK_LIBRARIES += -lspdk
SPDK_LIBRARIES += -lspdk_env_dpdk
SPDK_LIBRARIES += -lrte_ring
SPDK_LIBRARIES += -lrte_mempool
SPDK_LIBRARIES += -lrte_bus_pci
SPDK_LIBRARIES += -lrte_eal
SPDK_LIBRARIES += -lrte_pci
SPDK_LIBRARIES += -lrte_kvargs
SPDK_LIBRARIES += -lrte_vhost
SPDK_LIBRARIES += -lrte_power

EXTRA_LDFLAGS += -Wl,--whole-archive
EXTRA_LDFLAGS += -lisal
EXTRA_LDFLAGS += $(SPDK_LIBRARIES)
EXTRA_LDFLAGS += -Wl,--no-whole-archive

EXTRA_CXXFLAGS += -g

CXXFLAGS = $(shell pkg-config --cflags rocksdb)
LIBS = $(shell pkg-config --static --libs rocksdb)

CXXFLAGS +=  $(EXTRA_CXXFLAGS)
LDFLAGS +=  $(EXTRA_LDFLAGS)

all: $(TARGET)

$(TARGET): $(TARGET).cc
	$(CXX) $(CXXFLAGS) -o $(TARGET) $< $(LIBS) $(LDFLAGS)

clean:
	$(RM) $(TARGET)
