# Based on c_src.mk from erlang.mk by Loic Hoguin <essen@ninenines.eu>

CURDIR := $(shell pwd)
BASEDIR := $(abspath $(CURDIR)/..)

PROJECT := erlav_nif

ERTS_INCLUDE_DIR ?= $(shell erl -noshell -eval "io:format(\"~ts/erts-~ts/include/\", [code:root_dir(), erlang:system_info(version)])." -s init stop)
ERL_INTERFACE_INCLUDE_DIR ?= $(shell erl -noshell -eval "io:format(\"~ts\", [code:lib_dir(erl_interface, include)])." -s init stop)
ERL_INTERFACE_LIB_DIR ?= $(shell erl -noshell -eval "io:format(\"~ts\", [code:lib_dir(erl_interface, lib)])." -s init stop)

C_SRC_DIR = $(CURDIR)
C_SRC_OUTPUT ?= $(CURDIR)/../priv/$(PROJECT).so

# System type and C compiler/flags.

# Set NATIVE_ARCH=1 to tune codegen for the build machine's CPU (e.g. AVX2/BMI2).
# Only safe when the binary runs on the same (or identical) hardware it was built
# on -- a -march=native .so shipped to a different CPU can SIGILL at load time.
ifeq ($(NATIVE_ARCH), 1)
	ARCHFLAGS = -march=native
endif

UNAME_SYS := $(shell uname -s)
ifeq ($(UNAME_SYS), Darwin)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -finline-functions -funroll-loops -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -funroll-loops -Wall -fpermissive
	LDFLAGS ?= -flat_namespace -undefined suppress
else ifeq ($(UNAME_SYS), FreeBSD)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -finline-functions -funroll-loops -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -funroll-loops -Wall
else ifeq ($(UNAME_SYS), Linux)
	CC ?= gcc
	CFLAGS ?= -O3 -std=c99 -finline-functions -funroll-loops -Wall -Wmissing-prototypes
	CXXFLAGS ?= -O3 -finline-functions -funroll-loops -Wall
endif
CXXFLAGS += -std=c++17

# -flto lets the linker inline across translation units (e.g. decode_scalar/
# decodeLong into the decode/decode_array hot loops in mkh_avro_decoder.cc).
# -DNDEBUG strips nlohmann/json's internal assertions (schema parsing only,
# but free). ARCHFLAGS is empty unless NATIVE_ARCH=1 (see above).
#
# -fvisibility(-inlines)=hidden makes every symbol local by default, except
# the NIF's nif_init entry point, which erl_nif.h's ERL_NIF_INIT macro tags
# with __attribute__((visibility("default"))) itself regardless of this
# flag. Hidden visibility gives LTO certainty that nothing outside the .so
# calls the internal mkh_avro2/SchemaItem functions, which allows more
# complete cross-TU inlining/dead-code elimination, and shrinks the dynamic
# symbol table.
#
# -ffunction-sections/-fdata-sections + --gc-sections (Linux)/-dead_strip
# (Darwin) let the linker drop any unreferenced code/data at section
# granularity.
CFLAGS += -flto -DNDEBUG $(ARCHFLAGS) -fvisibility=hidden \
	-ffunction-sections -fdata-sections
CXXFLAGS += -flto -DNDEBUG $(ARCHFLAGS) -fvisibility=hidden \
	-fvisibility-inlines-hidden -ffunction-sections -fdata-sections

CFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR)
CXXFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR)

LDLIBS += -L $(ERL_INTERFACE_LIB_DIR) -lei
LDFLAGS += -shared -flto

ifeq ($(UNAME_SYS), Darwin)
	LDFLAGS += -Wl,-dead_strip
else
	LDFLAGS += -Wl,--gc-sections
endif

# Verbosity.

c_verbose_0 = @echo " C     " $(?F);
c_verbose = $(c_verbose_$(V))

cpp_verbose_0 = @echo " CPP   " $(?F);
cpp_verbose = $(cpp_verbose_$(V))

link_verbose_0 = @echo " LD    " $(@F);
link_verbose = $(link_verbose_$(V))

SOURCES := $(shell find $(C_SRC_DIR) -type f \( -name "*.c" -o -name "*.C" -o -name "*.cc" -o -name "*.cpp" \))
OBJECTS = $(addsuffix .o, $(basename $(SOURCES)))

COMPILE_C = $(c_verbose) $(CC) $(CFLAGS) $(CPPFLAGS) -c
COMPILE_CPP = $(cpp_verbose) $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c

$(C_SRC_OUTPUT): $(OBJECTS)
	@mkdir -p $(BASEDIR)/priv/
	$(link_verbose) $(CC) $(OBJECTS) $(LDFLAGS) $(LDLIBS) -o $(C_SRC_OUTPUT)

%.o: %.c
	$(COMPILE_C) $(OUTPUT_OPTION) $<

%.o: %.cc
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.C
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.cpp
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

clean:
	@rm -f $(C_SRC_OUTPUT) $(OBJECTS)
