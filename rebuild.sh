#!/bin/bash

rm c_src/erlav_nif.o
rm priv/erlav_nif.so

export NATIVE_ARCH=1
rebar3 compile
