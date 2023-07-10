#!/bin/bash

rm c_src/erlav_nif.o
rm priv/erlav_nif.so

rebar3 compile
