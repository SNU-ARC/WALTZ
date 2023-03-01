#!/bin/bash

cat $1 | grep LOG | sed 's/LOG //g' > temp.log

./parser/parser ./temp.log
#gdb --args ./parser/parser ./temp.log
