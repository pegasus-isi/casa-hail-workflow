#!/usr/bin/env bash

old_dir=`pwd`

cd $HAIL_WORKFLOW_DIR

daxgen_out=$(python daxgen.py -o dax_outputs -p pegasus.default.properties -r rc.default.txt -f $@)
dax_file=$(echo $daxgen_out | cut -d ' ' -f 1)
properties_file=$(echo $daxgen_out | cut -d ' ' -f 2)

echo $dax_file
./plan.sh ${dax_file} ${properties_file}

cd $old_dir

