#!/usr/bin/env bash

old_dir=`pwd`

cd $HAIL_WORKFLOW_DIR

dax_name=$(python pp_daxgen.py -o dax_outputs -f $@)
echo $dax_name;
./plan.sh ${dax_name}

cd $old_dir
