#!/usr/bin/env bash


DIR=$(cd $(dirname $0) && pwd)

if [ $# -ne 2 ]; then
    echo "Usage: $0 DAXFILE PROPERTIESFILE"
    exit 1
fi

DAXFILE=$1
PROPERTIESFILE=$2

# This command tells Pegasus to plan the workflow contained in 
# dax file passed as an argument. The planned workflow will be stored
# in the "submit" directory. The execution # site is "".
# --input-dir tells Pegasus where to find workflow input files.
# --output-dir tells Pegasus where to place workflow output files.
pegasus-plan --conf $PROPERTIESFILE \
    --dax $DAXFILE \
    --dir $DIR/submit \
    --sites condorpool \
    --output-site local \
    --cleanup leaf \
    --cluster label \
    --force \
    --submit
    #--cluster horizontal \
    #--cluster label \
