#!/usr/bin/env bash

watch_config=""
accepted_prefix=""
timeout=""
daxname=""
default_replica=""
default_props=""
cart_input=()

while getopts "c:p:t:n:r:s:i:" opt; do
  case ${opt} in
    c) watch_config=$OPTARG
      ;;
    p) accepted_prefix=$OPTARG
      ;;
    t) timeout=$OPTARG
      ;;
    n) daxname=$OPTARG
      ;;
    r) default_replica=$OPTARG
      ;;
    s) default_props=$OPTARG
      ;;
    i) cart_input+=("$OPTARG")
      ;;
    *) echo "Usage: cmd [-c HTTP_WATCH_CONFIG] [-p ACCEPTED_PREFIX] [-t TIMEOUT] [-n DAXNAME] [-r DEFAULT_REPLICA] [-s DEFAULT_PROPS] [i CART_INPUT]"
      ;;
  esac
done

if [ -z "$watch_config" ] || [ -z "$accepted_prefix" ] || [ -z "$timeout" ] || [ -z "$daxname" ] || [ -z "$default_replica" ] || [ -z "$default_props" ]; then
    echo "Missing arguments"
    echo "Usage: cmd [-c HTTP_WATCH_CONFIG] [-p ACCEPTED_PREFIX] [-t TIMEOUT] [-n DAXNAME] [-r DEFAULT_REPLICA] [-s DEFAULT_PROPS] [-i CART_INPUT]"
    exit 1
fi

echo "EMPTY" > ${daxname}.dax
echo "EMPTY" > ${daxname}.properties
echo "EMPTY" > ${daxname}.rc.txt
echo "EMPTY" > composite_cart_input.txt

echo "accepted_prefixes=${accepted_prefix}" >> ${HTTP_WATCH_CONFIG}

cat << EOF > generate_wf.sh
#!/usr/bin/env bash

daxgen_out=$(python daxgen.py -n ${daxname} -o . -p ${default_props} -r ${default_replica} -cf ${cart_input[@]} -f $@)
EOF

chmod +x generare_wf.sh

python casa_watch_http --conf ${HTTP_WATCH_CONFIG} --timeout ${timeout}

exit 0
