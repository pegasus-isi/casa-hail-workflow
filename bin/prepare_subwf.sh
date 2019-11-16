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

cat << EOF > ${daxname}.dax
<?xml version="1.0" encoding="UTF-8"?>
<!-- generated: 2019-11-15 18:24:44.600861 -->
<!-- generated by: ldm -->
<!-- generator: python -->
<adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="dummy">
	<metadata key="name">dummy</metadata>
	<metadata key="dax.api">python</metadata>
	<executable name="true" arch="x86_64" os="linux">
		<pfn url="/bin/true" site="local"/>
	</executable>
	<job id="ID0000001" name="true">
		<profile namespace="hints" key="execution.site">local</profile>
	</job>
</adag>
EOF

cp $default_props ${daxname}.properties
cp $default_replica ${daxname}.rc.txt
echo "EMPTY" > composite_cart_input.txt

echo "accepted_prefixes=${accepted_prefix}" >> ${watch_config}

cat << EOF > generate_wf.sh
#!/usr/bin/env bash

daxgen_out=\$(python nexrad_daxgen.py -n ${daxname} -o . -p ${default_props} -r ${default_replica} -cf ${cart_input[@]} -f \$@)
EOF

chmod +x generate_wf.sh

/usr/bin/python3 casa_watch_http.py --conf ${watch_config} --timeout ${timeout}

exit 0
