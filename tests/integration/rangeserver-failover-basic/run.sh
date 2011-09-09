#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
#DATA_SEED=42 # for repeating certain runs
MAX_KEYS=${MAX_KEYS:-"200000"}
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
RUN_DIR=`pwd`

. $HT_HOME/bin/ht-env.sh

stop_range_servers() {
  echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:38061
  echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:38060

  sleep 1

  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs?.pid`
}

stop_rs1() {
  echo "shutdown; quit;" | $HT_HOME/bin/ht rsclient localhost:38060
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs1.pid`
}

stop_rs2() {
  kill -9 `cat $HT_HOME/run/Hypertable.RangeServer.rs2.pid`
}

# stop and start servers
rm metadata.* keys.* rs*dump.* 
rm -rf fs fs_pre
$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
    --clear --config=${SCRIPT_DIR}/test.cfg
#start both rangeservers
#$HT_HOME/bin/ht valgrind -v --log-file=vg_rs1.log --track-origins=yes \
#   $HT_HOME/bin/Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs1 \
   --Hypertable.RangeServer.Port=38060 --config=${SCRIPT_DIR}/test.cfg 2>1 > rangeserver.rs1.output&
sleep 10
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=38061 --config=${SCRIPT_DIR}/test.cfg 2>1 > rangeserver.rs2.output&

#create table
$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

#write data 
$HT_HOME/bin/ht ht_load_generator update \
    --Hypertable.Mutator.FlushDelay=50 \
    --rowkey.component.0.order=random \
    --rowkey.component.0.type=integer \
    --rowkey.component.0.format="%020lld" \
    --rowkey.component.0.min=0 \
    --rowkey.component.0.max=10000000000 \
    --row-seed=1 \
    --Field.value.size=200 \
    --max-keys=$MAX_KEYS

#dump METADATA location
sleep 30
${HT_HOME}/bin/ht shell --config=${SCRIPT_DIR}/test.cfg --no-prompt --exec "use sys; select Location from METADATA MAX_VERSIONS=1 into file '${RUN_DIR}/metadata.pre';"
${HT_HOME}/bin/ht shell --config=${SCRIPT_DIR}/test.cfg --no-prompt --exec "use '/'; select * from LoadTest KEYS_ONLY into file '${RUN_DIR}/keys.pre';"
#dump ranges
echo "dump nokeys '${RUN_DIR}/rs1_dump.pre'; quit;" | $HT_HOME/bin/ht rsclient localhost:38060
echo "dump nokeys '${RUN_DIR}/rs2_dump.pre'; quit;" | $HT_HOME/bin/ht rsclient localhost:38061
#copy state
cp -R ${HT_HOME}/fs ${RUN_DIR}/fs_pre

#kill rs2
stop_rs1


#wait for recovery to complete 
sleep 180 

#dump rs2 ranges
echo "dump nokeys '${RUN_DIR}/rs2_dump.post'; quit;" | $HT_HOME/bin/ht rsclient localhost:38061

#dump keys
${HT_HOME}/bin/ht shell --config=${SCRIPT_DIR}/test.cfg --no-prompt --exec "use '/'; select * from LoadTest KEYS_ONLY into file '${RUN_DIR}/keys.post';"
#dump METADATA location
${HT_HOME}/bin/ht shell --config=${SCRIPT_DIR}/test.cfg --no-prompt --exec "use sys; select Files,Location from METADATA MAX_VERSIONS=1 into file '${RUN_DIR}/metadata.post';"

#copy state
cp -R ${HT_HOME}/fs ${RUN_DIR}
cp -R ${HT_HOME}/log/Hypertable.Master.log ${RUN_DIR}

#bounce servers
$HT_HOME/bin/stop-servers.sh
stop_rs2

$HT_HOME/bin/start-test-servers.sh --no-rangeserver --no-thriftbroker \
    --config=${SCRIPT_DIR}/test.cfg
$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
   --Hypertable.RangeServer.ProxyName=rs2 \
   --Hypertable.RangeServer.Port=38061 --config=${SCRIPT_DIR}/test.cfg 2>1 >> rangeserver.rs2.output&

#dump METADATA location
${HT_HOME}/bin/ht shell --config=${SCRIPT_DIR}/test.cfg --no-prompt --exec "use sys; select Files,Location from METADATA MAX_VERSIONS=1 into file '${RUN_DIR}/metadata.post2';"
#dump keys
${HT_HOME}/bin/ht shell --config=${SCRIPT_DIR}/test.cfg --no-prompt --exec "use '/'; select * from LoadTest KEYS_ONLY into file '${RUN_DIR}/keys.post2';"

#stop servers
$HT_HOME/bin/stop-servers.sh
stop_rs2

#check output
TOTAL_KEYS=`cat keys.post|wc -l`
TOTAL_KEYS2=`cat keys.post2|wc -l`
EXPECTED_KEYS=`cat keys.pre|wc -l`
ORIGINAL_RANGES=`cat metadata.pre|wc -l`
RS1_RANGES=`grep "rs1" metadata.post|wc -l`
RS2_RANGES=`grep "rs2" metadata.post|wc -l`
echo "Total keys returned=${TOTAL_KEYS},${TOTAL_KEYS2} , expected keys=${EXPECTED_KEYS}, original ranges=${ORIGINAL_RANGES}, rs1_ranges=${RS1_RANGES}, rs2_ranges=${RS2_RANGES}"

if [ $TOTAL_KEYS -ne $EXPECTED_KEYS  || $TOTAL_KEYS_2 -ne $EXPECTED_KEYS ]
then
  echo "Test failed, expected ${EXPECTED_KEYS}, retrieved ${TOTAL_KEYS}, ${TOTAL_KEYS2}"
  exit 1
fi

if [ $RS2_RANGES -lt $ORIGINAL_RANGES ] 
then
  echo "Test failed, expected at least ${ORIGINAL_RANGES} on rs2, found ${RS2_RANGES}"
  exit 1
fi

if [ $RS1_RANGES -gt 0 ]
then
  echo "Test failed, expected 0 ranges on rs1, found ${RS1_RANGES}"
  exit 1
fi

echo "Test passed"
$HT_HOME/bin/stop-servers.sh
stop_rs2

exit 0
