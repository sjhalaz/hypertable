#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=${HT_HOME}
HT_SHELL=$HT_HOME/bin/hypertable
SCRIPT_DIR=`dirname $0`
#DATA_SEED=42 # for repeating certain runs
DIGEST="openssl dgst -md5"
RS1_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs1.pid
RS2_PIDFILE=$HT_HOME/run/Hypertable.RangeServer.rs2.pid
RUN_DIR=`pwd`

. $HT_HOME/bin/ht-env.sh

gen_test_data() {
  seed=${DATA_SEED:-$$}
  size=${DATA_SIZE:-"2000000"}
  perl -e 'print "#row\tcolumn\tvalue\n"' > data.header
  perl -e 'srand('$seed'); for($i=0; $i<'$size'; ++$i) {
    printf "row%07d\tcolumn%d\tvalue%d\n", $i, int(rand(3))+1, $i
  }' > data.body
  $DIGEST < data.body > data.md5
}

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

set_tests() {
  for i in $@; do
    eval TEST_$i=1
  done
}

# Runs an individual test
run_test() {
  local TEST_ID=$1
  shift;
  
  echo "Running test $TEST_ID." >> report.txt

  stop_range_servers
  $HT_HOME/bin/start-test-servers.sh --no-master --no-rangeserver --no-thriftbroker \
                                       --clear
  $SCRIPT_DIR/master-launcher.sh $@ > master.output.$TEST_ID 2>&1 &
  sleep 10

  # give rangeserver time to get registered etc 
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS1_PIDFILE \
     --Hypertable.RangeServer.ProxyName=rs1 \
     --Hypertable.RangeServer.Port=38060 \
     --config=${SCRIPT_DIR}/test.cfg 2>1 > rangeserver.rs1.output.$TEST_ID &
  sleep 15
  $HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$RS2_PIDFILE \
    --Hypertable.RangeServer.ProxyName=rs2 \
    --Hypertable.RangeServer.Port=38061 \
    --config=${SCRIPT_DIR}/test.cfg 2>1 > rangeserver.rs2.output.$TEST_ID &

  $HT_SHELL --batch < $SCRIPT_DIR/create-test-table.hql
  if [ $? != 0 ] ; then
    echo "Unable to create table 'failover-test', exiting ..."
    exit 1
  fi

  $HT_SHELL --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=100K --batch < $SCRIPT_DIR/load.hql
  if [ $? != 0 ] ; then
    echo "Problem loading table 'failover-test', exiting ..."
    exit 1
  fi
  
  # wait a bit for splits 
  sleep 30
 $HT_SHELL --namespace 'sys' --batch --exec 'select Location from METADATA revs=1;' > locations.$TEST_ID  
  
  #kill rs1
  stop_rs1
  
  #wait for recovery to complete 
  sleep 240 
  
  #dump keys
  $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql | grep -v "hypertable" > dbdump.$TEST_ID
  if [ $? != 0 ] ; then
    echo "Problem dumping table 'failover-test', exiting ..."
    exit 1
  fi
  
  #shut everything down
  stop_rs2
  $HT_HOME/bin/stop-servers.sh

  $DIGEST < dbdump.$TEST_ID > dbdump.md5
  diff data.md5 dbdump.md5 > out
  if [ $? != 0 ] ; then
    echo "Test $TEST_ID FAILED." >> report.txt
    echo "Test $TEST_ID FAILED." >> errors.txt
    cat out >> report.txt
    touch error
    $HT_SHELL -l error --batch < $SCRIPT_DIR/dump-test-table.hql | grep -v "hypertable" > dbdump.$TEST_ID.again
    if [ $? != 0 ] ; then
        echo "Problem dumping table 'failover-test', exiting ..."
        exit 1
    fi
    #exec 1>&-
    #sleep 86400
  else
    echo "Test $TEST_ID PASSED." >> report.txt
  fi
 
}

if [ "$TEST_0" ] || [ "$TEST_1" ] ; then
    rm -f errors.txt
fi

rm -f report.txt

gen_test_data

env | grep '^TEST_[1-9][0-9]\?=' || set_tests 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 

[ "$TEST_1" ] && run_test 1 "--induce-failure=recover-server-ranges-root-1:exit:0"
[ "$TEST_2" ] && run_test 2 "--induce-failure=recover-server-ranges-root-4:exit:0"
[ "$TEST_3" ] && run_test 3 "--induce-failure=recover-server-ranges-root-7:exit:0"
[ "$TEST_4" ] && run_test 4 "--induce-failure=recover-server-ranges-root-10:exit:0"
[ "$TEST_5" ] && run_test 5 "--induce-failure=recover-server-ranges-root-13:exit:0"

[ "$TEST_6" ] && run_test 6 "--induce-failure=recover-server-ranges-metadata-1:exit:0"
[ "$TEST_7" ] && run_test 7 "--induce-failure=recover-server-ranges-metadata-4:exit:0"
[ "$TEST_8" ] && run_test 8 "--induce-failure=recover-server-ranges-metadata-7:exit:0"
[ "$TEST_9" ] && run_test 9 "--induce-failure=recover-server-ranges-metadata-10:exit:0"
[ "$TEST_10" ] && run_test 10 "--induce-failure=recover-server-ranges-metadata-13:exit:0"

[ "$TEST_11" ] && run_test 11 "--induce-failure=recover-server-ranges-user-1:exit:0"
[ "$TEST_12" ] && run_test 12 "--induce-failure=recover-server-ranges-user-4:exit:0"
[ "$TEST_13" ] && run_test 13 "--induce-failure=recover-server-ranges-user-7:exit:0"
[ "$TEST_14" ] && run_test 14 "--induce-failure=recover-server-ranges-user-10:exit:0"
[ "$TEST_15" ] && run_test 15 "--induce-failure=recover-server-ranges-user-13:exit:0"

[ "$TEST_16" ] && run_test 16 "--induce-failure=recover-server-1:exit:0"
[ "$TEST_17" ] && run_test 17 "--induce-failure=recover-server-2:exit:0"
[ "$TEST_18" ] && run_test 18 "--induce-failure=recover-server-3:exit:0"
[ "$TEST_18" ] && run_test 19 "--induce-failure=recover-server-4:exit:0"
[ "$TEST_20" ] && run_test 20 "--induce-failure=recover-server-5:exit:0"
[ "$TEST_21" ] && run_test 21 "--induce-failure=recover-server-6:exit:0"

if [ -e errors.txt ] && [ "$TEST_21" ] ; then
    ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
    mkdir $ARCHIVE_DIR
    mv core.* dbdump.* rangeserver.output.* errors.txt $ARCHIVE_DIR
fi

echo ""
echo "**** TEST REPORT ****"
echo ""
cat report.txt
grep FAILED report.txt > /dev/null && exit 1
exit 0
