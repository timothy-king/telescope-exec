#!/bin/bash

# How to run the runner:
# ./runner.sh <file> <cmd> <post> <postarg> <arg0> <arg1> .. <argN>
# The script then runs:
#  cmd <arg0> <arg1> .. <argN> <file> > $OUT
# and if successful formats the output (both stdout and stderr) using
#  post <postarg> $OUT


file=$1
cmd=$2
post=$3
stats_selected=$4
shift; shift; shift; shift

arguments="$@"

OUTFILE=cmd.out.$$
POSTFILE=post.out.$$

$cmd $arguments $file >& $OUTFILE # both stdout and stderr will be captured
cmd_status=$?

function finish {
  # Your cleanup code here
  rm -f $OUTFILE $POSTFILE
}
trap finish EXIT
if [ $cmd_status -eq 0 ]
then
    $post $stats_selected $OUTFILE > $POSTFILE
    post_status=$?
    if [ $post_status -eq 0 ]
    then
        cat $POSTFILE
        exit 0
    else
        echo error
        echo post_process_status $post_status
        cat $OUTFILE
        exit $post_status
    fi
else
    echo error
    cat $OUTFILE
    exit $cmd_status
fi