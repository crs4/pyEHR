#!/bin/sh
# launches iter times the script hardcoded with the query file given
# and writes the output to the directory 
# set through DIR_SCR environment variable or through input argument
# and the name given plus the underscore and the iter
# the conf files is taken from the SERVICE_CONFIG_FILE environment variable
# first argument query_file
# second argument iter
# third argument output_file
# optional fourth argument directory for the output_file
if [ $# -lt 3 ] || [ $# -gt 4 ]
then
echo "usage: $0 query_file iter outputfile [output_dir]"
echo "output_dir can also be set through DIR_SCR environment variable"
exit 1
fi

if [ -z "$SERVICE_CONFIG_FILE" ] 
then
echo "SERVICE_CONFIG_FILE  must be set before using this script"
exit 1
fi


QUERY_FILE=$1
ITER=$2
OUTPUT_FILE=$3
if [ $# -eq 4 ]
then
OUTPUT_DIR=$4
else
if [ -z "$DIR_SCR" ]
then
echo "DIR_SCR not set. Assuming current dir"
OUTPUT_DIR="."
else
OUTPUT_DIR=$DIR_SCR
fi
fi

echo "query file  = $QUERY_FILE"
echo "iter        = $ITER"
echo "output_file = $OUTPUT_FILE"
echo "output_dir  = $OUTPUT_DIR"

for i in `seq $ITER`
do
echo "----------------------------------------------------------------"
echo "iter=$i"
pippo=`python ./prova.py --queries_file $QUERY_FILE --query_processes 16 --log_file $OUTPUT_DIR/$OUTPUT_FILE.log.$i --pyehr_config $SERVICE_CONFIG_FILE --results_file $OUTPUT_DIR/$OUTPUT_FILE.res.$i`
echo $pippo
done
