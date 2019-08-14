#!/usr/bin/env bash

if [ "$#" -lt 6 ]; then
    echo "Usage: $0 [query_folder] [config_file] [result_file_name] [errors_file_name] [planonlyTrueorFalse] [printResultsTrueorFalse] "
    exit 1
fi

echo -e  "qname\tdecompositionTime\tplanningTime\tfirstResult\toverallExecTime\tstatus\tcardinality" >> $3

for query in `ls -v $1/*`; do
    echo $2;
    echo $query;
    (timeout -s 12 600 runExperiment.py -c $2 -q ${query}  -p $5 -r $6) 2>> $4 >> $3;

    # kill any remaining processes
    killall -9 --quiet runExperiment.py
done;
