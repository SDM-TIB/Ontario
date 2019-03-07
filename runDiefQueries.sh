#!/usr/bin/env bash

if [ "$#" -lt 5 ]; then
    echo "Usage: $0 [query_folder] [config_file] [result_folder] [result_file_name] [errors_file_name]"
    exit 1
fi

echo -e  "qname\tdecompositionTime\tplanningTime\tfirstResult\toverallExecTime\tstatus\tcardinality" >> $4

for query in `ls -v $1/*`; do
    (timeout -s 12 600 run_dief_experiment.py -c $2 -q $query -r $3) 2>> $5 >> $4;
    # kill any remaining processes
    killall -9 --quiet run_dief_experiment.py
done;