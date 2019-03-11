#!/usr/bin/env bash

if [ "$#" -lt 6 ]; then
    echo "Usage: $0 [query_folder] [config_file] [result_folder] [result_file_name] [errors_file_name] [joinLocally]"
    exit 1
fi

echo -e  "qname\tdecompositionTime\tplanningTime\tfirstResult\toverallExecTime\tstatus\tcardinality" >> $4

#for query in `ls -v $1/*`; do
#    (timeout -s 12 600 run_dief_experiment.py -c $2 -q $query -r $3) 2>> $5 >> $4;
#    # kill any remaining processes
#    killall -9 --quiet run_dief_experiment.py
#done;
RES_DIR=$3
for n in {1..3}; do
  mkdir -p $RES_DIR/"exec_res_$n"
  for query in `ls -v $1/*`; do
      (timeout -s 12 300 run_dief_experiment.py -c $2 -q $query -r $RES_DIR/"exec_res_$n" -t MULDER -s True -j $6 ) 2>> $5 >> $4;
      killall -9 --quiet run_dief_experiment.py
  done
mv ontario.log $RES_DIR/ontario_$n.log
done
