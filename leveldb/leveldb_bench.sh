# A bench marking tool to execute different YCSB workload on Leveldb
plogdir=logs/workloads
# Execute for all workloads
for workload in a b c d e f; do
        logdir="$plogdir/workload${workload}";
        mkdir -p $logdir;
        echo "## WORKLOAD: $workload ##";
        # Support 3 naming schems: row major, column major and full row
        # Details in Bigtable_Rados.pdf.
        for format in rowmajor colmajor fullrow; do
            dbpath="/tmp/$format";
            rm -rf $dbpath
            echo "Format: $format"
            echo "Loading ...";
            workloadfile=workloads/workload${workload};
            # Creating of LevelDB database
            bin/ycsb load leveldb -P $workloadfile -p "leveldb.path=$dbpath" -p format=$format -p dump=true &> $logdir/$format.load.out

            run_cmd="bin/ycsb run leveldb -P workloads/workload${workload} -p "leveldb.path=$dbpath" -p format=$format"
            echo "All columns 1 row";
            $run_cmd &> $logdir/$format.allcols.1row.out

            # Row Scan with differnt number of columns picked from each row
            for i in 1 2 4 6; do
                echo "$i cols 1 row"
                $run_cmd -p numcols=$i &> $logdir/$format.${i}cols.1row.${workload}.out
            done

            # Column Scan: Read all rows in the table but alter number of columns.
            for i in 1 4; do
                echo "Column scan $i column"
                $run_cmd -p allrows=true -p numcols=$i &> $logdir/${format}.${i}colscan.out
            done
        done
done
