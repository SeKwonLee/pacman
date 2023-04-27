#!/bin/bash

# Compile instructions
#cmake -DCMAKE_BUILD_TYPE=Release -DIDX_PERSISTENT=OFF -DPACMAN=ON ..; make -j

#Usage: ./ycsb [Num load keys] [Num run keys] [Value size] [workload pattern] [ycsb workload type] [num threads] [num cleaners]

for pattern in unif zipf
do
    for workload in a b c f
    do
        for nthreads in 16
        do
            sudo rm -rf /mnt/pmem0/*
            sudo numactl --membind=0 --cpunodebind=0 ./example/ycsb_flatstore 50331648 50331648 256 ${pattern} ${workload} ${nthreads} 4 &>> ../results/flatstore_results.txt
            printf "\n" &>> ../results/flatstore_results.txt
        done
    done
done

for pattern in latest
do
    for workload in d
    do
        for nthreads in 16
        do
            sudo rm -rf /mnt/pmem0/*
            sudo numactl --membind=0 --cpunodebind=0 ./example/ycsb_flatstore 50331648 50331648 256 ${pattern} ${workload} ${nthreads} 4 &>> ../results/flatstore_results.txt
            printf "\n" &>> ../results/flatstore_results.txt
        done
    done
done

for pattern in unif zipf
do
    for workload in a b c f
    do
        for nthreads in 16
        do
            sudo rm -rf /mnt/pmem0/*
            sudo numactl --membind=0 --cpunodebind=0 ./benchmarks/other/ycsb_viper 50331648 50331648 256 ${pattern} ${workload} ${nthreads} 4 &>> ../results/viper_results.txt
            printf "\n" &>> ../results/viper_results.txt
        done
    done
done

for pattern in latest
do
    for workload in d
    do
        for nthreads in 16
        do
            sudo rm -rf /mnt/pmem0/*
            sudo numactl --membind=0 --cpunodebind=0 ./benchmarks/other/ycsb_viper 50331648 50331648 256 ${pattern} ${workload} ${nthreads} 4 &>> ../results/viper_results.txt
            printf "\n" &>> ../results/viper_results.txt
        done
    done
done

for pattern in unif zipf
do
    for workload in a b c f
    do
        for nthreads in 16
        do
            sudo rm -rf /mnt/pmem0/*
            sudo numactl --membind=0 --cpunodebind=0 ./benchmarks/other/ycsb_chameleondb 50331648 50331648 256 ${pattern} ${workload} ${nthreads} 4 &>> ../results/chameleondb_results.txt
            printf "\n" &>> ../results/chameleondb_results.txt
        done
    done
done

for pattern in latest
do
    for workload in d
    do
        for nthreads in 16
        do
            sudo rm -rf /mnt/pmem0/*
            sudo numactl --membind=0 --cpunodebind=0 ./benchmarks/other/ycsb_chameleondb 50331648 50331648 256 ${pattern} ${workload} ${nthreads} 4 &>> ../results/chameleondb_results.txt
            printf "\n" &>> ../results/chameleondb_results.txt
        done
    done
done
