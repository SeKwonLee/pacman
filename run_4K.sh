#!/bin/bash

#Usage: ./ycsb [Num load keys] [Num run keys] [Value size] [ycsb workload type] [num threads] [num cleaners]

#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_flatstore 50331648 50331648 256 a 44 4 &>> ../../flatstore_results.txt
#printf "\n" &>> ../../flatstore_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_flatstore 50331648 50331648 256 b 44 4 &>> ../../flatstore_results.txt
#printf "\n" &>> ../../flatstore_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_flatstore 50331648 50331648 256 c 44 4 &>> ../../flatstore_results.txt
#printf "\n" &>> ../../flatstore_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_flatstore 50331648 50331648 256 d 44 4 &>> ../../flatstore_results.txt
#printf "\n" &>> ../../flatstore_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_flatstore 50331648 50331648 256 f 44 4 &>> ../../flatstore_results.txt
#printf "\n" &>> ../../flatstore_results.txt
#printf "\n" &>> ../../flatstore_results.txt

#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_viper 50331648 50331648 256 a 44 4 &>> ../../../viper_results.txt
#printf "\n" &>> ../../../viper_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_viper 50331648 50331648 256 b 44 4 &>> ../../../viper_results.txt
#printf "\n" &>> ../../../viper_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_viper 50331648 50331648 256 c 44 4 &>> ../../../viper_results.txt
#printf "\n" &>> ../../../viper_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_viper 50331648 50331648 256 d 44 4 &>> ../../../viper_results.txt
#printf "\n" &>> ../../../viper_results.txt
#sudo numactl --membind=0 --cpunodebind=0 ./ycsb_viper 50331648 50331648 256 f 44 4 &>> ../../../viper_results.txt
#printf "\n" &>> ../../../viper_results.txt
#printf "\n" &>> ../../../viper_results.txt

#for workload in a b c d f
#do
#    for nthreads in 1 2 4 8 16 32 44
#    do
#        sudo numactl --membind=0 --cpunodebind=0 ./ycsb_flatstore 50331648 50331648 256 ${workload} ${nthreads} 4 &>> ../../flatstore_scale_results.txt
#        printf "\n" &>> ../../flatstore_scale_results.txt
#    done
#done

#for workload in a b c d f
#do
#    for nthreads in 1 2 4 8 16 32 44
#    do
#        sudo numactl --membind=0 --cpunodebind=0 ./ycsb_viper 50331648 50331648 256 ${workload} ${nthreads} 4 &>> ../../../viper_scale_results.txt
#        printf "\n" &>> ../../../viper_scale_results.txt
#    done
#done

#for workload in a b c d f
#do
#    for nthreads in 1 2 4 8 16 32 44
#    do
#        sudo numactl --membind=0 --cpunodebind=0 ./ycsb_flatstore 50331648 50331648 1024 ${workload} ${nthreads} 4 &>> ../../flatstore_scale_1KB_results.txt
#        printf "\n" &>> ../../flatstore_scale_1KB_results.txt
#    done
#done

#for workload in a b c d f
#do
#    for nthreads in 1 2 4 8 16 32 44
#    do
#        sudo numactl --membind=0 --cpunodebind=0 ./ycsb_viper 50331648 50331648 1024 ${workload} ${nthreads} 4 &>> ../../../viper_scale_1KB_results.txt
#        printf "\n" &>> ../../../viper_scale_1KB_results.txt
#    done
#done

kvs_name=$1

for valuesize in 4096
do
    for nthreads in 1 2 4 8 16 32 44
    do
        sudo rm -rf /mnt/pmem0/*
        sudo numactl --membind=0 --cpunodebind=0 ./ycsb_${kvs_name} 10000000 10000000 ${valuesize} a ${nthreads} 4 &>> /home/cc/pacman/${kvs_name}_scale_${valuesize}_results.txt
        printf "\n" &>> /home/cc/pacman/${kvs_name}_scale_${valuesize}_results.txt
    done
done
