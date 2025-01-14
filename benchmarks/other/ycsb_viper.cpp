#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>

#include <string>
#include <thread>
#include <filesystem>

#include "config.h"
#include "viper/viper.hpp"

using namespace std;

enum {
    OP_INSERT,
    OP_UPDATE,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_D,
    WORKLOAD_E,
    WORKLOAD_F,
};

enum {
    ZIPFIAN,
    UNIFORM,
    LATEST,
};

uint64_t LOAD_SIZE;
uint64_t RUN_SIZE;

std::unique_ptr<viper::Viper<std::string, std::string>> db_;

void GenerateWorkload(int wd, int wl,
        uint64_t valueSize,
        std::vector<std::string> &loadKeys,
        std::vector<std::string> &loadVals,
        std::vector<std::string> &runKeys,
        std::vector<std::string> &runVals,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    std::string init_file;
    std::string txn_file;

    if (wd == ZIPFIAN) {
        if (wl == WORKLOAD_A) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/load_randint_workloada";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/txn_randint_workloada";
        } else if (wl == WORKLOAD_B) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/load_randint_workloadb";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/txn_randint_workloadb";
        } else if (wl == WORKLOAD_C) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/load_randint_workloadc";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/txn_randint_workloadc";
        } else if (wl == WORKLOAD_E) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/load_randint_workloade";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/txn_randint_workloade";
        } else if (wl == WORKLOAD_F) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/load_randint_workloadf";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/zipf/txn_randint_workloadf";
        } else {
            std::cout << "Unknown workload type" << std::endl;
            return ;
        }
    } else if (wd == UNIFORM) {
        if (wl == WORKLOAD_A) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/load_randint_workloada";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/txn_randint_workloada";
        } else if (wl == WORKLOAD_B) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/load_randint_workloadb";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/txn_randint_workloadb";
        } else if (wl == WORKLOAD_C) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/load_randint_workloadc";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/txn_randint_workloadc";
        } else if (wl == WORKLOAD_E) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/load_randint_workloade";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/txn_randint_workloade";
        } else if (wl == WORKLOAD_F) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/load_randint_workloadf";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/unif/txn_randint_workloadf";
        } else {
            std::cout << "Unknown workload type" << std::endl;
            return ;
        }
    } else if (wd == LATEST) {
        if (wl == WORKLOAD_D) {
            init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/latest/load_randint_workloadd";
            txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/latest/txn_randint_workloadd";
        } else {
            std::cout << "Unknown workload type" << std::endl;
            return ;
        }
    } else {
        std::cout << "Unknown workload type" << std::endl;
        return ;
    }

    std::string op;
    std::string key;
    int range;

    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");
    std::string scan("SCAN");

    uint64_t count = 0;
    uint64_t val;

    std::ifstream infile_load(init_file);
    // As it is datasets for load (all the ops are insert),
    // we do not store the operation type separately.
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return ;
        }

        if (key.length() < 19) {
            key = std::string(19 - key.length(), '0') + key;
        }
        key = key.substr(0, 8);

        loadKeys.push_back(key);
        loadVals.push_back(std::string(valueSize, '0'));
        count++;
    }

    printf("Loaded %lu keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;

        if (key.length() < 19) {
            key = std::string(19 - key.length(), '0') + key;
        }
        key = key.substr(0, 8);

        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            runKeys.push_back(key);
            runVals.push_back(std::string(valueSize, '0'));
            ranges.push_back(1);
        } else if (op.compare(update) == 0) {
            ops.push_back(OP_UPDATE);
            runKeys.push_back(key);
            runVals.push_back(std::string(valueSize, '0'));
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            runKeys.push_back(key);
            runVals.push_back(std::string(""));
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            runKeys.push_back(key);
            runVals.push_back(std::string(""));
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

    printf("Run %lu keys\n", count);
}

void LoadWorkload(std::vector<std::unique_ptr<viper::Viper<std::string, std::string>::ClientWrapper>> &client,
        std::vector<std::string> &loadKeys,
        std::vector<std::string> &loadVals,
        uint64_t num_threads,
        int wl, int wd)
{
    std::atomic<uint64_t> next_thread_id;
    next_thread_id.store(0);

    std::vector<std::thread> thread_group;
    std::vector<std::vector<double>> latency_maps;

    for (int i = 0; i < num_threads; i++)
        latency_maps.push_back(std::vector<double>());
    for (int i = 0; i < num_threads; i++)
        latency_maps[i].reserve(LOAD_SIZE / num_threads);

    auto starttime = std::chrono::system_clock::now();
    auto func = [&]() {
        uint64_t thread_id = next_thread_id.fetch_add(1);

        uint64_t start_key = LOAD_SIZE / num_threads * thread_id;
        uint64_t end_key = start_key + LOAD_SIZE / num_threads;
        if (thread_id == num_threads - 1)
            end_key = LOAD_SIZE;

        for (uint64_t i = start_key; i < end_key; i++) {
            auto op_start = std::chrono::system_clock::now();
            client[thread_id]->put(loadKeys[i], loadVals[i]);
            auto op_end = std::chrono::system_clock::now();
            double op_latency = (double) std::chrono::duration_cast<std::chrono::nanoseconds>(
                    op_end - op_start).count();
            latency_maps[thread_id].push_back(op_latency);
        }
    };

    for (int i = 0; i < num_threads; i++)
        thread_group.push_back(std::thread{func});

    for (int i = 0; i < num_threads; i++)
        thread_group[i].join();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    std::vector<double> combined_latency_map;
    combined_latency_map.reserve(LOAD_SIZE);
    for (int i = 0; i < num_threads; i++) {
        combined_latency_map.insert(combined_latency_map.end(), latency_maps[i].begin(), latency_maps[i].end());
    }
    std::sort(combined_latency_map.begin(), combined_latency_map.end());

    double average_latency = (double) std::accumulate(combined_latency_map.begin(), combined_latency_map.end(), 0.0) / (double)(LOAD_SIZE * 1.0);
    double median_latency = combined_latency_map[trunc((double)combined_latency_map.size() * 0.50)];
    double tail_latency_90 = combined_latency_map[trunc((double)combined_latency_map.size() * 0.90)];
    double tail_latency_95 = combined_latency_map[trunc((double)combined_latency_map.size() * 0.95)];
    double tail_latency_99 = combined_latency_map[trunc((double)combined_latency_map.size() * 0.99)];

    //for (int i = 0; i < combined_latency_map.size(); i++) {
    //    printf("%f\n", combined_latency_map[i]);
    //}
    printf("Throughput: load, %f ops/us\n", ((LOAD_SIZE * 1.0) / duration.count()));
    printf("Average latency = %f ns\n", average_latency);
    printf("50th median latency = %f ns\n", median_latency);
    printf("90th tail latency = %f ns\n", tail_latency_90);
    printf("95th tail latency = %f ns\n", tail_latency_95);
    printf("99th tail latency = %f ns\n", tail_latency_99);
}

void RunWorkload(std::vector<std::unique_ptr<viper::Viper<std::string, std::string>::ClientWrapper>> &client,
        std::vector<std::string> &runKeys,
        std::vector<std::string> &runVals,
        std::vector<int> &ranges,
        std::vector<int> &ops,
        uint64_t num_threads)
{
    std::atomic<uint64_t> next_thread_id;
    next_thread_id.store(0);

    std::vector<std::thread> thread_group;
    std::vector<std::vector<double>> latency_maps;

    for (int i = 0; i < num_threads; i++)
        latency_maps.push_back(std::vector<double>());
    for (int i = 0; i < num_threads; i++)
        latency_maps[i].reserve(RUN_SIZE / num_threads);

    auto starttime = std::chrono::system_clock::now();
    auto func = [&]() {
        uint64_t thread_id = next_thread_id.fetch_add(1);

        uint64_t start_key = RUN_SIZE / num_threads * thread_id;
        uint64_t end_key = start_key + RUN_SIZE / num_threads;
        if (thread_id == num_threads - 1)
            end_key = RUN_SIZE;

        for (uint64_t i = start_key; i < end_key; i++) {
            auto op_start = std::chrono::system_clock::now();
            if (ops[i] == OP_UPDATE || ops[i] == OP_INSERT) {
                client[thread_id]->put(runKeys[i], runVals[i]);
            } else if (ops[i] == OP_READ) {
                bool found = client[thread_id]->get(runKeys[i], &runVals[i]);
                if (!found) {
                    std::cout << "Key = " << runKeys[i] << " does not exist" << std::endl;
                }
            }
            auto op_end = std::chrono::system_clock::now();
            double op_latency = (double) std::chrono::duration_cast<std::chrono::nanoseconds>(
                    op_end - op_start).count();
            latency_maps[thread_id].push_back(op_latency);
        }
    };

    for (int i = 0; i < num_threads; i++)
        thread_group.push_back(std::thread{func});

    for (int i = 0; i < num_threads; i++)
        thread_group[i].join();

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    std::vector<double> combined_latency_map;
    combined_latency_map.reserve(RUN_SIZE);
    for (int i = 0; i < num_threads; i++) {
        combined_latency_map.insert(combined_latency_map.end(), latency_maps[i].begin(), latency_maps[i].end());
    }
    std::sort(combined_latency_map.begin(), combined_latency_map.end());

    double average_latency = (double) std::accumulate(combined_latency_map.begin(), combined_latency_map.end(), 0.0) / (double)(RUN_SIZE * 1.0);
    double median_latency = combined_latency_map[trunc((double)combined_latency_map.size() * 0.50)];
    double tail_latency_90 = combined_latency_map[trunc((double)combined_latency_map.size() * 0.90)];
    double tail_latency_95 = combined_latency_map[trunc((double)combined_latency_map.size() * 0.95)];
    double tail_latency_99 = combined_latency_map[trunc((double)combined_latency_map.size() * 0.99)];

    //for (int i = 0; i < combined_latency_map.size(); i++) {
    //    printf("%f\n", combined_latency_map[i]);
    //}
    printf("Throughput: run, %f ops/us\n", ((RUN_SIZE * 1.0) / duration.count()));
    printf("Average latency = %f ns\n", average_latency);
    printf("50th median latency = %f ns\n", median_latency);
    printf("90th tail latency = %f ns\n", tail_latency_90);
    printf("95th tail latency = %f ns\n", tail_latency_95);
    printf("99th tail latency = %f ns\n", tail_latency_99);
}

void PrintWorkload(std::vector<std::string> &loadKeys,
        std::vector<std::string> &loadVals,
        std::vector<std::string> &runKeys,
        std::vector<std::string> &runVals,
        std::vector<int> &ranges,
        std::vector<int> &ops)
{
    for (uint64_t i = 0; i < LOAD_SIZE; i++) {
        std::cout << "[INSERT] " << "Key = " << loadKeys[i] << " valueSize = " << loadVals[i].size() << std::endl;
    }

    for (uint64_t i = 0; i < RUN_SIZE; i++) {
        if (ops[i] == OP_INSERT) {
            std::cout << "[INSERT] " << "Key = " << runKeys[i] << " valueSize = " << runVals[i].size() << std::endl;
        } else if (ops[i] == OP_UPDATE) {
            std::cout << "[UPDATE] " << "Key = " << runKeys[i] << " valueSize = " << runVals[i].size() << std::endl;
        } else if (ops[i] == OP_READ) {
            std::cout << "[READ] " << "Key = " << runKeys[i] << " valueSize = " << runVals[i].size() << std::endl;
        } else if (ops[i] == OP_SCAN) {
            std::cout << "[SCAN] " << "Key = " << runKeys[i] << " valueSize = " << runVals[i].size() << std::endl;
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 8) {
        std::cout << "Usage: ./ycsb [Num load keys] [Num run keys] [Value size] [workload pattern] [ycsb workload type] [num threads] [num cleaners]\n";
        std::cout << "Example: ./ycsb 100 100 64 zipf f 4 4\n";
        std::cout << "Num load keys and Num run keys should be the same with the numbers in generated workloads\n";
        return 1;
    }

    printf("Num load keys: %s\nNum run keys: %s\nValue size: %s\nYcsb workload pattern: %s\nYcsb workload type: %s\nNum threads: %s\nNum cleaners: %s\n", argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7]);

    LOAD_SIZE = std::stoul(std::string(argv[1]));
    RUN_SIZE = std::stoul(std::string(argv[2]));

    uint64_t valueSize = std::stoul(std::string(argv[3]));

    int wd;
    if (strcmp(argv[4], "zipf") == 0) {
        wd = ZIPFIAN;
    } else if (strcmp(argv[4], "unif") == 0) {
        wd = UNIFORM;
    } else if (strcmp(argv[4], "latest") == 0) {
        wd = LATEST;
    } else {
        fprintf(stderr, "Unknown workload pattern: %s\n", argv[4]);
        exit(1);
    }

    int wl;
    if (strcmp(argv[5], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[5], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[5], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[5], "d") == 0) {
        wl = WORKLOAD_D;
    } else if (strcmp(argv[5], "e") == 0) {
        wl = WORKLOAD_E;
    } else if (strcmp(argv[5], "f") == 0) {
        wl = WORKLOAD_F;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[5]);
        exit(1);
    }

    uint64_t num_threads = std::stoul(std::string(argv[6]));
    uint64_t num_cleaners = std::stoul(std::string(argv[7]));

    std::vector<std::string> loadKeys;
    std::vector<std::string> loadVals;
    std::vector<std::string> runKeys;
    std::vector<std::string> runVals;
    std::vector<int> ranges;
    std::vector<int> ops;

    viper::ViperConfig v_config;
    v_config.num_client_threads = num_threads;
    v_config.num_reclaim_threads = num_cleaners;
    if (num_cleaners == 0) {
        v_config.enable_reclamation = false;
    }

    std::string db_path = std::string(PMEM_DIR) + "log_kvs";
    std::filesystem::remove_all(db_path);
    std::filesystem::create_directory(db_path);

    size_t log_size = 48ul * 2 * 1024 * 1024 * 1024;
    db_ = viper::Viper<std::string, std::string>::create(db_path, log_size, v_config);

    std::vector<std::unique_ptr<viper::Viper<std::string, std::string>::ClientWrapper>> client;
    for (uint64_t i = 0; i < num_threads; i++) {
        client.push_back(db_->get_client_ptr());
    }

    GenerateWorkload(wd, wl, valueSize, loadKeys, loadVals, runKeys, runVals, ranges, ops);

    LoadWorkload(client, loadKeys, loadVals, num_threads, wl, wd);

    RunWorkload(client, runKeys, runVals, ranges, ops, num_threads);

    //PrintWorkload(loadKeys, loadVals, runKeys, runVals, ranges, ops);

    return 0;
}
