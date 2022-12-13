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
#include "db.h"

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

uint64_t LOAD_SIZE;
uint64_t RUN_SIZE;

DB *db = NULL;

void GenerateWorkload(int wl,
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

    if (wl == WORKLOAD_A) {
        init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/load_randint_workloada";
        txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/txn_randint_workloada";
    } else if (wl == WORKLOAD_B) {
        init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/load_randint_workloadb";
        txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/txn_randint_workloadb";
    } else if (wl == WORKLOAD_C) {
        init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/load_randint_workloadc";
        txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/txn_randint_workloadc";
    } else if (wl == WORKLOAD_D) {
        init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/load_randint_workloadd";
        txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/txn_randint_workloadd";
    } else if (wl == WORKLOAD_E) {
        init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/load_randint_workloade";
        txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/txn_randint_workloade";
    } else if (wl == WORKLOAD_F) {
        init_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/load_randint_workloadf";
        txn_file = "/home/cc/mcsPMKV/src/tests/ycsbTracer/workloads/txn_randint_workloadf";
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

void LoadWorkload(std::vector<std::unique_ptr<DB::Worker>> &worker,
        std::vector<std::string> &loadKeys,
        std::vector<std::string> &loadVals,
        uint64_t num_threads)
{
    std::atomic<uint64_t> next_thread_id;
    next_thread_id.store(0);

    auto starttime = std::chrono::system_clock::now();
    auto func = [&]() {
        uint64_t thread_id = next_thread_id.fetch_add(1);

        uint64_t start_key = LOAD_SIZE / num_threads * thread_id;
        uint64_t end_key = start_key + LOAD_SIZE / num_threads;
        if (thread_id == num_threads - 1)
            end_key = LOAD_SIZE;

        for (uint64_t i = start_key; i < end_key; i++) {
            worker[thread_id]->Put(Slice(loadKeys[i]), Slice(loadVals[i]));
        }

        worker[thread_id]->FlushRemainAndUpdateIndex();
    };

    std::vector<std::thread> thread_group;

    for (int i = 0; i < num_threads; i++)
        thread_group.push_back(std::thread{func});

    for (int i = 0; i < num_threads; i++)
        thread_group[i].join();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    printf("Throughput: load, %f ops/us\n", ((LOAD_SIZE * 1.0) / duration.count()));
}

void RunWorkload(std::vector<std::unique_ptr<DB::Worker>> &worker,
        std::vector<std::string> &runKeys,
        std::vector<std::string> &runVals,
        std::vector<int> &ranges,
        std::vector<int> &ops,
        uint64_t num_threads)
{
    std::atomic<uint64_t> next_thread_id;
    next_thread_id.store(0);

    auto starttime = std::chrono::system_clock::now();
    auto func = [&]() {
        uint64_t thread_id = next_thread_id.fetch_add(1);

        uint64_t start_key = RUN_SIZE / num_threads * thread_id;
        uint64_t end_key = start_key + RUN_SIZE / num_threads;
        if (thread_id == num_threads - 1)
            end_key = RUN_SIZE;

        for (uint64_t i = start_key; i < end_key; i++) {
            if (ops[i] == OP_UPDATE) {
                worker[thread_id]->Put(Slice(runKeys[i]), Slice(runVals[i]));
            } else if (ops[i] == OP_READ) {
                bool found = worker[thread_id]->Get(Slice(runKeys[i]), &runVals[i]);
                if (!found) {
                    std::cout << "Key = " << runKeys[i] << " does not exist" << std::endl;
                }
            }
        }
    };

    std::vector<std::thread> thread_group;

    for (int i = 0; i < num_threads; i++)
        thread_group.push_back(std::thread{func});

    for (int i = 0; i < num_threads; i++)
        thread_group[i].join();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    printf("Throughput: run, %f ops/us\n", ((RUN_SIZE * 1.0) / duration.count()));
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
    if (argc != 7) {
        std::cout << "Usage: ./ycsb [Num load keys] [Num run keys] [Value size] [ycsb workload type] [num threads] [num cleaners]\n";
        std::cout << "Example: ./ycsb 100 100 64 f 4 4\n";
        std::cout << "Num load keys and Num run keys should be the same with the numbers in generated workloads\n";
        return 1;
    }

    printf("Num load keys: %s\nNum run keys: %s\nValue size: %s\nYcsb workload type: %s\nNum threads: %s\nNum cleaners: %s\n", argv[1], argv[2], argv[3], argv[4], argv[5], argv[6]);

    LOAD_SIZE = std::stoul(std::string(argv[1]));
    RUN_SIZE = std::stoul(std::string(argv[2]));

    uint64_t valueSize = std::stoul(std::string(argv[3]));

    int wl;
    if (strcmp(argv[4], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[4], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[4], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[4], "d") == 0) {
        wl = WORKLOAD_D;
    } else if (strcmp(argv[4], "e") == 0) {
        wl = WORKLOAD_E;
    } else if (strcmp(argv[4], "f") == 0) {
        wl = WORKLOAD_F;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    uint64_t num_threads = std::stoul(std::string(argv[5]));
    uint64_t num_cleaners = std::stoul(std::string(argv[6]));

    std::vector<std::string> loadKeys;
    std::vector<std::string> loadVals;
    std::vector<std::string> runKeys;
    std::vector<std::string> runVals;
    std::vector<int> ranges;
    std::vector<int> ops;

    std::string db_path = std::string(PMEM_DIR) + "log_kvs";
    std::filesystem::remove_all(db_path);
    std::filesystem::create_directory(db_path);

    size_t log_size = 48ul * 5 * 1024 * 1024 * 1024;
    db = new DB(db_path, log_size, num_threads, num_cleaners);

    std::vector<std::unique_ptr<DB::Worker>> worker;
    for (uint64_t i = 0; i < num_threads; i++) {
        worker.push_back(db->GetWorker());
    }

    GenerateWorkload(wl, valueSize, loadKeys, loadVals, runKeys, runVals, ranges, ops);

    LoadWorkload(worker, loadKeys, loadVals, num_threads);

    RunWorkload(worker, runKeys, runVals, ranges, ops, num_threads);

    //PrintWorkload(loadKeys, loadVals, runKeys, runVals, ranges, ops);

    return 0;
}
