#include <iostream>
#include <string>
#include <filesystem>

#include "config.h"
#include "db.h"

int main() {
  size_t log_size = 1ul << 30;
  int num_workers = 1;
  int num_cleaners = 1;

  std::string db_path = std::string(PMEM_DIR) + "log_kvs";
  std::filesystem::remove_all(db_path);
  std::filesystem::create_directory(db_path);

  DB *db = new DB(db_path, log_size, num_workers, num_cleaners);
  std::unique_ptr<DB::Worker> worker = db->GetWorker();

  for (uint64_t i = 0; i < 100; i++) {
      uint64_t key = i;
      std::string value = "hello world";
      worker->Put(Slice((const char *)&key, sizeof(uint64_t)), Slice(value));
  }

  for (uint64_t i = 0; i < 100; i++) {
      uint64_t key = i;
      std::string val;
      bool found = worker->Get(Slice((const char *)&key, sizeof(uint64_t)), &val);
      if (!found)
          std::cout << "Key " << key << " does not exist" << std::endl;
      std::cout << "Key " << key << " value: " << val << std::endl;
  }
  return 0;
}
