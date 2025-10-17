#include "Timer.h"
#include "Tree.h"
#include "zipf.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>


//////////////////// workload parameters /////////////////////

int kReadRatio;
int kThreadCount;
int kNodeCount;
uint64_t kKeySpace = 64 * define::MB;

//////////////////// workload parameters /////////////////////

extern uint64_t read_cnt;
extern uint64_t read_bytes;
extern uint64_t write_cnt;
extern uint64_t write_bytes;
extern uint64_t cas_cnt;

Tree *tree;
DSM *dsm;

inline Key to_key(uint64_t k) {
    // kKeySpace 表示key的范围是 0~64*1024*1024.
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

void parse_args(int argc, char *argv[]) {
  if (argc != 4) {
    printf("Usage: ./benchmark kNodeCount kReadRatio kThreadCount\n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kReadRatio = atoi(argv[2]);
  kThreadCount = atoi(argv[3]);

  printf("kNodeCount %d, kReadRatio %d, kThreadCount %d\n", kNodeCount,
         kReadRatio, kThreadCount);
}

int main(int argc, char *argv[]) {

  // 解析命令行参数
  parse_args(argc, argv);
  std::srand(static_cast<unsigned int>(std::time(0)));

  // 设置配置节点数，并创建DSM对象。
  DSMConfig config;
  config.machineNR = kNodeCount;
  dsm = DSM::getInstance(config);

  // 注册当前节点线程
  dsm->registerThread();

  // 创建分布式系统 树索引
  tree = new Tree(dsm);
  for (uint64_t i = 1; i <= 2000000; ++i) {
      // to_key 通过 hash 生成 key
      uint64_t random_num = 1 + rand() % 2000000;
      tree->insert(to_key(random_num), i * 2);
  }

  printf("total read cnt: %llu\n", read_cnt);
  printf("total read bytes: %llu\n", read_bytes);
  printf("total write cnt: %llu\n", write_cnt);
  printf("total write bytes: %llu\n", write_bytes);
  printf("total cas cnt: %llu\n", cas_cnt);

    while (true) {
        sleep(1000);
    }
}