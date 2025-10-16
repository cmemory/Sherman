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

// #define USE_CORO
const int kCoroCnt = 3;

int kReadRatio;
int kThreadCount;
int kNodeCount;
uint64_t kKeySpace = 64 * define::MB;
double kWarmRatio = 0.8;
double zipfan = 0;

//////////////////// workload parameters /////////////////////


extern uint64_t cache_miss[MAX_APP_THREAD][8];
extern uint64_t cache_hit[MAX_APP_THREAD][8];
extern uint64_t read_cnt;
extern uint64_t read_bytes;


std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][8];

extern uint64_t latency[MAX_APP_THREAD][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

Tree *tree;
DSM *dsm;

inline Key to_key(uint64_t k) {
    // kKeySpace 表示key的范围是 0~64*1024*1024.
  return (CityHash64((char *)&k, sizeof(k)) + 1) % kKeySpace;
}

class RequsetGenBench : public RequstGen {

public:
  RequsetGenBench(int coro_id, DSM *dsm, int id)
      : coro_id(coro_id), dsm(dsm), id(id) {
    seed = rdtsc();
    mehcached_zipf_init(&state, kKeySpace, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ id);
  }

  Request next() override {
    Request r;
    uint64_t dis = mehcached_zipf_next(&state);

    r.k = to_key(dis);
    r.v = 23;
    r.is_search = rand_r(&seed) % 100 < kReadRatio;

    tp[id][0]++;

    return r;
  }

private:
  int coro_id;
  DSM *dsm;
  int id;

  unsigned int seed;
  struct zipf_gen_state state;
};

RequstGen *coro_func(int coro_id, DSM *dsm, int id) {
  return new RequsetGenBench(coro_id, dsm, id);
}

// 测试tree索引在分布式共享内存（DSM）环境下的性能。
// 多线程与 DSM 进行交互，执行插入和查找操作，记录吞吐量（tp）和延迟（latency）等性能指标。

// Timer 实例，用于测量基准测试的时间。
Timer bench_timer;
// 所有线程完成热身（warmup）操作的数量，原子变量确保线程安全，
std::atomic<int64_t> warmup_cnt{0};
// 原子布尔变量，表示所有线程是否都准备好
std::atomic_bool ready{false};
void thread_run(int id) {

  // 将当前线程绑定到特定的 CPU 核心上，优化性能，避免线程在多个核心之间迁移。
  bindCore(id);

  // 注册线程到 DSM 系统中，初始化相关资源
  dsm->registerThread();

  // 所有线程数 = 一个节点的线程总数 * 集群节点数
  uint64_t all_thread = kThreadCount * dsm->getClusterSize();
  // 当前线程id，多节点多线程二维扁平到一维。
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;

  printf("I am thread %ld on compute nodes\n", my_id);

  // 0号线程，开始启动计时，统计热身过程时间
  if (id == 0) {
    bench_timer.begin();
  }

  // end_warm_key，热身数据的数量
  uint64_t end_warm_key = kWarmRatio * kKeySpace;
  for (uint64_t i = 1; i < end_warm_key; ++i) {
      // 线程划分方法，分线程热身数据insert。
    if (i % all_thread == my_id) {
      tree->insert(to_key(i), i * 2);
    }
  }

  // 每个线程热身数据处理完，计数器+1
  warmup_cnt.fetch_add(1);

  // ID 为 0 的主线程，输出节点完成热身操作的信息
  if (id == 0) {
      // 循环等待，确保当前节点的所有线程热身完成
    while (warmup_cnt.load() != kThreadCount)
      ;
    printf("node %d finish\n", dsm->getMyNodeID());
    // dsm->barrier("warm_finish") 进行多节点同步，确保所有节点的0号线程在此处同步完成。
    dsm->barrier("warm_finish");

    // 统计热身时间
    uint64_t ns = bench_timer.end();
    printf("warmup time %lds\n", ns / 1000 / 1000 / 1000);

    // 索引缓存统计输出
    tree->index_cache_statistics();
    tree->clear_statistics();

    // 热身完成，可以开始正式的测试。
    ready = true;

    // 重置 warmup_cnt 为 0，用于线程同步
    warmup_cnt.store(0);
  }

  // 其他线程会等待，直到主线程确定热身完成。
  while (warmup_cnt.load() != 0)
    ;

#ifdef USE_CORO
  tree->run_coroutine(coro_func, id, kCoroCnt);
#else

  // 基于 Zipf 分布生成负载模式进行数据插入
  /// without coro
  unsigned int seed = rdtsc();
  struct zipf_gen_state state;
  mehcached_zipf_init(&state, kKeySpace, zipfan,
                      (rdtsc() & (0x0000ffffffffffffull)) ^ id);

  Timer timer;
  while (true) {

    uint64_t dis = mehcached_zipf_next(&state);
    uint64_t key = to_key(dis);

    Value v;
    timer.begin();

    if (rand_r(&seed) % 100 < kReadRatio) { // GET
      tree->search(key, v);
    } else {
      v = 12;
      tree->insert(key, v);
    }

    // 纳秒数/100，相当于微秒数*10
    auto us_10 = timer.end() / 100;
    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[id][us_10]++;

    tp[id][0]++;
  }
#endif

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

void cal_latency() {
  uint64_t all_lat = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k) {
      latency_th_all[i] += latency[k][i];
    }
    all_lat += latency_th_all[i];
  }

  uint64_t th50 = all_lat / 2;
  uint64_t th90 = all_lat * 9 / 10;
  uint64_t th95 = all_lat * 95 / 100;
  uint64_t th99 = all_lat * 99 / 100;
  uint64_t th999 = all_lat * 999 / 1000;

  uint64_t cum = 0;
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    cum += latency_th_all[i];

    if (cum >= th50) {
      printf("p50 %f\t", i / 10.0);
      th50 = -1;
    }
    if (cum >= th90) {
      printf("p90 %f\t", i / 10.0);
      th90 = -1;
    }
    if (cum >= th95) {
      printf("p95 %f\t", i / 10.0);
      th95 = -1;
    }
    if (cum >= th99) {
      printf("p99 %f\t", i / 10.0);
      th99 = -1;
    }
    if (cum >= th999) {
      printf("p999 %f\n", i / 10.0);
      th999 = -1;
      return;
    }
  }
}

// 测试的时候，每个节点都需要执行这个，因为需要把当前节点注册到DSM

int main(int argc, char *argv[]) {

  // 解析命令行参数
  parse_args(argc, argv);

  // 设置配置节点数，并创建DSM对象。
  DSMConfig config;
  config.machineNR = kNodeCount;
  dsm = DSM::getInstance(config);

  // 注册当前节点线程
  dsm->registerThread();
  // 创建分布式系统 树索引
  tree = new Tree(dsm);

  // 插入数据，只有ID为0的节点才执行插入。这样多个节点只有一个节点写入数据
  if (dsm->getMyNodeID() == 0) {
    for (uint64_t i = 1; i <= 1024000; ++i) {
        // to_key 通过 hash 生成 key
      tree->insert(to_key(i), i * 2);
    }
  }
  printf("total read cnt: %llu\n", read_cnt);
  printf("total read bytes: %llu\n", read_bytes);

  // 同步操作，所有线程将在此等待，直到所有节点都执行到这。
  // 应该是主要等待数据插入完成。这里使用 memcache 中的key进行同步。
  dsm->barrier("benchmark");
  // TODO（chenqiang）重置线程，可能是在同步点后，为启动新的任务做准备。
  dsm->resetThread();

  // 创建 kThreadCount 个线程启动 thread_run 任务
  for (int i = 0; i < kThreadCount; i++) {
    th[i] = std::thread(thread_run, i);
  }

  // 等待所有线程启动执行完成后，继续后面测试
  while (!ready.load())
    ;

  // 两个时间对象，用于记录时间戳
  timespec s, e;
  // 用于存储前一次的吞吐量数据
  uint64_t pre_tp = 0;

  int count = 0;

  // 获取当前时间，保存到 s 变量
  clock_gettime(CLOCK_REALTIME, &s);
  while (true) {

    // 每隔 2 秒进行一次吞吐量统计
    sleep(2);
    // 获取当前时间，并计算从上次统计到现在经过的微秒数。
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    // 累加计算所有线程的吞吐量
    uint64_t all_tp = 0;
    for (int i = 0; i < kThreadCount; ++i) {
      all_tp += tp[i][0];
    }
    // 计算一轮增量的吞吐量，并更新 pre_tp
    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;

    // 计算查询所有线程查询缓存的总次数（hit+miss），以及hit命中次数，后面计算命中率。
    uint64_t all = 0;
    uint64_t hit = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      all += (cache_hit[i][0] + cache_miss[i][0]);
      hit += cache_hit[i][0];
    }

    // 更新统计区间的起始时间戳
    clock_gettime(CLOCK_REALTIME, &s);

    // 每 3 次统计，会调用 cal_latency 计算延时
    if (++count % 3 == 0 && dsm->getMyNodeID() == 0) {
      cal_latency();
    }

    // 计算当前节点每微秒的吞吐量。
    double per_node_tp = cap * 1.0 / microseconds;
    // 计算集群所有节点的总吞吐量。使用 mamcache 同步数据
    uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));

    printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

    // 0号节点 打印 集群吞吐量 和 缓存命中率
    if (dsm->getMyNodeID() == 0) {
      printf("cluster throughput %.3f\n", cluster_tp / 1000.0);
      printf("cache hit rate: %lf\n", hit * 1.0 / all);
    }
  }

  return 0;
}