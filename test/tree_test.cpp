#include "DSM.h"
#include "Tree.h"

// 测试分布式共享内存（DSM）系统中的 Tree 数据结构。
// 核心任务包括插入、删除和查询操作，同时验证查询结果。
int main() {

  // 设置两个节点配置，并基于配置创建DSM对象。
  // NOTE(chenqiang) 配置两个节点的话，每个节点上都需要创建DSM实例并注册线程。
  DSMConfig config;
  config.machineNR = 2;
  DSM *dsm = DSM::getInstance(config);

  // DSM中注册线程
  dsm->registerThread();

  // 创建一个基于DSM的 Tree对象
  auto tree = new Tree(dsm);

  Value v;

  // 初始化节点完成会返回非0的ID，这里等待DSM中当前节点初始化完成。
  // myNodeID 是在 DSM 创建中 initRDMAConnection 后赋值的。
  // 理论上前面注册线程里也会用到这个id，所以应该放前面检测。
  if (dsm->getMyNodeID() != 0) {
    while (true)
      ;
  }

  // 第一遍插入元素
  for (uint64_t i = 1; i < 10240; ++i) {
    tree->insert(i, i * 2);
  }

  // 第二遍插入元素，新值覆盖。并发插入到分布式索引上。理论上最终值都是 i * 3
  for (uint64_t i = 10240 - 1; i >= 1; --i) {
    tree->insert(i, i * 3);
  }

  // 遍历查询检测最值值是否都是 i * 3
  for (uint64_t i = 1; i < 10240; ++i) {
    auto res = tree->search(i, v);
    assert(res && v == i * 3);
    std::cout << "search result:  " << res << " v: " << v << std::endl;
  }

  // 删除元素
  for (uint64_t i = 1; i < 10240; ++i) {
    tree->del(i);
  }

  // 遍历元素，检查是否都删了。判断删除功能是否正确。
  for (uint64_t i = 1; i < 10240; ++i) {
    auto res = tree->search(i, v);
    std::cout << "search result:  " << res << std::endl;
  }

  // 重新插入元素，值为 1 * 3
  for (uint64_t i = 10240 - 1; i >= 1; --i) {
    tree->insert(i, i * 3);
  }

  // 再次查询插入的数据，验证是否正确插入
  for (uint64_t i = 1; i < 10240; ++i) {
    auto res = tree->search(i, v);
    assert(res && v == i * 3);
    std::cout << "search result:  " << res << " v: " << v << std::endl;
  }

  printf("Hello\n");

  while (true)
    ;
}