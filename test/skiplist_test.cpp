#include "third_party/inlineskiplist.h"
#include "Timer.h"

// 使用跳表（Skip List）来存储 64 位无符号整数 Key，并进行并发插入和查询操作。
// 主要进行跳表的插入和查找性能测试

// Our test skip list stores 8-byte unsigned integers
typedef uint64_t Key;

// 将 uint64_t 键值转换为字符数组
// static const char *Encode(const uint64_t *key) {
//   return reinterpret_cast<const char *>(key);
// }

// Decode 函数将一个 char* 类型的键（字节数组）解码成 Key 类型（uint64_t）。
static Key Decode(const char *key) {
  Key rv;
  // memcpy 将字节数组 key 拷贝到 rv 变量中，转换为 Key 类型的值。
  memcpy(&rv, key, sizeof(Key));
  return rv;
}

// 比较器类，定义了如何比较 Key 类型的元素。它的作用是使跳表能够正确排序。
struct TestComparator {
  // DecodedType 即 Key 类型，是解码后的类型
  typedef Key DecodedType;

  // decode_key 静态函数用于解码字节数组。
  static DecodedType decode_key(const char *b) { return Decode(b); }

  // 重载操作符 ()，用于比较两个字节数组 a 和 b。
  int operator()(const char *a, const char *b) const {
    if (Decode(a) < Decode(b)) {
      return -1;
    } else if (Decode(a) > Decode(b)) {
      return +1;
    } else {
      return 0;
    }
  }

  // 又一个重载的 () 操作符，用于将一个字节数组与一个 DecodedType（即 Key）进行比较
  int operator()(const char *a, const DecodedType b) const {
    if (Decode(a) < b) {
      return -1;
    } else if (Decode(a) > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

int main() {
  // 创建 内存分配器对象 和 比较的对象
  Allocator alloc;
  TestComparator cmp;
  // 创建跳表，传入比较器 cmp、内存分配器 alloc 以及 跳表层高 21
  InlineSkipList<TestComparator> list(cmp, &alloc, 21);

  // 创建一个跳表的迭代器 iter，用于遍历跳表元素
  InlineSkipList<TestComparator>::Iterator iter(&list);

  // Space 要插入跳表的元素总数，设为 100000。
  // loop 是进行查询操作次数，设为 10000。
  const uint64_t Space = 100000ull;
  const int loop = 10000;
  // 处理元素插入
  for (uint64_t i = 0; i < Space; ++i) {
    // 分配空间
    auto buf = list.AllocateKey(sizeof(Key));
    // 赋值
    *(Key *)buf = i;
    // 并发插入
    bool res = list.InsertConcurrently(buf);
    // 表示返回值未被使用，避免警告
    (void)res;
  }


  // 创建 Timer 对象 t 来计时。
  Timer t;
  t.begin();
  // loop次循环查找k。
  for (int i = 0; i < loop; ++i) {
    // 随机指定查找k
    uint64_t k = rand() % Space;
    // 使用迭代器 iter.Seek 查找 k
    iter.Seek((char *)&k);
  }
  // 结束计时并打印执行时间
  t.end_print(loop);

  return 0;
}
