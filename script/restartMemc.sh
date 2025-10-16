#!/bin/bash

addr=$(head -1 ../memcached.conf)
port=$(awk 'NR==2{print}' ../memcached.conf)

# kill old me
ssh ${addr} -o StrictHostKeyChecking=no "cat /tmp/memcached.pid | xargs kill"

# launch memcached
ssh ${addr} -o StrictHostKeyChecking=no "memcached -u root -l ${addr} -p  ${port} -c 10000 -d -P /tmp/memcached.pid"
sleep 1

# init 这里只是初始化 num 设置为0
#  set：memcached 的写操作命令。
#  serverNum：key（键名）。
#  第一个 0：flags，用户自定义的 16-bit 数值，通常不关心就设 0。
#  第二个 0：exptime（过期时间），单位是秒，0 表示不过期。
#  1：后面要发送的数据（value）的字节长度（bytes）。非常重要 —— 必须精确等于下一行实际字节长度。
#  \r\n：协议规定每行以回车换行结束（CRLF）
#  0\r\n：实际要存入的 value 内容
#  查询使用 echo -e "get serverNum\r" | nc ${addr} ${port}
#  清空数据 echo -e "flush_all\r" | nc ${addr} ${port}
echo -e "set serverNum 0 0 1\r\n0\r\nquit\r" | nc ${addr} ${port}
echo -e "set clientNum 0 0 1\r\n0\r\nquit\r" | nc ${addr} ${port}
