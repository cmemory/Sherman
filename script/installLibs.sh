#!/bin/bash

if [ ! -d "tmp" ]; then
	mkdir tmp
fi

cd tmp

sudo apt update
sudo apt-get -y --force-yes install cmake

# memcached
sudo apt install -y --force-yes memcached libmemcached-dev

# cityhash
git clone https://github.com/google/cityhash.git
cd cityhash
./configure
make all check CXXFLAGS="-g -O3"
sudo make install
cd ..

# boost
sudo apt-get -y --force-yes install libboost-all-dev

