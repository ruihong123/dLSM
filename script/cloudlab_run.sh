#!/bin/bash
home_dir="/users/Ruihong/TimberSaw"
nmemory="8"
ncompute="8"
nmachines="16"
nshard="8"
numa_node=("0" "1")
port=$((10000+RANDOM%1000))
github_repo="https://github.com/ruihong123/TimberSaw"
gitbranch="Multinodes_follow_nova"
function run_bench() {
  communication_port="19843"
#	memory_port=()
	memory_server=()
  memory_shard=()

#	compute_port=()
	compute_server=()
	compute_shard=()
#	machines=()
	i=0
  n=0
  while [ $n -lt $nmemory ]
  do
    memory_server+=("node-$i")
    i=$((i+1))
    n=$((n+1))
  done
  n=0
  i=$((nmachines-1))
  while [ $n -lt $ncompute ]
  do

    compute_server+=("node-$i")
    i=$((i-1))
    n=$((n+1))
  done
  echo "here are the sets up"
  echo $?
  echo compute servers are ${compute_server[@]}
  echo memoryserver is ${memory_server[@]}
#  echo ${machines[@]}
  n=0
#  while [ $n -lt $nshard ]
#  do
#    communication_port+=("$((port+n))")
#    n=$((n+1))
#  done
  n=0
  while [ $n -lt $nshard ]
  do
    # if [[ $i == "2" ]]; then
    # 	i=$((i-1))
    # 	continue
    # fi
    compute_shard+=(${compute_server[$n%$ncompute]})
    memory_shard+=(${memory_server[$n%$nmemory]})
    n=$((n+1))
  done
  echo compute shards are ${compute_shard[@]}
  echo memory shards are ${memory_shard[@]}
#  echo communication ports are ${communication_port[@]}
#  test for dowload and compile the codes
  n=0
  while [ $n -lt $nshard ]
  do
    echo "Set up the ${compute_shard[n]}"
#    ssh -o StrictHostKeyChecking=no ${compute_shard[n]}  "screen -d -m pwd && cd /users/Ruihong && git clone --recurse-submodules $github_repo && cd TimberSaw/ && mkdir build &&  cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && sudo apt-get install -y libnuma-dev  && make db_bench -j 32 && make Server -j 32" &
    ssh -o StrictHostKeyChecking=no ${compute_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build && git checkout $gitbranch && git pull && make db_bench -j 32 && make Server -j 32 && sudo apt install numactl -y" &
    echo "Set up the ${memory_shard[n]}"
    ssh -o StrictHostKeyChecking=no ${memory_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build && git checkout $gitbranch && git pull && make db_bench -j 32 && make Server -j 32 && sudo apt install numactl -y" &
#    ssh -o StrictHostKeyChecking=no ${memory_shard[n]}  "screen -d -m pwd && cd /users/Ruihong && git clone --recurse-submodules $github_repo && cd TimberSaw/ && mkdir build &&  cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && sudo apt-get install -y libnuma-dev  && make db_bench -j 32 && make Server -j 32" &
    n=$((n+1))
    sleep 1
  done

#Set up memory nodes
  n=0
  while [ $n -lt $nshard ]
  do
    echo "Set up the ${memory_shard[n]}"
    ssh -o StrictHostKeyChecking=no ${memory_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build &&
    numactl --cpunodebind=all --localalloc ./Server ${communication_port} 56 $n" &
    #${numa_node[n%${#numa_node[@]}]}
    n=$((n+1))

  done
  sleep 10
##Set up compute nodes and run the benchmark at the same time
  n=0
  while [ $n -lt $nshard ]
  do
    echo "Set up the ${compute_shard[n]}"
    ssh -o StrictHostKeyChecking=no ${compute_shard[n]} "screen -d -m pwd && cd /users/Ruihong/TimberSaw/build &&
    numactl --cpunodebind=all --localalloc ./db_bench --benchmarks=fillrandomshard,readrandomshard,readrandomshard --threads=16 --value_size=400 --block_size=8192 --num=3125000 --bloom_bits=10 --cache_size=67108864 --readwritepercent=50 --compute_node_id=$n > result_8M8C.txt" &
    #
    n=$((n+1))
#    sleep 1
  done
	}
	run_bench