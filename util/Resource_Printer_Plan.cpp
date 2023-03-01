//
// Created by ruihong on 9/24/22.
//
#include "Resource_Printer_Plan.h"

#include <fstream>
#include <iostream>
#include <limits>
#include <numa.h>
#include <vector>
#include <unistd.h>
#include <sys/syscall.h>
// int cpu_id_arr[NUMA_CORE_NUM] = {84,85,86,87,88,89,90,91,92,93,94,95,180,181,182,183,184,185,186,187,188,189,190,191};
int cpu_id_arr[NUMA_CORE_NUM] = {0,1,2,3,4,5,6,7};
int cpu_id_arr_compute[COMPUTE_NUMA_CORE_NUM] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151};

//TODO (ruihong): see whether this page can help with available core number detection: https://stackoverflow.com/questions/150355/programmatically-find-the-number-of-cores-on-a-machine

std::fstream& GotoLine(std::fstream& file, unsigned int num){
  file.seekg(std::ios::beg);
  for(int i=0; i < num - 1; ++i){
    file.ignore(std::numeric_limits<std::streamsize>::max(),'\n');
  }
  return file;
}
Resource_Printer_PlanA::Resource_Printer_PlanA() {

  bool is_compute = false; 
  std::string line;
  std::string space_delimiter = " ";
  size_t pos = 0;
  std::vector<std::string> words{};
  std::ifstream input("/proc/stat");
  int line_num = 0;
  int cpu_list_index = 0;
  int target_line = cpu_id_arr[cpu_list_index] + 1;
  int coreNumber = NUMA_CORE_NUM;

  // decide whether is compute node 
  if (getCurrentHost() == "dbserver3"){
    is_compute = true;
    coreNumber = COMPUTE_NUMA_CORE_NUM;
    target_line = cpu_id_arr_compute[cpu_list_index] + 1;
  }

  while( std::getline( input, line ) ) {

    assert(line_num <= target_line);
    if (line_num == target_line){
      while ((pos = line.find(space_delimiter)) != std::string::npos) {
        words.push_back(line.substr(0, pos));
        line.erase(0, pos + space_delimiter.length());
      }
//      assert(words.size() == 11);
      lastTotalUser[cpu_list_index] = std::stoi(words[1]);
      lastTotalUserLow[cpu_list_index] = std::stoi(words[2]);
      lastTotalSys[cpu_list_index] = std::stoi(words[3]);
      lastTotalIdle[cpu_list_index] = std::stoi(words[4]);
      
      cpu_list_index++;
      if (is_compute){
        printf("CPU %d get its intial value %llu\n", cpu_id_arr_compute[cpu_list_index], lastTotalUser[cpu_list_index]);
        if (cpu_list_index == COMPUTE_NUMA_CORE_NUM)
          break;
        target_line=cpu_id_arr_compute[cpu_list_index+1];
      } else{
        printf("CPU %d get its intial value %llu\n", cpu_id_arr[cpu_list_index], lastTotalUser[cpu_list_index]);
        if (cpu_list_index == NUMA_CORE_NUM)
          break;
        target_line=cpu_id_arr[cpu_list_index+1];
      }

      pos = 0;

    }

    line_num++;

  }
  std::cout << "cpu_list_index:" << cpu_list_index << "; coreNumber:" << coreNumber << std::endl;
  // assert(cpu_list_index == coreNumber);

}

std::string Resource_Printer_PlanA::getCurrentHost() {
  std::string hostname;
  std::ifstream infile("/proc/sys/kernel/hostname");
	std::getline(infile, hostname);
  infile.close();
  return hostname;
}


long double Resource_Printer_PlanA::getCurrentValue() { 
  // long double percent[NUMA_CORE_NUM] = {};
  long double aggre_percent = 0;
  FILE* file;
  // unsigned long long totalUser[NUMA_CORE_NUM], totalUserLow[NUMA_CORE_NUM], totalSys[NUMA_CORE_NUM], totalIdle[NUMA_CORE_NUM], total[NUMA_CORE_NUM];
  bool is_compute = false;
  int coreNumber = NUMA_CORE_NUM;

  std::string line;
  std::string space_delimiter = " ";
  size_t pos = 0;
  std::vector<std::string> words{};
  std::ifstream input("/proc/stat");
  int line_num = 0;
  int cpu_list_index = 0;
  int target_line = cpu_id_arr[cpu_list_index] + 1;

  if (getCurrentHost() == "dbserver3"){
    is_compute = true;
    coreNumber = COMPUTE_NUMA_CORE_NUM;
    target_line = cpu_id_arr_compute[cpu_list_index] + 1;
  }

  long double percent[coreNumber] = {};
  unsigned long long totalUser[coreNumber], totalUserLow[coreNumber], \
                      totalSys[coreNumber], totalIdle[coreNumber], total[coreNumber];

  while( std::getline( input, line ) ) {

    assert(line_num <= target_line);
    if (line_num == target_line){
      while ((pos = line.find(space_delimiter)) != std::string::npos) {
        words.push_back(line.substr(0, pos));
        line.erase(0, pos + space_delimiter.length());
      }
//      assert(words.size() == 11);
      totalUser[cpu_list_index] = std::stoi(words[1]);
      totalUserLow[cpu_list_index] = std::stoi(words[2]);
      totalSys[cpu_list_index] = std::stoi(words[3]);
      totalIdle[cpu_list_index] = std::stoi(words[4]);

      if (totalUser[cpu_list_index] < lastTotalUser[cpu_list_index] || totalUserLow[cpu_list_index] < lastTotalUserLow[cpu_list_index] ||
          totalSys[cpu_list_index] < lastTotalSys[cpu_list_index] || totalIdle[cpu_list_index] < lastTotalIdle[cpu_list_index]){
        //Overflow detection. Just skip this value.
        percent[cpu_list_index] = -1.0;
        fclose(file);
        break ;
      }
      else{
        total[cpu_list_index] = (totalUser[cpu_list_index] - lastTotalUser[cpu_list_index]) + (totalUserLow[cpu_list_index] - lastTotalUserLow[cpu_list_index]) +
                   (totalSys[cpu_list_index] - lastTotalSys[cpu_list_index]);
        percent[cpu_list_index] = total[cpu_list_index];
        total[cpu_list_index] += (totalIdle[cpu_list_index] - lastTotalIdle[cpu_list_index]);
        percent[cpu_list_index] /= total[cpu_list_index];
        percent[cpu_list_index] *= 100;
      }

      lastTotalUser[cpu_list_index] = totalUser[cpu_list_index];
      lastTotalUserLow[cpu_list_index] = totalUserLow[cpu_list_index];
      lastTotalSys[cpu_list_index] = totalSys[cpu_list_index];
      lastTotalIdle[cpu_list_index] = totalIdle[cpu_list_index];

      cpu_list_index++;
      
      if (is_compute){
        if (cpu_list_index == COMPUTE_NUMA_CORE_NUM)
          break;
        target_line=cpu_id_arr_compute[cpu_list_index+1];
      } else{
        if (cpu_list_index == NUMA_CORE_NUM)
          break;
        target_line=cpu_id_arr[cpu_list_index+1];
      }
      pos = 0;

    }

    line_num++;

  }
  assert(cpu_list_index == coreNumber);

  for (int i = 0; i < coreNumber; ++i) {
    aggre_percent += percent[i];
    if (percent[i] == -1.0){
      return -1.0;
    }
  }
  aggre_percent = aggre_percent/coreNumber;
  return aggre_percent;
}


Resource_Printer_PlanB::Resource_Printer_PlanB() {
  numa_bind_core_num = numa_num_task_cpus();
  paramInit();

}

void Resource_Printer_PlanB::paramInit(){
  struct tms timeSample;

  lastCPU = times(&timeSample);
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;
}


std::string Resource_Printer_PlanB::getCurrentHost() {
  std::string hostname;
  std::ifstream infile("/proc/sys/kernel/hostname");
	std::getline(infile, hostname);
  infile.close();
  return hostname;
}

double Resource_Printer_PlanB::getCurrentValue() {
  struct tms timeSample;
  long double percent;
//  int all_possible_core_num = numa_num_configured_cpus();
  //TODO(ruihong): make numa_bind_core_num a static variable or a class variable.
  //TODO(chuqing): generalize the compute node identification by read file

  clock_t now = times(&timeSample);
  if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
      timeSample.tms_utime < lastUserCPU){
    //Overflow detection. Just skip this value.
    // // for debug, ignore it 
    // std::cout << getCurrentHost() << " : ";
    // if(now <= lastCPU){
    //   printf("now <= lastCPU\n");
    //   std::fprintf(stdout, "now: %Ld; lastCPU: %Ld \n", now, lastCPU);
    //   // std::cout << "now: " << now << "; lastCPU: " << lastCPU << std::endl;
    // } else if (timeSample.tms_stime < lastSysCPU) {
    //   printf("timeSample.tms_stime < lastSysCPU\n");
    // } else {
    //   printf("timeSample.tms_utime < lastUserCPU\n");
    // }
    percent = -1.0;
  }
  else{
    percent = (timeSample.tms_stime - lastSysCPU) +
              (timeSample.tms_utime - lastUserCPU);
    percent /= (now - lastCPU);
    percent /= (numa_bind_core_num*1.16*1.07);// The 1.16*1.07 is a calibration parameter, I found the CPU utilization can acheive as high as 116.
    percent *= 100;
  }
  lastCPU = now;
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;
  
  if (percent > 0.0){
#ifdef CALCULATE_MAX_UTIL
    if (max_util < percent){
      max_util = static_cast<double>(percent);
      printf("Max utilization is %f\n", max_util);
    }
#endif
    current_percent = percent;
  }

  return static_cast<double>(percent);

}