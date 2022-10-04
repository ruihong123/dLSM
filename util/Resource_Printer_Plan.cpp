//
// Created by ruihong on 9/24/22.
//

#include "Resource_Printer_Plan.h"

#include <fstream>
#include <iostream>
#include <limits>
#include <vector>
int cpu_id_arr[NUMA_CORE_NUM] = {84,85,86,87,88,89,90,91,92,93,94,95,180,181,182,183,184,185,186,187,188,189,190,191};


std::fstream& GotoLine(std::fstream& file, unsigned int num){
  file.seekg(std::ios::beg);
  for(int i=0; i < num - 1; ++i){
    file.ignore(std::numeric_limits<std::streamsize>::max(),'\n');
  }
  return file;
}
Resource_Printer_PlanA::Resource_Printer_PlanA() {


//    std::string prefix = "cpu";
//    std::string str;
//    std::string suffix = " %llu %llu %llu %llu";
//    for (int i = 0; i < NUMA_CORE_NUM; ++i) {
//      FILE* file = fopen("/proc/stat", "r");
//      std::string s = std::to_string(cpu_id_arr[i]);
//      str = prefix + s +suffix;
//      int ret = fscanf(file, str.c_str(), &lastTotalUser[i], &lastTotalUserLow[i],
//                       &lastTotalSys[i], &lastTotalIdle[i]);
//      assert(ret != 0);
//      fclose(file);
//    }
std::string line;
std::string space_delimiter = " ";
size_t pos = 0;
std::vector<std::string> words{};
  std::ifstream input("/proc/stat");
  int line_num = 0;
  int cpu_list_index = 0;
  int target_line = cpu_id_arr[cpu_list_index] + 1;

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
      printf("CPU %d get its intial value %llu\n", cpu_id_arr[cpu_list_index], lastTotalUser[cpu_list_index]);

      cpu_list_index++;
      if (cpu_list_index == NUMA_CORE_NUM)
        break ;
      target_line=cpu_id_arr[cpu_list_index+1];
      pos = 0;

    }


    line_num++;

  }
  assert(cpu_list_index == NUMA_CORE_NUM);


}
long double Resource_Printer_PlanA::getCurrentValue() { long double percent[NUMA_CORE_NUM] = {};
  long double aggre_percent = 0;
  FILE* file;
  unsigned long long totalUser[NUMA_CORE_NUM], totalUserLow[NUMA_CORE_NUM], totalSys[NUMA_CORE_NUM], totalIdle[NUMA_CORE_NUM], total[NUMA_CORE_NUM];

  std::string line;
  std::string space_delimiter = " ";
  size_t pos = 0;
  std::vector<std::string> words{};
  std::ifstream input("/proc/stat");
  int line_num = 0;
  int cpu_list_index = 0;
  int target_line = cpu_id_arr[cpu_list_index] + 1;

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
      if (cpu_list_index == NUMA_CORE_NUM)
        break ;
      target_line=cpu_id_arr[cpu_list_index+1];
      pos = 0;

    }


    line_num++;

  }
  assert(cpu_list_index == NUMA_CORE_NUM);



//
//  for (int i = 0; i < NUMA_CORE_NUM; ++i) {
//    file = fopen("/proc/stat", "r");
//    int ret = fscanf(file, "cpu %llu %llu %llu %llu", &totalUser[i], &totalUserLow[i],
//           &totalSys[i], &totalIdle[i]);
//    assert(ret != 0);
//    if (totalUser[i] < lastTotalUser[i] || totalUserLow[i] < lastTotalUserLow[i] ||
//        totalSys[i] < lastTotalSys[i] || totalIdle[i] < lastTotalIdle[i]){
//      //Overflow detection. Just skip this value.
//      percent[i] = -1.0;
//      fclose(file);
//      break ;
//    }
//    else{
//      total[i] = (totalUser[i] - lastTotalUser[i]) + (totalUserLow[i] - lastTotalUserLow[i]) +
//                 (totalSys[i] - lastTotalSys[i]);
//      percent[i] = total[i];
//      total[i] += (totalIdle[i] - lastTotalIdle[i]);
//      percent[i] /= total[i];
//      percent[i] *= 100;
//    }
//
//    lastTotalUser[i] = totalUser[i];
//    lastTotalUserLow[i] = totalUserLow[i];
//    lastTotalSys[i] = totalSys[i];
//    lastTotalIdle[i] = totalIdle[i];
//    fclose(file);
//  }
  for (int i = 0; i < NUMA_CORE_NUM; ++i) {
    aggre_percent += percent[i];
    if (percent[i] == -1.0){
      return -1.0;
    }
  }
  aggre_percent = aggre_percent/NUMA_CORE_NUM;
  return aggre_percent;
}


Resource_Printer_PlanB::Resource_Printer_PlanB() {

getCurrentValue();



}
long double Resource_Printer_PlanB::getCurrentValue() {
  struct tms timeSample;
  clock_t now;
  long double percent;

  now = times(&timeSample);
  if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
      timeSample.tms_utime < lastUserCPU){
    //Overflow detection. Just skip this value.
    percent = -1.0;
  }
  else{
    percent = (timeSample.tms_stime - lastSysCPU) +
              (timeSample.tms_utime - lastUserCPU);
    percent /= (now - lastCPU);
    percent /= NUMA_CORE_NUM;
    percent *= 100;
  }
  lastCPU = now;
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;

  return percent;

}