//
// Created by ruihong on 9/24/22.
//

#ifndef TIMBERSAW_RESOURCE_PRINTER_PLAN_H
#define TIMBERSAW_RESOURCE_PRINTER_PLAN_H

#include <string>

#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "sys/times.h"
//#include "sys/vtimes.h"
#include "cassert"

#define NUMA_CORE_NUM 24

class Resource_Printer_PlanA {
  unsigned long long lastTotalUser[NUMA_CORE_NUM], lastTotalUserLow[NUMA_CORE_NUM], lastTotalSys[NUMA_CORE_NUM], lastTotalIdle[NUMA_CORE_NUM];

 public:
  Resource_Printer_PlanA();

  long double getCurrentValue();
};

class Resource_Printer_PlanB {
  clock_t lastCPU, lastSysCPU, lastUserCPU;
 public:
  Resource_Printer_PlanB();

  long double getCurrentValue();
};

#endif  // TIMBERSAW_RESOURCE_PRINTER_PLAN_H
