#!/bin/bash
OUTPUTFILE="serverout.txt"

numactl --physcpubind 0-7 --localalloc ./Server > ${OUTPUTFILE} 
