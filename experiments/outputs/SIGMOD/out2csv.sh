#!/bin/bash

dos2unix $1
grep "TOTAL INSERTION" $1 | sed 's/TOTAL[A-Z ]*://' | sed 's/msec//' | sed 's/$/,/' > $1-ins.csv
grep "TOTAL DELETION" $1 | sed 's/TOTAL[A-Z ]*://' | sed 's/msec//' | sed 's/$/,/' > $1-del.csv
