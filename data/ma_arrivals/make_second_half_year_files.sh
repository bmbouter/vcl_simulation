#!/bin/bash

for i in {1..50}
do
   tail -n 52560 "arrivals_k_$i.txt" > "second_half_year/arrivals_k_$i.txt"
done
