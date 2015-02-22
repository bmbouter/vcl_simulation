#!/bin/bash

for i in {1..100}
do
   tail -n 52560 "arrivals_alpha_0.$i.txt" > "second_half_year/arrivals_alpha_0.$i.txt"
done

rm second_half_year/arrivals_alpha_0.90.txt
rm second_half_year/arrivals_alpha_0.80.txt
rm second_half_year/arrivals_alpha_0.70.txt
rm second_half_year/arrivals_alpha_0.60.txt
rm second_half_year/arrivals_alpha_0.50.txt
rm second_half_year/arrivals_alpha_0.40.txt
rm second_half_year/arrivals_alpha_0.30.txt
rm second_half_year/arrivals_alpha_0.20.txt
rm second_half_year/arrivals_alpha_0.10.txt
rm second_half_year/arrivals_alpha_0.100.txt

for i in {1..9}
do
   tail -n 52560 "arrivals_alpha_0.0$i.txt" > "second_half_year/arrivals_alpha_0.0$i.txt"
done

tail -n 52560 "arrivals_alpha_1.txt" > "second_half_year/arrivals_alpha_1.txt"

