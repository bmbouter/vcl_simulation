import numpy as np

arrivals_filename = '/home/bmbouter/Documents/Research/vcl_simulation/data/2008_year_arrivals.txt'

arrivals_f = open(arrivals_filename, 'r')
service_times = []
for line in arrivals_f:
    parsed_line = [int(item) for item in line.rstrip().split(', ')]
    service_times.append(parsed_line[1])

service_times = service_times[:(len(service_times) / 2)]

percentile_index = sorted(service_times).index(300)
probability_of_departure_within_five_minutes = percentile_index / float(len(service_times))
print probability_of_departure_within_five_minutes
