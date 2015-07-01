import csv


def main():
    existing_year_arrival_filename = '2008_year_arrivals.txt'
    first_half_output_filename = '2008_first_half_year_arrivals.txt'
    second_half_output_filename = '2008_second_half_year_arrivals.txt'
    first_half_output = open(first_half_output_filename, 'w')
    second_half_output = open(second_half_output_filename, 'w')
    dataReader = csv.reader(open(existing_year_arrival_filename, 'rb'), delimiter=',')
    cumulative_interarrival_time = 0
    first_arrival = True
    first_half_year = []
    second_half_year = []
    for row in dataReader:
        interarrival_time = float(row[0])
        service_time = float(row[1])
        cumulative_interarrival_time = cumulative_interarrival_time + interarrival_time
        if cumulative_interarrival_time >= 15724800:
            if first_arrival:
                interarrival_time = cumulative_interarrival_time - 15724800
                first_arrival = False
            second_half_output.write('%s, %s\n' % (int(interarrival_time), int(service_time)))
            second_half_year.append((interarrival_time, service_time))
        else:
            first_half_output.write('%s, %s\n' % (int(interarrival_time), int(service_time)))
            first_half_year.append((interarrival_time, service_time))
    first_half_year_stimes = [data[1] for data in first_half_year]
    first_half_year_avg_stime = sum(first_half_year_stimes) / float(len(first_half_year_stimes))
    print 'first_half_year service_time = %s' % first_half_year_avg_stime


if __name__ == "__main__":
    main()