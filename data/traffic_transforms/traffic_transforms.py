import os
import random

import numpy as np

def read_data():
    arrival_data = open('../2008_year_arrivals.txt', 'r')
    data = []
    for line in arrival_data:
        parsed_line  = [int(item) for item in line.rstrip().split(', ')]
        data.append(parsed_line)
    return data

def write_data(data, filename):
    f = open(filename, 'w')
    for line in data:
        line = [str(item) for item in line]
        line_to_write = ', '.join(line) + '\n'
        f.write(line_to_write)

def get_filename(arrival_scale, service_scale):
    return 'arrival=%s_service=%s_arrivals.txt' % (arrival_scale, service_scale)

def traffic_condition_iterator(arrival_start=0.5, service_start=0.5, arrival_steps=11,
        service_steps=11, step_size=0.1):
    arrival_scale = arrival_start
    for i in range(arrival_steps):
        service_scale = service_start
        for j in range(service_steps):
            filename = get_filename(arrival_scale, service_scale)
            yield (arrival_scale, service_scale, filename)
            service_scale = service_scale + step_size
        arrival_scale = arrival_scale + step_size

def get_service_time_mu_and_sigma(data):
    service_data = [item[1] for item in data]
    mu = np.average(service_data)
    sigma = np.var(service_data)
    return (mu, sigma)

def transform_service_data(data, service_scale):
    for i, observation in enumerate(data):
        data[i][1] = data[i][1] * service_scale
    return data

def thin_arrival_data(data, arrival_scale):
    thinned_data = []
    for observation in data:
        if random.random() <= arrival_scale:
            thinned_data.append(observation)
    return thinned_data

def add_arrival_data(data, arrival_scale):
    mu, sigma = get_service_time_mu_and_sigma(data)
    prob_of_addition = arrival_scale - 1.0
    transformed_data = []
    for i, observation in enumerate(data):
        transformed_data.append(observation)
        if random.random() <= prob_of_addition:
            # add in new arrival in between this one and the next
            current_arrival_time = data[i][0]
            try:
                next_arrival_time = data[i+1][0]
            except IndexError:
                next_arrival_time = data[i-1][0]
            numrange = sorted([current_arrival_time, next_arrival_time])
            new_arrival_time = random.randint(*numrange)
            new_service_time = random.normalvariate(mu, sigma)
            new_observation = [new_arrival_time, new_service_time]
            transformed_data.append(new_observation)
    return transformed_data

def transform_arrival_data(data, arrival_scale):
    if arrival_scale > 1:
        return add_arrival_data(data, arrival_scale)
    else:
        return thin_arrival_data(data, arrival_scale)

def transform_traffic(data, arrival_scale, service_scale, filename):
    data = transform_service_data(data, service_scale)
    data = transform_arrival_data(data, arrival_scale)
    write_data(data, filename)

def generate_all_traffic_environments(data):
    for arrival_scale, service_scale, filename in traffic_condition_iterator():
        output_string = 'Generating (arrival=%s, service=%s)'
        print output_string % (arrival_scale, service_scale)
        transform_traffic(data, arrival_scale, service_scale, filename)

def clean_data(data):
    clean_data = []
    for observation in data:
        if observation[1] < 0:
            continue
        clean_data.append(observation)
    return clean_data

def main():
    data = read_data()
    data = clean_data(data)
    add_arrival_data(data, 1.2)
    generate_all_traffic_environments(data)

if __name__ == "__main__":
    if not os.path.isfile('traffic_transforms.py'):
        print 'Please run in the same directory as traffic_transforms.py'
        exit()
    main()
