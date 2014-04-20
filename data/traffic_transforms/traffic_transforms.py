import copy
import os
import random

def read_data():
    """
    Read VCL arrival data.

    Arrival are kept in a text file in the form:

    143, 2456

    This method reads all arrival data and creates a list of observations.
    Each observation is also a list with the interarrival time in position 0
    and service time in position 1.  Interarrival and service time data are
    integers.

    :return: A list of observations
    :rtype: list

    """
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

def transform_service_data(data, service_scale):
    for i, observation in enumerate(data):
        data[i][1] = int(data[i][1] * service_scale)
    return data

def thin_arrival_data(data, arrival_scale):
    thinned_data = []
    for i, observation in enumerate(data):
        if random.random() <= arrival_scale:
            thinned_data.append(observation)
        else:
            try:
                # If this arrival is culled, add the interarrival time to the
                # next arrival
                data[i+1][0] = data[i+1][0] + data[i][0]
            except IndexError:
                # If this is the last arrival, do nothing
                pass
    return thinned_data

def sample_random_service_time(data):
    return random.choice(data)[1]

def add_arrival_data(data, arrival_scale):
    prob_of_addition = arrival_scale - 1.0
    transformed_data = []
    for i, observation in enumerate(data):
        transformed_data.append(observation)
        if random.random() <= prob_of_addition:
            # add in new arrival in between this one and the next
            try:
                next_interarrival_time = data[i+1][0]
            except IndexError:
                # If this is the last arrival, use the distance to the i - 1
                # interarrival time
                next_interarrival_time = data[i-1][0]
            new_interarrival_time = random.randint(0, next_interarrival_time)
            if new_interarrival_time < 0:
                raise Exception('New interarrival time should never be negative!')
            new_service_time = sample_random_service_time(data)
            new_observation = [new_interarrival_time, new_service_time]
            transformed_data.append(new_observation)
            try:
                # Update the interarrival time of the next arrival
                if data[i+1][0] - new_interarrival_time < 0:
                    Exception('Updating the next interval caused the next interval to become negative')
                data[i+1][0] = data[i+1][0] - new_interarrival_time
            except IndexError:
                # If the new arrival is the last one there is no later arrival
                # to update, do nothing
                pass
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

def generate_all_traffic_environments(original_data):
    for arrival_scale, service_scale, filename in traffic_condition_iterator():
        data = copy.deepcopy(original_data)
        output_string = 'Generating (arrival=%s, service=%s)'
        print output_string % (arrival_scale, service_scale)
        transform_traffic(data, arrival_scale, service_scale, filename)

def clean_data(data):
    clean_data = []
    for observation in data:
        if observation[0] < 0 or observation[1] < 0:
            print 'skipping Observation(arrival=%s, service=%s)' % (observation[0], observation[1])
            continue
        clean_data.append(observation)
    return clean_data

def main():
    original_data = read_data()
    original_data = clean_data(original_data)
    generate_all_traffic_environments(original_data)

if __name__ == "__main__":
    if not os.path.isfile('traffic_transforms.py'):
        print 'Please run in the same directory as traffic_transforms.py'
        exit()
    main()
