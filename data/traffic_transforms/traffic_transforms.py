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
    arrival_data = open('/home/bmbouter/Documents/Research/vcl_simulation/data/2008_year_arrivals.txt', 'r')
    data = []
    for line in arrival_data:
        parsed_line  = [int(item) for item in line.rstrip().split(', ')]
        data.append(parsed_line)
    return data

def write_data(data, filename):
    """
    Write VCL data to a text file.

    VCL data is written in the form:

    678, 3763

    This method writes interarrival and service time information from data to
    a file name named filename.  The filename is assumed to be in the current
    directory.

    :param data: A list of observations.  Each observation is a list of length
        2.  Position 0 is interarrival time, and Position 1 is service time.
    :type data: list
    :param filename: The filename to write the data to.
    :type filename: str
    """
    f = open(filename, 'w')
    for line in data:
        line = [str(item) for item in line]
        line_to_write = ', '.join(line) + '\n'
        f.write(line_to_write)

def get_filename(arrival_scale, service_scale):
    """
    Get the filename for a given arrivla_scale and service_scale.

    :param arrival_scale: A float of the arrival_scale factor.
    :type arrival_scale: float
    :param service_scale: A float of the service_scale factor.
    :type service_scale: float

    :return: A string containing the formed filename.
    :rtype: str
    """
    return 'arrival=%s_service=%s_arrivals.txt' % (arrival_scale, service_scale)

def traffic_condition_iterator(arrival_start=0.5, service_start=0.5, arrival_steps=11,
        service_steps=11, step_size=0.1):
    """
    A generator that yields an arrival_scale, service_scale, and filename.

    Iterates through the different names, allowing for simple looping through
    all traffic variation cases.

    :param arrival_start: The lowest arrival scale value.
    :type arrival_start: float
    :param service_start: The lowest service scale value.
    :type service_start: float
    :param arrival_steps: The number of loops for the arrival_steps dimension.
    :type arrival_steps: int
    :param service_steps: The number of loops for the service_steps dimension.
    :type service_steps: int
    :param step_size: The step amount to increase each variable by each time.
    :type step_size: float

    :return: A generator, so it provides an object that has a callable named
        next() and raises StopException when it exits.
    :rtype: generator
    """
    arrival_scale = arrival_start
    for i in range(arrival_steps):
        service_scale = service_start
        for j in range(service_steps):
            filename = get_filename(arrival_scale, service_scale)
            yield (arrival_scale, service_scale, filename)
            service_scale = service_scale + step_size
        arrival_scale = arrival_scale + step_size

def transform_service_data(data, service_scale):
    """
    Scale the service time of all observations in data by service_scale.

    Data is a list of observations.  The service time is kept in position 1 of
    each observation.  All observations are iterated, and updated.  Although
    the method updates data in place, it is also explicitly returned.

    :param data: The list of observations.  Each observation is a list of
        length 2, with service time in position 1.
    :type data: list
    :param service_scale: The scale value that each service time will be
        multiplied by.
    :type service_scale: float

    :return: the data originall passed in, with the updated service times.
    :rtype: list
    """
    for i, observation in enumerate(data):
        data[i][1] = int(data[i][1] * service_scale)
    return data

def thin_arrival_data(data, arrival_scale):
    """
    Thin the arrival data proportional to arrival_scale.

    Each observation is added to the new list of observations with a
    probability of arrival_scale.  Using a new list ensures that each item
    from the original list is evaluated exactly once.

    :param data: The list of observations.  The interarrival time is in
        position 0, and the service time in position 1.  The observations are
        kept with a probability of arrival_scale.
    :type data: list
    :param arrival_scale: The proportion of traffic to arrivals to keep.  ie: 0.9 will
        keep 90 percent of the traffic.  It is expected to be <= 1.0
    :type arrival_scale: float

    :return: A new list of observations thinned probabilstically.
    :rtype: list
    """
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
    """
    Select a random service time from data.

    :param data: The observation to select the random service time from.
    :type data: list

    :return: A random service time from data.
    :rtype: int
    """
    return random.choice(data)[1]

def add_arrival_data(data, arrival_scale):
    """
    Add in observations proportionally based on arrival_scale.

    A new list is created to be returned.  Each arrival in data is randomly
    selected with a probability of arrival_scale - 1.0.  The data is iterated
    through, and each observation is added to the new list, along with any
    introduced traffic.

    New arrivals, use a random service time selected from data.  The ensures
    a selection from an empirical distribution, and avoids complications from
    sampling negative values from a service time distribution.

    New arrivals have an interarrival time between 0 and the current arrival.
    The 'original' arrival will be added before the 'new' arrival, and so the
    'original' arrival needs to have its interarrival time reduced by the
    interarrival time being attributed to the 'new' arrival.  Without this
    interarrival adjustment, new time would be introduced in the simulation,
    essentially thinning the traffic.

    A few assertions are contained in this method that will raise Exceptions
    if these incorrect situations occur.

    :param data: The list of observations to have arrivals added to.
    :type data: list
    :param arrival_scale: The proportion of arrivals to introduce.  ie: 1.3
        will introduce 30 percent additional traffic.  It is expected
        arrival_scale > 1.0 for this method.
    :type arrival_scale: float

    :return: A new list of observations added to probabilistically.
    :rtype: list
    """
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
    """
    Thin or add traffic to/from data based on arrival_scale.

    Thinning and Adding traffic are different operations, so this method
    abstracts that complexity for the caller and accepts numbers larger and
    smaller than 1.0.

    :param data: the list of data observations.
    :type data: list
    :param arrival_scale: The proportion amount of traffic to thin or
        introduce.  A number larger or smaller than 1.0.
    :type arrival_scale: float

    :return: A new list of observations either thinned or with introduced
        traffic.
    :rtype: list
    """
    if arrival_scale > 1:
        return add_arrival_data(data, arrival_scale)
    else:
        return thin_arrival_data(data, arrival_scale)

def transform_traffic(data, arrival_scale, service_scale, filename):
    """
    Transform data by arrival_scale and service_scale and write to a file.

    This method first scales service times of data by service_scale.  Then it
    thins or adds traffic to data by using arrival_scale.  The introduction of
    arrival traffic samples from the service time, so the correct order is to
    adjust the service time first, then the arrival traffic.

    :param data: The list of observations.
    :type data: list
    :param arrival_scale: The proportion amount of traffic to thin or
        introduce.  A number larger or smaller than 1.0.
    :type arrival_scale: float
    :param service_scale: The proportion adjustment to the service time of
        observations in data.  A number larger or smaller than 1.0.
    :type service_scale: float
    :param filename: The filename to write the updated observations to.
    :type filename: str
    """
    data = transform_service_data(data, service_scale)
    data = transform_arrival_data(data, arrival_scale)
    write_data(data, filename)

def generate_all_traffic_environments(original_data):
    """
    The main loop of the program.

    This method is responsible for iterating through the problems provided by
    traffic_condition_iterator().  For each problem it creates a deep copy of
    the original_data for that problem.  It outputs the current run to the
    user, and then calls transform_traffic().

    :param original_data: The original data list of observations read in from
        the file.
    :type original_data: list
    """
    for arrival_scale, service_scale, filename in traffic_condition_iterator():
        data = copy.deepcopy(original_data)
        output_string = 'Generating (arrival=%s, service=%s)'
        print output_string % (arrival_scale, service_scale)
        transform_traffic(data, arrival_scale, service_scale, filename)

def clean_data(data):
    """
    Clean the input data, throwing away bad values.

    The only case currently handles is if either interarrival or the service
    time of an observation is negative.  Those are skipped, but all others are
    added to a new list and returned.

    :param data: The list of observations to be cleaned.
    :type data: list

    :return: A new list containing the cleaned observations.
    :rtype: list
    """
    clean_data = []
    for observation in data:
        if observation[0] < 0 or observation[1] < 0:
            print 'skipping Observation(arrival=%s, service=%s)' % (observation[0], observation[1])
            continue
        clean_data.append(observation)
    return clean_data

def main():
    """
    The entry point of the program.

    Reads the data, cleans the data, calls the method that iterates through
    the problems.
    """
    original_data = read_data()
    original_data = clean_data(original_data)
    generate_all_traffic_environments(original_data)

if __name__ == "__main__":
    """
    Code in this file rely on relative paths, so this ensures the user is in
    the correct current working directory when it is run.
    """
    if not os.path.isfile('traffic_transforms.py'):
        print 'Please run in the same directory as traffic_transforms.py'
        exit()
    main()
