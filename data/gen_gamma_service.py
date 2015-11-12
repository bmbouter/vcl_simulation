import os
from random import gammavariate


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
    arrival_data = open('./2008_year_arrivals.txt', 'r')
    data = []
    for line in arrival_data:
        parsed_line  = [int(item) for item in line.rstrip().split(', ')]
        data.append(parsed_line)
    return data


def write_arrivals(arrivals, filename):
    """
    Writes the interarrival and service times to a file, overwriting any
    existing contents.

    :param arrivals: List of arrivals
    :type arrivals:  list
    :param filename: filename to write to
    :type filename:  basestring
    """
    with open(filename, 'w') as f:
        for arrival in arrivals:
            f.write('%s, %s\n' % tuple(arrival))


def main():
    original_data = read_data()
    modified_data = []
    for data in original_data:
        data[1] = gammavariate(24, 5)
        modified_data.append(data)
    write_arrivals(modified_data, './2008_ch_5_gamma_120_year_arrivals.txt')


if __name__ == "__main__":
    """
    Code in this file rely on relative paths, so this ensures the user is in
    the correct current working directory when it is run.
    """
    if not os.path.isfile('gen_gamma_service.py'):
        print 'Please run in the same directory as gen_gamma_service.py'
        exit()
    main()