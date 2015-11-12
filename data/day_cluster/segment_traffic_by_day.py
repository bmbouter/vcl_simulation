import copy
import datetime

import numpy

from data.traffic_transforms.traffic_transforms import read_data, clean_data


ABSOLUTE_START_DATETIME = datetime.datetime(2008, 7, 1, 0, 0)


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


def write_arrivals_per_five_minutes(arrivals, filename):
    """
    Writes the arrivals per five minute period to a file, overwriting any
    existing contents.

    :param arrivals: List of arrivals
    :type arrivals:  list
    :param filename: filename to write to
    :type filename:  basestring
    """
    five_minute_counts = get_arrivals_per_time_period(arrivals, 300) # 300 seconds per 5 minutes
    with open(filename, 'w') as f:
        for count in five_minute_counts:
            f.write('%s\n' % count)


def get_arrivals_per_time_period(arrivals, period_seconds):
    """
    Compute a list of the arrivals per time_period.

    :param arrivals:       List of arrivals
    :type arrivals:        list
    :param period_seconds: The length of a period in seconds
    :type period_seconds:  int

    :return:               List of arrival counts per period
    :rtype:                list
    """
    num_periods = (31536000 / period_seconds)  # fixed at 1 year
    period_counts = [0] * num_periods
    total_seconds = 0
    for arrival in arrivals:
        total_seconds = total_seconds + arrival[0]
        day_index = total_seconds / period_seconds
        period_counts[day_index] = period_counts[day_index] + 1
    return period_counts


def get_mean_std(data):
    """
    Return a dict containing the mean and std of the numbers in the list data.

    :param data: A list of numbers
    :type data:  list

    :return:     A dict with the key 'mean' and 'std' each containing that
                 type of summary statistic of 'data'.
    :rtype:      dict
    """
    mean = "{0:.2f}".format(numpy.mean(data))
    std = "{0:.2f}".format(numpy.std(data))
    return {'mean': mean, 'std': std}


def get_statistics_per_day(arrivals):
    """
    Compute the average and standard deviation of arrivals per day. Uses
    get_arrivals_per_time_period() with period_seconds set to 86400 (number
    of seconds / day).

    :param arrivals: List of arrivals
    :type arrivals:  list

    :return:         A dict containing the 'mean' and 'std' as keys.
    :rtype:          dict
    """
    day_arrivals = get_arrivals_per_time_period(arrivals, 86400)
    return get_mean_std(day_arrivals)


def get_interarrival_statistics(arrivals):
    """
    Compute the average and standard deviation of interarrival times.

    :param arrivals: List of arrivals
    :type arrivals:  list

    :return:         A dict containing the 'mean' and 'std' as keys
    :rtype:          dict
    """
    return get_mean_std(map(lambda a: a[0], arrivals))


def get_statistics_per_five_min(arrivals):
    """
    Compute the average and standard deviation of arrivals per five minute
    interval. Uses get_arrivals_per_time_period() with period_seconds set to
    300 which is equivalent to five minutes.

    :param arrivals: List of arrivals
    :type arrivals:  list

    :return:         A dict containing the 'mean' and 'std' as keys
    :rtype:          dict
    """
    day_arrivals = get_arrivals_per_time_period(arrivals, 300)
    return get_mean_std(day_arrivals)


def get_clean_arrivals():
    """
    Gets the arrival data. Each arrival is a list of length 2 with the
    interarrival time in seconds as the first item, and the service time in
    seconds as the second item.

    :return: The arrival data as a list of arrivals.
    :rtype:  list of lists
    """
    original_data = read_data()
    return clean_data(original_data)


def get_seconds_since_start_of_data(date_to_convert):
    """
    Compute the number of seconds since the ABSOLUTE_START_DATETIME.

    :param date_to_convert: The date to measure in seconds since the
                            ABSOLUTE_START_DATETIME.
    :type date_to_convert:  datetime.datetime

    :return:                The number of seconds between date_to_convert and
                            ABSOLUTE_START_DATETIME.
    :rtype:                 int
    """
    if date_to_convert < ABSOLUTE_START_DATETIME:
        raise Exception('date_to_convert cannot occur after ABSOLUTE_START_DATETIME')
    diff = date_to_convert - ABSOLUTE_START_DATETIME
    return int(diff.total_seconds())


def get_traffic_for_multiple_dates(arrivals, dates_list):
    """
    Gets the traffic for a list of date tuples with (start, end). Uses
    get_arrivals_by_date_range() to get the arrivals for each start and end
    range.

    With each concatenation, the remaining time from a previous one is added
    to the interarrival time from the next one. This allows all time to be
    accounted for using midnight as a crossover point.

    :param dates_list: A list of tuple dates that will be concatenated
                       together.
    :type dates_list:  list of tuples
    :return:           A list of arrivals filtered by the date ranges and
                       concatenated together.
    """
    to_return = []
    remaining_time = None
    for start, end in dates_list:
        data = copy.copy(arrivals)
        start_seconds = get_seconds_since_start_of_data(start)
        end_seconds = get_seconds_since_start_of_data(end)
        filtered_arrivals = get_arrivals_by_date_range(data, start_seconds, end_seconds)
        if remaining_time:
            filtered_arrivals['arrivals'][0][0] = filtered_arrivals['arrivals'][0][0] + remaining_time
        to_return.extend(filtered_arrivals['arrivals'])
        remaining_time = filtered_arrivals['remaining_time']
    return to_return


def get_arrivals_by_date_range(clean_arrivals, start_offset, end_offset):
    """
    Returns clean_arrivals that arrive after start and before end.

    The clean_arrivals are a list of arrivals. Each arrival is a list of
    length two with index 0 being the interarrival before the arrival occurs,
    and index[1] is the service time of the arrival.

    In the result, the first arrival has its interarrival time set to the
    number of seconds between the start time and the arrival occurring.

    The amount of time in seconds between the last arrival time and end is
    also returned as part of the data structure.

    :param clean_arrivals: A list of arrivals. Each arrival is a list with
                           interarrival at index 0, and service time at
                           index 1.
    :type clean_arrivalS:  list
    :param start_offset:   The start offset of the filter in seconds
    :type start_offset:    int
    :param end_offset:     The end offset of the filter in seconds
    :type end_offset:      int

    :return:               A subset of clean_arrivals that occur within the
                           start and end range.
    :rtype:                list
    """
    next_arrival_time = 0
    remaining_time = 0
    arrivals_in_range = []
    found_start = False
    for arrival in clean_arrivals:
        next_arrival_time = next_arrival_time + arrival[0]
        if next_arrival_time < start_offset:
            continue
        if not found_start and next_arrival_time >= start_offset:
            adjusted_interarrival = next_arrival_time - start_offset
            arrivals_in_range.append([adjusted_interarrival, arrival[1]])
            found_start = True
            continue
        if found_start and next_arrival_time >= end_offset:
            remaining_time = end_offset - (next_arrival_time - arrival[0])
            break
        arrivals_in_range.append(arrival)
    return {'remaining_time': remaining_time, 'arrivals': arrivals_in_range}


def get_dates_by_index(index):
    """
    Returns a tuple in the form (start, end). start and end are
    datetime.datetime() objects.

    :param index: the day index to include
    :return: A tuple of datetime objects (start, end)
    """
    start = ABSOLUTE_START_DATETIME + datetime.timedelta(days=index)
    end = start + datetime.timedelta(days=1)
    return (start, end)


def write_busiest_traffic_days(arrivals):
    busiest_indexes = [66, 71, 70, 142, 105, 69, 78, 77, 128, 79]
    busiest_dates = map(get_dates_by_index, busiest_indexes)
    busiest_arrivals = get_traffic_for_multiple_dates(arrivals, busiest_dates)
    write_arrivals(busiest_arrivals, '2008_busiest_arrivals.txt')
    write_arrivals_per_five_minutes(busiest_arrivals, '2008_busiest_five_minute_counts.csv')
    print 'Average arrivals per day of Busiest = %s' % get_statistics_per_day(busiest_arrivals)


def write_slowest_traffic_days(arrivals):
    slowest_indexes = [178, 180, 184, 46, 176, 177, 186, 182, 185, 3]
    slowest_dates = map(get_dates_by_index, slowest_indexes)
    slowest_arrivals = get_traffic_for_multiple_dates(arrivals, slowest_dates)
    write_arrivals(slowest_arrivals, '2008_slowest_arrivals.txt')
    write_arrivals_per_five_minutes(slowest_arrivals, '2008_slowest_five_minute_counts.csv')
    print 'Average arrivals per day of Slowest = %s' % get_statistics_per_day(slowest_arrivals)


def write_mon_fri_semester_traffic(arrivals):
    d = datetime.datetime
    mon_fri_semester_dates = [
        (d(2008, 8, 20), d(2008, 8, 23)),
        (d(2008, 8, 25), d(2008, 8, 30)),
        (d(2008, 9, 2), d(2008, 9, 6)),     # Labor day on 9/1/2008
        (d(2008, 9, 8), d(2008, 9, 13)),
        (d(2008, 9, 15), d(2008, 9, 20)),
        (d(2008, 9, 22), d(2008, 9, 27)),
        (d(2008, 9, 29), d(2008, 10, 4)),
        (d(2008, 10, 6), d(2008, 10, 9)),   # Fall break 10/9, 10/10
        (d(2008, 10, 13), d(2008, 10, 18)),
        (d(2008, 10, 20), d(2008, 10, 25)),
        (d(2008, 10, 27), d(2008, 11, 1)),
        (d(2008, 11, 3), d(2008, 11, 8)),
        (d(2008, 11, 10), d(2008, 11, 15)),
        (d(2008, 11, 17), d(2008, 11, 22)),
        (d(2008, 11, 24), d(2008, 11, 26)), # Thanksgiving 11/26 - 11/28
        (d(2008, 12, 1), d(2008, 12, 6)),   # Last day of Fall classes 12/5
        (d(2009, 1, 7), d(2009, 1, 10)),
        (d(2009, 1, 12), d(2009, 1, 17)),
        (d(2009, 1, 20), d(2009, 1, 24)),   # MLK Holiday 1/19
        (d(2009, 1, 26), d(2009, 1, 31)),
        (d(2009, 2, 2), d(2009, 2, 7)),
        (d(2009, 2, 9), d(2009, 2, 14)),
        (d(2009, 2, 16), d(2009, 2, 21)),
        (d(2009, 2, 23), d(2009, 2, 28)),
        # (d(2009, 3, 2), d(2009, 3, 7)),   # Spring break 3/2 - 3/6
        (d(2009, 3, 9), d(2009, 3, 14)),
        (d(2009, 3, 16), d(2009, 3, 21)),
        (d(2009, 3, 23), d(2009, 3, 28)),
        (d(2009, 3, 30), d(2009, 4, 4)),
        (d(2009, 4, 6), d(2009, 4, 10)),    # Spring holiday 4/10
        (d(2009, 4, 13), d(2009, 4, 18)),
        (d(2009, 4, 20), d(2009, 4, 25)),   # Last day of Spring classes 4/24
    ]
    mon_fri_semester_arrivals = get_traffic_for_multiple_dates(arrivals, mon_fri_semester_dates)
    write_arrivals(mon_fri_semester_arrivals, '2008_mon_fri_semester_arrivals.txt')
    write_arrivals_per_five_minutes(mon_fri_semester_arrivals, '2008_mon_fri_semester_five_minute_counts.csv')
    print 'Average arrivals per day of Mon-Fri Semester = %s' % get_statistics_per_day(mon_fri_semester_arrivals)


def write_winter_spring_break_holiday_traffic(arrivals):
    d = datetime.datetime
    winter_spring_break_dates = [
        (d(2008, 12, 24), d(2009, 1, 3)),   # Winter Holiday 12/24 - 1/2
        (d(2009, 3, 2), d(2009, 3, 7)),     # Spring Break 3/2 - 3/6
    ]
    winter_spring_break_arrivals = get_traffic_for_multiple_dates(arrivals, winter_spring_break_dates)
    write_arrivals(winter_spring_break_arrivals, '2008_winter_spring_break_arrivals.txt')
    write_arrivals_per_five_minutes(winter_spring_break_arrivals, '2008_winter_spring_break_five_minute_counts.csv')
    print 'Average arrivals per day of Winter+Spring Break = %s' % get_statistics_per_day(winter_spring_break_arrivals)


def main():
    clean_arrivals = get_clean_arrivals()

    write_slowest_traffic_days(clean_arrivals)
    write_busiest_traffic_days(clean_arrivals)
    write_mon_fri_semester_traffic(clean_arrivals)
    write_winter_spring_break_holiday_traffic(clean_arrivals)
    print 'Average arrivals per day of unsegmented data = %s' % get_statistics_per_day(copy.copy(clean_arrivals))



if __name__ == '__main__':
    main()
