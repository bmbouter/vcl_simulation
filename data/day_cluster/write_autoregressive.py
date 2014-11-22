import os

from data.auto_regressive.autoregressive import AR_year


def compute_autoregressive_for_day_cluster():
    AR_year('2008_busiest_five_minute_counts.csv', 'busiest/autoregressive_five_min_counts.txt')
    AR_year('2008_slowest_five_minute_counts.csv', 'slowest/autoregressive_five_min_counts.txt')
    AR_year('2008_mon_fri_semester_five_minute_counts.csv', 'mon_fri_semester/autoregressive_five_min_counts.txt')
    AR_year('2008_winter_spring_break_five_minute_counts.csv', 'winter_spring_break/autoregressive_five_min_counts.txt')


if __name__ == '__main__':
    if not os.path.isfile('write_autoregressive.py'):
        print 'Please run in the same directory as write_autoregressive.py'
        exit()
    compute_autoregressive_for_day_cluster()