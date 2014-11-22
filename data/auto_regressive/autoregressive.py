import os


def nextAutoregressivePoint(phi_one, phi_two, x_t_minus_one, x_t_minus_two):
    return phi_one * x_t_minus_one + phi_two * x_t_minus_two


def ARForecastStream(phi_one, phi_two, filename):
    with open(filename, 'r') as f:
        x_t_minus_two = float(f.next())
        yield x_t_minus_two
        x_t_minus_one = float(f.next())
        yield x_t_minus_one
        for line in f:
            x_t_minus_two = x_t_minus_one
            x_t_minus_one = float(line)
            x_t = nextAutoregressivePoint(phi_one, phi_two, x_t_minus_one, x_t_minus_two)
            yield x_t


def AR_year(input_filename, output_filename):
    phi_one = 0.4108
    phi_two = 0.3368
    with open(output_filename, 'w') as f:
        AR_forecast = ARForecastStream(phi_one, phi_two, input_filename)
        for x_t in AR_forecast:
            f.write('%s\n' % x_t)


def AR_mixed():
    summer_one = 0.2604
    summer_two = 0.1966
    classes_one = 0.3993
    classes_two = 0.3226
    exams_one = 0.2530
    exams_two = 0.2044
    output_filename = 'mixed_autoregressive_five_minute_counts.txt'
    calendar_data = [('2008_summer_five_minute_counts_0.csv', summer_one, summer_two),
                     ('2008_classes_five_minute_counts_0.csv', classes_one, classes_two),
                     ('2008_exams_five_minute_counts_0.csv', exams_one, exams_two),
                     ('2008_summer_five_minute_counts_1.csv', summer_one, summer_two),
                     ('2008_classes_five_minute_counts_1.csv', classes_one, classes_two),
                     ('2008_exams_five_minute_counts_1.csv', exams_one, exams_two),
                     ('2008_summer_five_minute_counts_2.csv', summer_one, summer_two)]
    with open(output_filename, 'w') as f:
        for filename, phi_one, phi_two in calendar_data:
            AR_forecast = ARForecastStream(phi_one, phi_two, filename)
            for x_t in AR_forecast:
                f.write('%s\n' % x_t)


def main():
    AR_year('../2008_five_minute_counts.csv', 'yearlong_autoregressive_five_minute_counts.txt')
    AR_mixed()


if __name__ == "__main__":
    if not os.path.isfile('autoregressive.py'):
        print 'Please run in the same directory as autoregressive.py'
        exit()
    main()
