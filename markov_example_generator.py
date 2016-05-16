from collections import defaultdict
from datetime import datetime
import math
import os

import matplotlib.pyplot as plt


MAX_CUSTOMERS = 8
MAX_SERVERS = 9
R = 1

PERIOD_LENGTH = 300

LAMBDA = 1.0 / 29849.74874
MU = 1 / 29849.74874

BASE_PATH = '/home/bmbouter/Documents/Research/matlab/'


class Evaluator(object):

    arrival_memo = {}
    departure_memo = {}

    @classmethod
    def arrivals(cls, count):
        """
        Numerically evaluate the probability of 'count' arrivals occurring.

        :param count: The number of arrivals which would occur

        :return: The probability of count arrivals occurring.
        """
        # From equation 2.121 on page 60
        return math.pow((LAMBDA * PERIOD_LENGTH), count) / math.factorial(count) * math.exp(-LAMBDA * PERIOD_LENGTH)

    @classmethod
    def departures(cls, count, num_customers_in_service):
        """
        Numerically evaluate the probability of 'count' departures occurring.

        :param count: The number of departures which would occur
        :param num_customers_in_service: the number of customers who are in service

        :return: The probability of count departures occurring.
        """
        # Also using equation 2.121 but with a scaled Mu for the number of in-service customers
        scaled_mu = num_customers_in_service * MU
        return math.pow((scaled_mu * PERIOD_LENGTH), count) / math.factorial(count) * math.exp(-scaled_mu * PERIOD_LENGTH)


class State(object):

    def __init__(self, n, s, q):
        """
        Creates a Markovian state of the modeled system.

        :param n: The number of customers in the system
        :param s: The number of servers in the system
        :param q: The number of inactive servers in the system
        :return: An initialized State object
        """
        self.n = n
        self.s = s
        self.q = q

    def __str__(self):
        return '(%s; %s; %s)' % (self.n, self.s, self.q)

    @classmethod
    def all(cls):
        for n in range(MAX_CUSTOMERS + 1):
            for s in range(1, MAX_SERVERS + 1):
                for q in range(MAX_SERVERS):
                    yield State(n, s, q)


class TableMaker(object):

    def __init__(self):
        self.f_symbolic = open('data/states_symbolic.csv', 'w')
        self.f_numeric = open('data/states_numeric.csv', 'w')
        self.data = []

    def run(self):
        self.write_header()
        for current in State.all():
            self.write_row(current)
        # self.data = [
        #     [0.25, 0.25, 0, 0],
        #     [0, 0.25, 0.25, 0],
        #     [0, 0, 0.5, 0.5],
        #     [0.125, 0.125, 0.25, 0.5]
        # ]
        self.write_matlab_code()

    def write_matlab_code(self):
        with open(BASE_PATH + 'markov_sparse_%s.dat' % len(self.data[0]), 'w') as f_sparse_dat:
            for row, row_data in enumerate(self.data):
                scale = 1.0 / sum(row_data)
                normalized_row_data = map(lambda data: data * scale, row_data)
                for col, value in enumerate(normalized_row_data):
                    if value == 0:
                        continue
                    f_sparse_dat.write('%s    %s    %s\n' % (row+1, col+1, value))

            # matlab recommends to force the largest row, largest column to define the correct matrix size
            if self.data[-1][-1] == 0:
                f_sparse_dat.write('%s    %s    %s\n' % (len(self.data), len(self.data[-1]), 0))

        with open(BASE_PATH + 'markov_sparse_%s.m' % len(self.data[0]), 'w') as f_matlab:
            f_matlab.write('load \'markov_sparse_%s.dat\'\n' % len(self.data[0]))
            f_matlab.write('P_mat = spconvert(markov_sparse_%s);\n' % len(self.data[0]))
            f_matlab.write('Q = P_mat - eye(%s);\n' % len(self.data[0]))
            f_matlab.write('clearvars P_mat\n')
            f_matlab.write('Q_t = transpose(Q);\n')
            f_matlab.write('clearvars Q\n')
            x_initial = 1.0 / len(self.data[0])
            f_matlab.write('x0(1:%s) = %s;\n' % (len(self.data[0]), x_initial))
            f_matlab.write('x0 = x0\';\n')
            f_matlab.write('b(1:%s) = 0;\n' % len(self.data[0]))
            f_matlab.write('b = b\';\n')
            #f_matlab.write('gs(Q_t, x0, b, 1)\n')

    def write_zero(self):
        self.write_string('0,')
        self.row_data.append(0)

    def write_string(self, data):
        return
        self.f_symbolic.write(data)
        self.f_numeric.write(data)

    def write_non_zero_value(self, change_in_n, in_service):
        if in_service < 0:
            raise RuntimeError('in_service must be >= 0')
        #self.f_symbolic.write('a(%s)d(%s),' % (arrivals, departures))
        value_sum = 0
        for depart_count in range(in_service):
            needed_arrivals = change_in_n + depart_count
            if needed_arrivals < 0:
                continue
                value_sum += Evaluator.arrivals(needed_arrivals) * Evaluator.departures(depart_count, in_service)
        #self.f_numeric.write('%s,' % value)
        self.row_data.append(value_sum)

    def write_row(self, current):
        self.row_data = []
        required_next_s = current.n + R
        change_in_s = required_next_s - current.s
        required_next_q = abs(min(change_in_s, 0))
        self.write_string('%s,' % current)
        for next in State.all():
            if next.s != required_next_s:
                self.write_zero()
                continue
            if next.q != required_next_q:
                self.write_zero()
                continue
            change_in_n = next.n - current.n
            if current.n >= current.s:
                in_service = current.s
            else:
                in_service = current.n
            self.write_non_zero_value(change_in_n=change_in_n, in_service=in_service)
        self.write_string('\n')
        self.data.append(self.row_data)

    def write_header(self):
        self.write_string('(n;s;q),')
        self.write_string(','.join([str(state) for state in State.all()]))
        self.write_string('\n')


class SumOutputs(object):

    @classmethod
    def run(cls, filepath):
        data = []
        with open(filepath, 'r') as f_ans:
            for line, state in zip(f_ans, State.all()):
                ans = float(line.strip())
                data.append((state, ans))
        cls.sum_outputs(data)
        cls.prob_busy(data)
        cls.waiting_time(data)
        cls.utiliation(data)

    @classmethod
    def prob_busy(cls, data):
        prob_sum = 0
        for state, prob in filter(lambda s: s[0].n >= s[0].s, data):
            prob_sum += prob
        print 'P(busy) = %s\n' % prob_sum

    @classmethod
    def waiting_time(cls, data):
        departures_needed = defaultdict(int)
        for state, prob in filter(lambda s: s[0].n > s[0].s, data):
            departures_needed[state.n] += prob
        cls.plot_and_save(departures_needed, 'departures_needed.png')
        expected_waiting_time = 0
        mean_service_time = 1.0 / MU
        for key, prob in departures_needed.iteritems():
            expected_waiting_time += key * mean_service_time * prob
        print 'Expected Waiting Time = %s\n' % expected_waiting_time

    @classmethod
    def utiliation(cls, data):
        unused_servers = []
        for state, prob in data:
            unused_servers.append(max(state.s - state.n, 0) * prob)
        print 'Average Unused Servers = %s\n' % sum(unused_servers)

    @classmethod
    def sum_outputs(cls, data):
        n_sum = defaultdict(float)
        s_sum = defaultdict(float)
        q_sum = defaultdict(float)
        for state, prob in data:
            n_sum[state.n] += prob
            s_sum[state.s] += prob
            q_sum[state.q] += prob
        cls.plot_and_save(n_sum, 'n_sum.png')
        cls.plot_and_save(s_sum, 's_sum.png')
        cls.plot_and_save(q_sum, 'q_sum.png')
        simulation_n_marginals = [0.01158827785611152, 0.05140067515207855, 0.11425892042257983, 0.17162040113767102, 0.1896135483992488, 0.16897409266094127, 0.12613087402380632, 0.08127186429378533, 0.04581734288102304, 0.022596266629325224, 0.010361299398535122, 0.0041036902390239275, 0.0015167424815938962, 0.0005047563727632472, 0.00017770164046772443, 5.041855966161346e-05, 1.312785138358992e-05]
        n_sum_list = [n_sum[i] for i in range(17)]
        residuals = [numeric - sim for sim, numeric in zip(simulation_n_marginals, n_sum_list)]
        print '\nn residuals = %s\n' % residuals

    @classmethod
    def plot_and_save(cls, data, filename):
        print '%s: %s' % (filename, data)
        x_value = []
        y_value = []
        for k, v in data.iteritems():
            x_value.append(k)
            y_value.append(v)
        plt.plot(x_value, y_value)
        plt.savefig(BASE_PATH + filename)
        plt.close()


if __name__ == "__main__":
    if not os.path.isfile('markov_example_generator.py'):
        print 'Please run in the same directory as markov_example_generator.py'
        exit()
    startTime = datetime.now()
    TableMaker().run()
    # SumOutputs.run(BASE_PATH + 'markov_sparse_125.txt')
    # SumOutputs.run(BASE_PATH + 'markov_sparse_4913.txt')
    d = datetime.now() - startTime
    time_in_seconds = d.days * 60 * 60 * 24 + d.seconds + d.microseconds / 1e6
    num_states = len([c for c in State.all()])
    print '(%s, %s, %s), %s, %s' % (MAX_CUSTOMERS, MAX_SERVERS, R, time_in_seconds, num_states)
