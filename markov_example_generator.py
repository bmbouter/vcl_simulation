from collections import defaultdict
from datetime import datetime
import math
import os

import matplotlib.pyplot as plt


MAX_CUSTOMERS = 25  # 3
MAX_SERVERS = 26  # 4
R = 1

LAMBDA = 2
MU = 3

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
        return cls._compute(count, cls.arrival_memo, LAMBDA)

    @classmethod
    def departures(cls, count):
        """
        Numerically evaluate the probability of 'count' departures occurring.

        :param count: The number of departures which would occur

        :return: The probability of count departures occurring.
        """
        return cls._compute(count, cls.departure_memo, MU)

    @classmethod
    def _compute(cls, count, memo, lambda_or_mu):
        """
        Numerically evaluate the probability of 'count' arrival or departure events occurring.

        This method uses memoization and recursion.

        :param count: The number of arrivals or departures which would occur
        :param memo: The memoization dictionary which is keyed on 'count'
        :param lambda_or_mu: The lambda or mu value to be used in the computation

        :return: The probability of count arrivals or departures occurring
        """
        if count in memo:
            return memo[count]

        if count == 0:
            to_return = math.exp(-lambda_or_mu)
        else:
            P_one_arrival_or_departure = cls._compute(0, memo, lambda_or_mu)
            to_return = pow(1 - P_one_arrival_or_departure, count)

        memo[count] = to_return
        return to_return


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

    def write_non_zero_value(self, arrivals=0, departures=0):
        #self.f_symbolic.write('a(%s)d(%s),' % (arrivals, departures))
        value = Evaluator.arrivals(arrivals) * Evaluator.departures(departures)
        #self.f_numeric.write('%s,' % value)
        self.row_data.append(value)

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
            if change_in_n > 0:
                # more arrivals than departures
                self.write_non_zero_value(arrivals=change_in_n, departures=0)
            elif change_in_n == 0:
                # equal num of arrivals and departures
                self.write_non_zero_value(arrivals=0, departures=0)
            else:
                # more departures than arrivals
                self.write_non_zero_value(arrivals=0, departures=abs(change_in_n))
        self.write_string('\n')
        self.data.append(self.row_data)

    def write_header(self):
        self.write_string('(n;s;q),')
        self.write_string(','.join([str(state) for state in State.all()]))
        self.write_string('\n')


class SumOutputs(object):

    @classmethod
    def run(cls, filepath):
        n_sum = defaultdict(float)
        s_sum = defaultdict(float)
        q_sum = defaultdict(float)
        with open(filepath, 'r') as f_ans:
            for line, state in zip(f_ans, State.all()):
                ans = float(line.strip())
                n_sum[state.n] += ans
                s_sum[state.s] += ans
                q_sum[state.q] += ans
        cls.plot_and_save(n_sum, 'n_sum.png')
        cls.plot_and_save(s_sum, 's_sum.png')
        cls.plot_and_save(q_sum, 'q_sum.png')

    @classmethod
    def plot_and_save(cls, data, filename):
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
    #TableMaker().run()
    SumOutputs.run(BASE_PATH + 'markov_sparse_17576.txt')
    d = datetime.now() - startTime
    time_in_seconds = d.days * 60 * 60 * 24 + d.seconds + d.microseconds / 1e6
    num_states = len([c for c in State.all()])
    print '(%s, %s, %s), %s, %s' % (MAX_CUSTOMERS, MAX_SERVERS, R, time_in_seconds, num_states)
