from collections import defaultdict
from datetime import datetime
import math
import os

import matplotlib.pyplot as plt


MAX_CUSTOMERS = 25
MAX_SERVERS = 25
MAX_Q = 5
R = 1

PERIOD_LENGTH = 300

LAMBDA = 1 / 2984.974874
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
        # From equation 2.131 on page 60
        return (math.pow((LAMBDA * PERIOD_LENGTH), count) / math.factorial(count)) * math.exp(-LAMBDA * PERIOD_LENGTH)

    @classmethod
    def departures(cls, count, num_customers_in_service):
        """
        Numerically evaluate the probability of 'count' departures occurring.

        :param count: The number of departures which would occur
        :param num_customers_in_service: the number of customers who are in service

        :return: The probability of count departures occurring.
        """
        # Also using equation 2.131 but with a scaled Mu for the number of in-service customers
        scaled_mu = num_customers_in_service * MU
        return (math.pow((scaled_mu * PERIOD_LENGTH), count) / math.factorial(count)) * math.exp(-scaled_mu * PERIOD_LENGTH)


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
        global MAX_Q
        for n in range(MAX_CUSTOMERS + 1):
            for s in range(1, MAX_SERVERS + 1):
                if MAX_Q is None:
                    MAX_Q = MAX_SERVERS
                for q in range(MAX_Q + 1):
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
                try:
                    scale = 1.0 / sum(row_data)
                except ZeroDivisionError as exc:
                    scale = 0
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
        for depart_count in range(in_service + 1):
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
        cls.queue_length_distribution(data)
        cls.waiting_time_distribution(data)
        cls.unused_seat_distribution(data)
        cls.utilization_distribution(data)

    @classmethod
    def utilization_distribution(cls, data):
        utilization_distribution = defaultdict(int)
        for state, prob in data:
            unused_seats = max(state.s - state.n, 0)
            state_utilization = (state.s - unused_seats) / float(state.s)
            utilization_distribution[state_utilization] += prob
        cls.plot_and_save(utilization_distribution, 'utilization_distribution.png')

    @classmethod
    def unused_seat_distribution(cls, data):
        unused_seat_distribution = defaultdict(int)
        for state, prob in data:
            unused_seats = max(state.s - state.n, 0)
            unused_seat_distribution[unused_seats] += prob
        cls.plot_and_save(unused_seat_distribution, 'unused_seat_distribution.png')

    @classmethod
    def waiting_time_distribution(cls, data):
        waiting_time_distribution = defaultdict(int)
        expected_service_time = 1.0 / MU
        for state, prob in data:
            state_queue_length = max(state.n - state.s, 0)
            expected_wait_time = state_queue_length * expected_service_time
            expected_wait_time_including_provisioning = min(expected_wait_time, PERIOD_LENGTH)
            waiting_time_distribution[expected_wait_time_including_provisioning] += prob
        cls.plot_and_save(waiting_time_distribution, 'waiting_time_distribution.png')

    @classmethod
    def queue_length_distribution(cls, data):
        queue_length_distribution = defaultdict(int)
        for state, prob in data:
            state_queue_length = max(state.n - state.s, 0)
            queue_length_distribution[state_queue_length] += prob
        cls.plot_and_save(queue_length_distribution, 'queue_length_distribution.png')

    @classmethod
    def prob_busy(cls, data):
        prob_sum = 0
        for state, prob in filter(lambda s: s[0].n >= s[0].s, data):
            prob_sum += prob
        print 'P(busy) = %s\n' % prob_sum

    @classmethod
    def sum_outputs(cls, data):
        n_sum = defaultdict(float)
        s_sum = defaultdict(float)
        q_sum = defaultdict(float)
        for state, prob in data:
            n_sum[state.n] += prob
            s_sum[state.s] += prob
            q_sum[state.q] += prob

        # extend lower section for R values not produced by Gauss Seidel output
        for value in range(min(s_sum.keys())):
            s_sum[value] = 0

        n_R_01 = [6.659049981877872e-05, 0.00035388094189408117, 0.002328764907948147, 0.008614908090840857, 0.019332173390246014, 0.03888980318702132, 0.0656582328213158, 0.09213937201353499, 0.11336461818434343, 0.12249037053807978, 0.12397153351262033, 0.11287089719282992, 0.0935720190524933, 0.07166945236924242, 0.05109203663238524, 0.03455476164881883, 0.02154107539852036, 0.012958511264734338, 0.007322101101501996, 0.0039012519965258784, 0.0020214973159272107, 0.0008409428834257197, 0.0003053650063118281, 0.00011986289967380168, 1.9025857091079633e-05, 9.512928545539816e-07]
        simulation_n_marginals = n_R_01
        if len(simulation_n_marginals) != len(n_sum):
            raise RuntimeError('n_sum and simulation_n_marginals are not the same length, something is wrong...')
        n_sum_list = [n_sum[i] for i in sorted(n_sum.keys())]
        n_residuals = [numeric - sim for sim, numeric in zip(simulation_n_marginals, n_sum_list)]
        print '\nn residuals = %s' % n_residuals
        print 'n RMSE = %s\n' % cls.RMSE(n_residuals)

        s_R_01 = [1.9025857091079632e-06, 6.944437838244065e-05, 0.0003862248989489165, 0.0025019002074769714, 0.009076285125299538, 0.02027014814483624, 0.040517465261163184, 0.06794038437939082, 0.09429214774339066, 0.1148847841659207, 0.12357008792799855, 0.12394014084842005, 0.11181210824571133, 0.09197004188542439, 0.06992287868828131, 0.04940634569411559, 0.033173484424006444, 0.02051177652989295, 0.01229260626654655, 0.0068350391599703575, 0.0036329874115416556, 0.0018369465021437384, 0.000769595919334171, 0.00027111846354788474, 9.988574972816807e-05, 1.4269392818309724e-05]
        simulation_s_marginals = s_R_01
        if len(simulation_s_marginals) != len(s_sum):
            raise RuntimeError('s_sum and simulation_s_marginals are not the same length, something is wrong...')
        s_sum_list = [s_sum[i] for i in sorted(s_sum.keys())]
        s_residuals = [numeric - sim for sim, numeric in zip(simulation_s_marginals, s_sum_list)]
        print '\ns residuals = %s' % s_residuals
        print 's RMSE = %s\n' % cls.RMSE(s_residuals)

        cls.plot_and_save(n_sum, 'n_sum.png', simulation_n_marginals)
        cls.plot_and_save(s_sum, 's_sum.png', simulation_s_marginals)
        cls.plot_and_save(q_sum, 'q_sum.png')

    @classmethod
    def RMSE(cls, residual):
        squared_residual = map(lambda x: x ** 2, residual)
        mean_residual = sum(squared_residual) / len(squared_residual)
        return math.sqrt(mean_residual)

    @classmethod
    def plot_and_save(cls, data, filename, sim_data=None):
        print '%s: %s' % (filename, data)
        x_value = []
        y_value = []
        for k, v in data.iteritems():
            x_value.append(k)
            y_value.append(v)
        if filename == 'utilization_distribution.png':
            plt.hist(x_value, bins=200, weights=y_value, label='Gauss-Seidel')
            loc = 'upper left'
        else:
            plt.plot(x_value, y_value, label='Gauss-Seidel')
            loc = 'upper right'
            if sim_data:
                plt.plot(x_value, sim_data, label='Simulation')
        plt.legend(loc=loc, shadow=True, fontsize='x-large')
        plt.savefig(BASE_PATH + filename)
        plt.close()


if __name__ == "__main__":
    if not os.path.isfile('markov_example_generator.py'):
        print 'Please run in the same directory as markov_example_generator.py'
        exit()
    startTime = datetime.now()

    # TableMaker().run()
    # SumOutputs.run(BASE_PATH + 'markov_sparse_3900.txt_10000')

    d = datetime.now() - startTime
    time_in_seconds = d.days * 60 * 60 * 24 + d.seconds + d.microseconds / 1e6
    num_states = len([c for c in State.all()])
    print '(%s, %s, %s), %s, %s' % (MAX_CUSTOMERS, MAX_SERVERS, R, time_in_seconds, num_states)
