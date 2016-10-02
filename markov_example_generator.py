from collections import defaultdict
from datetime import datetime
import math
import os

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from numpy import linspace, zeros
from scipy import integrate


MAX_CUSTOMERS = 7
MAX_SERVERS = 8
MAX_Q = 3
R = 1

PERIOD_LENGTH = 300

LAMBDA = 1 / 29849.74874
MU = 1 / 29849.74874

BASE_PATH = '/home/bmbouter/Documents/Research/matlab/'


def dP_dt(p, t, lam, mu, servers):
    """Returns the set of ordinary differential equations for M/M/C Queues
    describing the current size in population probability of the M/M/C
    birth-death process

    parameters:
    p -- a list of probabilities.  Each index represents the probability the
        system will have index customers in service.  For example, and initial
        state of p = [1, 0, 0] would be a system size of C=2, with a
        probability there are 0 customers in the system.
    t -- the current eq time.  This variable is required by the odeint solver
    lam -- the Poisson arrival process parameter
    mu -- the Poisson service time process parameter
    servers -- the number of servers in the cluster

    """
    size = len(p)
    if size == 1:
        derivative = p
    elif size == 2:
        derivative = [(mu * p[1]) - (lam * p[0]), (lam * p[0]) - (mu * p[1])]
    else:
        X = zeros(size)
        for i in range(0, size):
            if i != 0:
                # account for transitions from/to left states
                if i < servers:
                    departure_rate_to_left = (i * mu) * p[i]
                else:
                    departure_rate_to_left = (servers * mu) * p[i]
                arrival_rate_from_left = lam * p[i - 1]
                X[i] += arrival_rate_from_left - departure_rate_to_left
            if i != size - 1:
                # account for transitions from/to right states
                if i + 1 < servers:
                    departure_rate_from_right = ((i + 1) * mu) * p[i + 1]
                else:
                    departure_rate_from_right = (servers * mu) * p[i + 1]
                arrival_rate_to_right = lam * p[i]
                X[i] += departure_rate_from_right - arrival_rate_to_right
        derivative = X
    return derivative


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

    def write_row(self, current):
        if current.n >= current.s:
            in_service = current.s
        else:
            in_service = current.n
        steps_per_time_unit = 100.0
        t = linspace(0, PERIOD_LENGTH, PERIOD_LENGTH * steps_per_time_unit)
        initial_conditions = zeros(MAX_CUSTOMERS + 1)
        initial_conditions[in_service] = 1
        args = (LAMBDA, MU, current.s)
        X, infodict = integrate.odeint(dP_dt, initial_conditions, t, args=args, full_output=True)
        end_probabilities = X[-1]
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
            self.row_data.append(end_probabilities[next.n])
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
        queue_length_sum = defaultdict(float)
        for state, prob in data:
            n_sum[state.n] += prob
            s_sum[state.s] += prob
            q_sum[state.q] += prob
            queue_length = max(state.n - state.s, 0)
            queue_length_sum[queue_length] += prob

        # extend lower section for R values not produced by Gauss Seidel output
        for value in range(min(s_sum.keys())):
            s_sum[value] = 0

        simulation_n_marginals = [0.37338910446241963, 0.3678668494417338, 0.1834891709577902, 0.05826383346286771, 0.013573997741630763, 0.0028053626280796916, 0.0006002657912235624, 1.1415514254647778e-05]

        if len(simulation_n_marginals) != len(n_sum):
            raise RuntimeError('n_sum and simulation_n_marginals are not the same length, something is wrong...')
        n_sum_list = [n_sum[i] for i in sorted(n_sum.keys())]
        n_residuals = [numeric - sim for sim, numeric in zip(simulation_n_marginals, n_sum_list)]
        print '\nn residuals = %s' % n_residuals
        print 'n RMSE = %s\n' % cls.RMSE(n_residuals)

        simulation_s_marginals = [1.9025857091079632e-06, 0.3770325560953614, 0.3677669636920056, 0.18168076324128307, 0.05707186351611157, 0.01315257500706335, 0.0027111846354788477, 0.000571727005586943, 1.0464221400093798e-05]

        if len(simulation_s_marginals) != len(s_sum):
            raise RuntimeError('s_sum and simulation_s_marginals are not the same length, something is wrong...')
        s_sum_list = [s_sum[i] for i in sorted(s_sum.keys())]
        s_residuals = [numeric - sim for sim, numeric in zip(simulation_s_marginals, s_sum_list)]
        print '\ns residuals = %s' % s_residuals
        print 's RMSE = %s\n' % cls.RMSE(s_residuals)

        simulation_queue_length_marginals = [0.9998040336719619, 0.00019596632803812021]

        queue_length_sum_list = [queue_length_sum[i] for i in sorted(queue_length_sum.keys())]
        queue_length_residuals = [simulation_queue_length_marginals[i] - queue_length_sum_list[i] for i in range(len(simulation_queue_length_marginals))]
        print '\nqueue length residuals = %s' % queue_length_residuals
        print 'queue length RMSE = %s\n' % cls.RMSE(queue_length_residuals)

        cls.plot_and_save(n_sum, 'n_sum.png', s_sum_list)
        # cls.plot_and_save(n_sum, 'n_sum.png', simulation_n_marginals)
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
            plt.hist(x_value, bins=100, weights=y_value)
            loc = 'upper left'
        else:
            # plt.plot(x_value, y_value, label='Gauss-Seidel')
            plt.plot(x_value, y_value, label='Users')
            # plt.plot(x_value, y_value)
            # x = linspace(0, 25, 100)
            # plt.plot(x, mlab.normpdf(x, 10, math.sqrt(10)), label='Normal Distribution')
            loc = 'upper right'
            # if sim_data:
            #     plt.plot(x_value, sim_data, label='Simulation')

            if sim_data:
                plt.plot(range(len(sim_data)), sim_data, label='Virtual Machines')
        plt.xlabel('Number in the System')
        # plt.xlabel('Number of Customers in the System')
        plt.ylabel('Probability Density')

        plt.legend(loc=loc, shadow=True, fontsize='large')
        # plt.legend(loc=loc, shadow=True, fontsize='x-large')
        plt.savefig(BASE_PATH + filename)
        plt.close()


if __name__ == "__main__":
    if not os.path.isfile('markov_example_generator.py'):
        print 'Please run in the same directory as markov_example_generator.py'
        exit()
    startTime = datetime.now()

    # TableMaker().run()
    SumOutputs.run(BASE_PATH + 'markov_sparse_256.txt')

    d = datetime.now() - startTime
    time_in_seconds = d.days * 60 * 60 * 24 + d.seconds + d.microseconds / 1e6
    num_states = len([c for c in State.all()])
    print '(%s, %s, %s), %s, %s' % (MAX_CUSTOMERS, MAX_SERVERS, R, time_in_seconds, num_states)
