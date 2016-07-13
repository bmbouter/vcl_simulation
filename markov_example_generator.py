from collections import defaultdict
from datetime import datetime
import math
import os

import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from numpy import linspace, zeros
from scipy import integrate


MAX_CUSTOMERS = 25
MAX_SERVERS = 25
MAX_Q = 5
R = 1

PERIOD_LENGTH = 300

LAMBDA = 1 / 2984.974874
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
        # args = (LAMBDA, MU, current.s)
        args = (LAMBDA / steps_per_time_unit, MU / steps_per_time_unit, current.s)
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
        for state, prob in data:
            n_sum[state.n] += prob
            s_sum[state.s] += prob
            q_sum[state.q] += prob

        # extend lower section for R values not produced by Gauss Seidel output
        for value in range(min(s_sum.keys())):
            s_sum[value] = 0

        # n_violation_01 = [0.36755244148545635, 0.36733497577340646, 0.18482245342038559, 0.06156543729729123, 0.01519995905632438, 0.002928652410834777, 0.0005388126828742993, 4.3188728464853794e-05, 1.4079144962110927e-05]
        # n_violation_20 = [0.36327770866101433, 0.36201020509699294, 0.1861097438908402, 0.06597068760070632, 0.01781544562111004, 0.003947868350862186, 0.0007429602848249078, 0.00010578384593153616, 1.6362249550561348e-05, 2.2831045884504207e-06, 7.610348628168069e-07, 1.9025871570420173e-07]
        # n_violation_40 = [0.35419970429990405, 0.35253684312464934, 0.18924615881922396, 0.07343891296824334, 0.022848930203780402, 0.006008560500654395, 0.0013759510319727869, 0.00028196341667362695, 5.346269911288069e-05, 8.561642206689078e-06, 7.610348628168069e-07, 1.9025871570420173e-07]
        # n_violation_60 = [0.34552238479406683, 0.34269704286585945, 0.18982568686725898, 0.08021668945648983, 0.02895528368430676, 0.009214800377701603, 0.0026714226272026966, 0.0006906391380062523, 0.00015829525146589585, 3.805174314084035e-05, 7.6103486281680696e-06, 2.092845872746219e-06]
        # n_violation_80 = [0.33717954011043755, 0.33218600985806507, 0.18738733116679393, 0.08598362138819989, 0.035733821207416055, 0.01385920588675687, 0.0050972212524312685, 0.0017576100156754156, 0.0005761033911523229, 0.00016933025697673954, 5.3272440397176484e-05, 1.198629908936471e-05, 4.185691745492438e-06, 3.8051743140840347e-07, 3.8051743140840347e-07]
        n_violation_99 = [0.32072553203431126, 0.3166511447382565, 0.17748080528842725, 0.08876133108701381, 0.04615292413154097, 0.024362610005127468, 0.012802499236587484, 0.006650488346186885, 0.003355209898011893, 0.0015934155313779192, 0.0007372519622793357, 0.000413812391730982, 0.0001607684924196229, 8.466506405530436e-05, 3.139266420028139e-05, 1.9977149945633613e-05, 8.561635690985835e-06, 6.659049981877871e-06, 9.512928545539816e-07]

        simulation_n_marginals = n_violation_99
        if len(simulation_n_marginals) != len(n_sum):
            raise RuntimeError('n_sum and simulation_n_marginals are not the same length, something is wrong...')
        n_sum_list = [n_sum[i] for i in sorted(n_sum.keys())]
        n_residuals = [numeric - sim for sim, numeric in zip(simulation_n_marginals, n_sum_list)]
        print '\nn residuals = %s' % n_residuals
        print 'n RMSE = %s\n' % cls.RMSE(n_residuals)

        # s_violation_01 = [3.8051743140840347e-07, 0.3711454773315302, 0.36731043239908057, 0.18306008693681755, 0.06035786522871666, 0.014746382278085564, 0.0028156387337064815, 0.0005093225819401481, 4.109588259210757e-05, 1.3318110099294121e-05]
        # s_violation_20 = [3.8051743140840347e-07, 0.41396757725122196, 0.3611222676708014, 0.1606930820599159, 0.04959264657674152, 0.011796230632376212, 0.002366057388497453, 0.0003987822681160068, 5.3272440397176484e-05, 7.990866059576473e-06, 1.3318110099294121e-06, 3.8051743140840347e-07]
        # s_violation_40 = [3.8051743140840347e-07, 0.41470711287916423, 0.36303912423152124, 0.1561613097105575, 0.04929850660226282, 0.01297164897799677, 0.0030348167741977216, 0.0006434549765116103, 0.00011738962758949247, 2.3401822031616813e-05, 2.092845872746219e-06, 5.707761471126052e-07, 1.9025871570420173e-07]
        # s_violation_60 = [3.8051743140840347e-07, 0.38713729316261547, 0.3785269447247061, 0.160769566063629, 0.05192065222209813, 0.015703764135509106, 0.004410767806170508, 0.0011670469621295734, 0.00028253419282073955, 6.107304774104876e-05, 1.5220697256336139e-05, 3.614915598379833e-06, 1.1415522942252103e-06]
        # s_violation_80 = [3.8051743140840347e-07, 0.3404510387269713, 0.398423249919473, 0.17515978402591628, 0.056808018111107667, 0.019192918722808462, 0.006719557321240997, 0.002258751472840283, 0.0007127091490279397, 0.0001978690643323698, 5.308218168147228e-05, 1.8264836707603366e-05, 3.4246568826756313e-06, 7.610348628168069e-07, 1.9025871570420173e-07]
        s_violation_99 = [1.9025857091079632e-06, 0.2513981626729807, 0.3999748858686398, 0.21812384120639156, 0.0752605828951837, 0.028755680407457754, 0.01354926412741236, 0.006934924909698526, 0.003244859926883631, 0.0016067336313416748, 0.0006830282695697588, 0.00027682622067520867, 0.0001189116068192477, 3.9954299891267226e-05, 2.1879735654741576e-05, 4.756464272769908e-06, 9.512928545539816e-07, 9.512928545539816e-07, 1.9025857091079632e-06]

        simulation_s_marginals = s_violation_99
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
            # x = linspace(0, 25, 100)
            # plt.plot(x, mlab.normpdf(x, 10, math.sqrt(10)), label='Normal Distribution')
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
    SumOutputs.run(BASE_PATH + 'markov_sparse_5130.txt_2000')

    d = datetime.now() - startTime
    time_in_seconds = d.days * 60 * 60 * 24 + d.seconds + d.microseconds / 1e6
    num_states = len([c for c in State.all()])
    print '(%s, %s, %s), %s, %s' % (MAX_CUSTOMERS, MAX_SERVERS, R, time_in_seconds, num_states)
