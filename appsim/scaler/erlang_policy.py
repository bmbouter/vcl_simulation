import cPickle
import csv
from itertools import count
import math
import os
import signal
from subprocess import check_output
import sys

from scaler import Scale

def get_erlang_c_memo_filename():
    filename = 'service_level_memo.pkl'
    return 'data' + os.sep + 'erlang_c_memo_data' + os.sep + filename


def load_erlang_c_memo_file(filename):
    try:
        dict_file = open(filename, 'rb')
    except IOError:
        service_level_memo = {}
    else:
        service_level_memo = cPickle.load(dict_file)
        dict_file.close()
    return service_level_memo


filename = get_erlang_c_memo_filename()
service_level_memo = load_erlang_c_memo_file(filename)


def sigint_handler(signal, frame):
    save_service_level_memo()
    sys.exit(0)


def save_service_level_memo():
    output = open(filename, 'wb')
    cPickle.dump(service_level_memo, output)
    output.close()

#signal.signal(signal.SIGINT, sigint_handler)


def find_erlang_C_service_level_memoized(s, t, lamda, avg_service_time):
    """
    Finds the number of servers such that the P(wait <= t) >= s.

    This function is memoized so that all previously calculated results are
    stored and returned if it has already been calculated. Memoized results
    are considered unique using the key tuple(s, t, lamda).

    :param s: The minimum acceptable value for P(wait <= t)
    :param t: The target answer time
    :param lamda: The arrival rate parameter
    :param avg_service_time: The average service time

    :return: The number of servers such that P(wait <= t) >= s
    """
    global service_level_memo
    memo_tuple = tuple([s, t, lamda])
    previous_result = service_level_memo.get(memo_tuple)
    if previous_result is not None:
        return previous_result
    for i in count(int(math.ceil(lamda * avg_service_time))):
        service_level = find_erlang_C_service_level(t, lamda, avg_service_time, i)
        if service_level >= s:
            service_level_memo[memo_tuple] = i
            return i


def find_erlang_C_service_level(t, lamda, avg_service_time, N):
    """
    Computes the Erlang C probability P(wait <= t)

    :param t: The target answer time
    :param lamda: The arrival rate parameter
    :param avg_service_time: The average service time
    :param N: The number of servers

    :return: P(wait <= t)
    """
    if lamda == 0:
        return 1.0
    E = lamda * avg_service_time

    try:
        P_block = erlang_C(E, N)
    except OverflowError:
        arg_string = 'A=%f;M=%f' % (E, N)
        raw_output = check_output(["appsim/scaler/erlang_c_mathematica.sh", arg_string])
        P_block = float(raw_output.strip())

    exponent = -(N - E) * (t / avg_service_time)
    return 1.0 - (P_block * math.exp(exponent))


def erlang_C(E, N):
    """
    Compute the probability of queueing in an M/M/N system.

    :param E: The offered traffic load in Erlangs
    :param N: The number of servers

    :return: The probability a customer will queue.
    """
    E_to_N_divide_factorial_N = (E**N) / math.factorial(N)
    numerator = E_to_N_divide_factorial_N
    denominator_sum = 0
    for i in range(N):
        denominator_sum += (E**i) / math.factorial(i)
    denominator = E_to_N_divide_factorial_N + (1 - (E / N)) * denominator_sum
    return (numerator / denominator)

def ErlangBLossEquation(offered_traffic_in_erlangs, num_servers):
    """Computes the Erlang B Blocking Probability from the closed form Erlang B
    loss equation"""

    numerator = (math.pow(offered_traffic_in_erlangs, num_servers)) / math.factorial(num_servers)
    denominator = 0
    for k in range(0, num_servers + 1):
        denominator = denominator + math.pow(offered_traffic_in_erlangs, k) / math.factorial(k)
    return numerator / denominator

def erlang_b_loss_recursive(E, c):
    """Computes the Erlang B Blocking Probability from the closed form Erlang B
    loss equation using a numerically stable, recursive method"""
    E = float(E)
    if E == 0:
        return 0.0
    s = 0.0
    for i in range(1, c + 1):
        s = (1.0 + s) * (i / E)
    return 1.0 / (1.0 + s)


class ProvisioningDataFileEmptyException(Exception):
    pass


class ErlangBFormulaFixedPolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler policy attempts to provision based on blocking probability
    predicted from the closed-form erlang B loss equation for an M/M/C queue.
    A threshold is used to request and release server resources from the
    cluster.

    This policy is designed for steady state and does not consider the current
    number of customers in the system. This policy sets
    self.num_vms_in_cluster to the smallest value that permits the Erlang-B
    predicted blocking probability to be less than the threshold value
    'worst_bp'. This is set the first time the scaler is called, and no
    further requests are made to modify the cluster size afterwards.

    """

    def __init__(self, sim, scale_rate, startup_delay_func,
            shutdown_delay, worst_bp, lamda, mu):
        """Initializes a ErlangBFormulaFixedPolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        worst_bp -- ensure the blocking probability does exceed this value as
            computed by the closed form Erlang B formula.
        lamda -- the parameter to a Poisson distribution (in seconds)
            which defines the arrival process
        mu -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process

        """

        self.worst_bp = worst_bp
        self.lamda = lamda
        self.mu = mu
        self.offered_traffic_in_erlangs = float(self.lamda) / self.mu
        self.scaler_logic_called = False
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay_func=startup_delay_func,
                             shutdown_delay=shutdown_delay)

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        """
        servers_to_stop = 0
        if self.scaler_logic_called is False:
            servers_to_start = -1
            calculated_bp = 1.0
            while (calculated_bp >= self.worst_bp):
                servers_to_start = servers_to_start + 1
                possible_capacity = servers_to_start * self.sim.cluster.density
                calculated_bp = ErlangBLossEquation(self.offered_traffic_in_erlangs, possible_capacity)
            self.scaler_logic_called = True
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop


class ErlangBFormulaDataPolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler policy attempts to provision based on blocking probability
    predicted from the closed-form erlang B loss equation for an M/M/C queue.
    A threshold is used to request and release server resources from the
    cluster.

    This policy is designed for time-varying Poisson predicted demand as a
    series of lamda values. This policy does not consider the current number of
    customers in the system. This policy sets self.num_vms_in_cluster to the
    smallest value that permits the Erlang-B predicted blocking probability to
    be less than the threshold value 'worst_bp'. Each call to scaler_logic will
    iterate one item in the list of lamdas.

    """

    def __init__(self, sim, scale_rate, startup_delay_func,
                 shutdown_delay, worst_bp, pred_user_count_file_path, mu, lag):
        """Initializes a ErlangBFormulaDataPolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        worst_bp -- ensure the blocking probability does exceed this value as
            computed by the closed form Erlang B formula.
        pred_user_count_file_path -- a file path to the arrival predictions
            data file with one comma separated column containing user count
            values
        mu -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process. Used for prediction
            purposes.
        lag -- an integer number of periods to lag values in
            pred_user_count_file_path by. Effectively, this introduces lag
            number of zeros at the beginning of pred_user_count_file

        """
        self.worst_bp = worst_bp
        self.pred_user_count_file_path = pred_user_count_file_path
        self.mu = mu
        self.lag = lag
        self.slot_iter = count(0)
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay_func=startup_delay_func,
                             shutdown_delay=shutdown_delay)
        self.capacity_plan = self.parse_pred_user_count_erlang_C()

    def parse_pred_user_count_erlang_C(self):
        """
        Builds a capacity plan using Erlang C.

        Erlang C does not have a loss assumption so customers queue. This
        function has hard-coded values for the service level and target
        maximum wait time.
        """
        events = []
        dataReader = csv.reader(open(self.pred_user_count_file_path, 'rb'),
                                         delimiter=',')
        average_service_time = 1.0 / self.mu
        i = 0
        for row in dataReader:
            i = i + 1
            pred_user_count = float(row[0])
            offered_traffic_lambda = pred_user_count / self.scale_rate
            minimum_seats = find_erlang_C_service_level_memoized(0.99, 15, offered_traffic_lambda, average_service_time)
            num_servers = int(math.ceil(minimum_seats / float(self.sim.cluster.density)))
            events.append(num_servers)
        return events

    def parse_pred_user_count_erlang_B(self):
        """
        Builds a capacity plan using Erlang B which has a loss assumption.
        """
        events = []
        dataReader = csv.reader(open(self.pred_user_count_file_path, 'rb'),
                                         delimiter=',')
        memoizer = {}
        for row in dataReader:
            pred_user_count = float(row[0])
            if pred_user_count in memoizer:
                num_servers = memoizer[pred_user_count]
            else:
                calculated_bp = 1.0
                num_servers = -1
                while (calculated_bp >= self.worst_bp):
                    num_servers = num_servers + 1
                    possible_capacity = num_servers * self.sim.cluster.density
                    offered_traffic_in_erlangs = (pred_user_count / self.scale_rate) / self.mu
                    calculated_bp = erlang_b_loss_recursive(offered_traffic_in_erlangs, possible_capacity)
                memoizer[pred_user_count] = num_servers
            events.append(num_servers)
        return events

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        """
        if self.lag > 0:
            self.lag = self.lag - 1
            return (0, 0)
        i = self.slot_iter.next()
        try:
            required_capacity = self.capacity_plan[i]
        except IndexError:
            raise ProvisioningDataFileEmptyException()
        current_servers = len(self.sim.cluster.booting) + len(self.sim.cluster.active)
        servers_to_start = max(required_capacity - current_servers, 0)
        servers_to_stop = max(current_servers - required_capacity, 0)
        return servers_to_start, servers_to_stop
