# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os

import matplotlib.pyplot as plt
from numpy import linspace, zeros
from scipy import integrate

from markov_example_generator import dP_dt


filename_base = '/home/bmbouter/Documents/Research/diff_eq_images/%s.png'


def main():
    # LAMBDA = 1.0 / 179.711763670048
    # MU = 1.0 / 4957.2

    # LAMBDA = 1.0 / 17.9

    LAMBDA = 1 / 18.640048
    MU = 1 / 186.40048

    PERIOD_LENGTH = 300
    # PERIOD_LENGTH = 3600
    # PERIOD_LENGTH = 28000
    MAX_CUSTOMERS = 5
    steps_per_time_unit = 100.0

    filename = 'interarrival=%0.2f,service=%0.2f,length=%d' % (1.0 / LAMBDA, 1.0 / MU, PERIOD_LENGTH)

    t = linspace(0, PERIOD_LENGTH, PERIOD_LENGTH * steps_per_time_unit)
    initial_conditions = zeros(MAX_CUSTOMERS + 1)
    initial_conditions[0] = 1

    args = (LAMBDA, MU, MAX_CUSTOMERS + 1)
    X, infodict = integrate.odeint(dP_dt, initial_conditions, t, args=args, full_output=True)

    for n_customers in range(len(initial_conditions)):
        y_values = [y_vector[n_customers] for y_vector in X]
        if n_customers == MAX_CUSTOMERS:
            # the last one so the label is different
            label_msg = 'P(n≥%d)'
        else:
            label_msg = 'P(n=%d)'
        plt.plot(t, y_values, label=label_msg % n_customers)
    plt.legend(loc='best', prop={'size': 14}, scatterpoints=1)
    full_path = filename_base % filename
    plt.savefig(full_path, bbox_inches='tight')
    plt.close()


if __name__ == "__main__":
    if not os.path.isfile('diff_eq_plot.py'):
        print 'Please run in the same directory as diff_eq_plot.py'
        exit()
    main()