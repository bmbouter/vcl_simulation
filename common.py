import random


def fixed_startup_delay(startup_delay):
    return lambda: startup_delay


def exponential_start_delay(mean_delay):
    lambd = 1.0 / mean_delay
    return lambda: random.expovariate(lambd)

