import random


def fixed_startup_delay(startup_delay):
    return lambda: startup_delay


def gamma_startup_delay(alpha, beta):
    return lambda: random.gammavariate(alpha, beta)

