from functools import wraps
import math
import random


class NegativeStartupTimeException(Exception):
    pass


def raise_exception_if_negative_startup_value(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        startup_time = f(*args, **kwargs)
        if startup_time < 0:
            raise NegativeStartupTimeException(
                'Startup Time %s Should Not Be Negative' % startup_time)
        else:
            return startup_time
    return wrapper


def truncate_at_zero(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        startup_time = -1
        while startup_time <= 0:
            startup_time = f(*args, **kwargs)
        return startup_time
    return wrapper


def fixed_startup_delay(startup_delay):
    return lambda: startup_delay


def gamma_startup_delay(alpha, beta):
    return lambda: random.gammavariate(alpha, beta)


def normal_distribution_startup_delay(mu, sigma):
    @truncate_at_zero
    def func():
        return random.normalvariate(mu, sigma)
    return func


if __name__ == "__main__":
    for sigma in range(0, 401, 50):
        func = normal_distribution_startup_delay(300, sigma)
        values = []
        for i in range(1000000):
            values.append(func())
        mean = sum(values) / len(values)
        variance = sum((mean - value) ** 2 for value in values) / len(values)
        print "truncated ~N(300, %s) truncates to ~N(%0.2f, %0.2f)" % (sigma, mean, math.sqrt(variance))