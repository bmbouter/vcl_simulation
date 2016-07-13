import os

from appsim.sim import ReservePolicyFixedPoissonSim

from common import fixed_startup_delay

LAMBDA = 1 / 18.640048
MU = 1 / 186.40048
R = 9

three_hundred_second_startup_delay = fixed_startup_delay(300)


def reserve_policy_sim():
    reserve = ReservePolicyFixedPoissonSim()
    results = reserve.run(R, 1, 300, LAMBDA, MU, three_hundred_second_startup_delay, 300)
    #reserve.run(reserved, density, scale_rate, lamda, mu, startup_delay_func, shutdown_delay)
    print results


if __name__ == "__main__":
    if not os.path.isfile('vcl_sim.py'):
        print 'Please run in the same directory as vcl_sim.py'
        exit()

    reserve_policy_sim()