import os

from appsim.sim import ReservePolicyFixedPoissonSim

from common import fixed_startup_delay

LAMBDA = 1 / 2984.974874
MU = 1 / 29849.74874
R = 1

three_hundred_second_startup_delay = fixed_startup_delay(300)


def reserve_policy_sim():
    reserve = ReservePolicyFixedPoissonSim()
    users_data_file_path = 'data/2008_year_arrivals.txt'
    results = reserve.run(R, 1, 300, LAMBDA, MU, three_hundred_second_startup_delay, 300)
    #reserve.run(reserved, density, scale_rate, lamda, mu, startup_delay_func, shutdown_delay)
    print results


if __name__ == "__main__":
    if not os.path.isfile('vcl_sim.py'):
        print 'Please run in the same directory as vcl_sim.py'
        exit()

    reserve_policy_sim()