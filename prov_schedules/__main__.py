import math
import csv

import numpy

from solver import NoMinimumScheduleSolverMultipleD

def parse_five_minute_arrivals(data_file_path):
    dataReader = csv.reader(open(data_file_path, 'rb'), delimiter=',')
    arrivals = []
    for row in dataReader:
        arrivals_in_five_min = float(row[0])
        arrivals_per_sec = arrivals_in_five_min / 300
        arrivals.append(arrivals_per_sec)
    return arrivals

def erlang_b_loss_recursive(E, c):
    E = float(E)
    if E == 0:
        return 0.0
    s = 0.0
    for i in range(1, c + 1):
        s = (1.0 + s) * (i / E)
    return 1.0 / (1.0 + s)

def find_capacity(E, bp):
    left = 0
    right = int(math.ceil(E))
    R = erlang_b_loss_recursive(E, right)
    while R > bp:
        left = right
        right = right + 32
        R = erlang_b_loss_recursive(E, right)
    while (right - left) > 1:
        mid = int(math.ceil((left + right) / 2))
        mid_bp = erlang_b_loss_recursive(E, mid)
        if mid_bp > bp:
            left = mid
        else:
            right = mid
    return right

def find_C_t(lambda_t, mu, bp):
    return [find_capacity(item / mu, bp) for item in lambda_t]

def smooth(x, window_len=11, window='hanning'):
    """smooth the data using a window with requested size.
    
    This method is based on the convolution of a scaled window with the signal.
    The signal is prepared by introducing reflected copies of the signal 
    (with the window size) in both ends so that transient parts are minimized
    in the begining and end part of the output signal.
    
    input:
        x: the input signal 
        window_len: the dimension of the smoothing window; should be an odd integer
        window: the type of window from 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'
            flat window will produce a moving average smoothing.

    output:
        the smoothed signal
        
    example:

    t=linspace(-2,2,0.1)
    x=sin(t)+randn(len(t))*0.1
    y=smooth(x)
    
    see also: 
    
    numpy.hanning, numpy.hamming, numpy.bartlett, numpy.blackman, numpy.convolve
    scipy.signal.lfilter
 
    TODO: the window parameter could be the window itself if an array instead of a string   
    """
    if x.ndim != 1:
        raise ValueError, "smooth only accepts 1 dimension arrays."
    if x.size < window_len:
        raise ValueError, "Input vector needs to be bigger than window size."
    if window_len<3:
        return x
    if not window in ['flat', 'hanning', 'hamming', 'bartlett', 'blackman']:
        raise ValueError, "Window is on of 'flat', 'hanning', 'hamming', 'bartlett', 'blackman'"

    s=numpy.r_[2*x[0]-x[window_len-1::-1],x,2*x[-1]-x[-1:-window_len:-1]]
    if window == 'flat': #moving average
        w=numpy.ones(window_len,'d')
    else:
        w=eval('numpy.'+window+'(window_len)')
    y=numpy.convolve(w/w.sum(),s,mode='same')
    return y[window_len:-window_len+1]

def write_year_provision_schedule(schedule_in_seconds, output_file_path):
    f = open(output_file_path, 'w')
    for s in schedule_in_seconds:
        f.write('%s,%s\n' % s)
    f.close()

def main():
    density = 2
    ws = 8
    data_file = parse_five_minute_arrivals('/home/bmbouter/simulations/rewrite/prov_schedules/2008_five_minute_counts.csv')
    smooth_lambdas = smooth(numpy.array(data_file), window_len=ws)
    mu = 1 / 4957.54969411
    C_t = find_C_t(smooth_lambdas, mu, 0.01)
    solver = NoMinimumScheduleSolverMultipleD()
    schedule = solver.solve(C_t, density)
    schedule_in_seconds = [s.get_time_in_seconds() for s in schedule]
    write_year_provision_schedule(schedule_in_seconds, '/home/bmbouter/simulations/rewrite/data/2008_optimal_provisioning_schedule.csv')

if __name__ == "__main__":
    main()
