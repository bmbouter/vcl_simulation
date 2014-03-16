import os

alpha_values = ['0.01', '0.02', '0.03', '0.04', '0.05', '0.06', '0.07', '0.08', '0.09', '0.1', '0.11', '0.12', '0.13', '0.14', '0.15', '0.16', '0.17', '0.18', '0.19', '0.2', '0.21', '0.22', '0.23', '0.24', '0.25', '0.26', '0.27', '0.28', '0.29', '0.3', '0.31', '0.32', '0.33', '0.34', '0.35', '0.36', '0.37', '0.38', '0.39', '0.4', '0.41', '0.42', '0.43', '0.44', '0.45', '0.46', '0.47', '0.48', '0.49', '0.5', '0.51', '0.52', '0.53', '0.54', '0.55', '0.56', '0.57', '0.58', '0.59', '0.6', '0.61', '0.62', '0.63', '0.64', '0.65', '0.66', '0.67', '0.68', '0.69', '0.7', '0.71', '0.72', '0.73', '0.74', '0.75', '0.76', '0.77', '0.78', '0.79', '0.8', '0.81', '0.82', '0.83', '0.84', '0.85', '0.86', '0.87', '0.88', '0.89', '0.9', '0.91', '0.92', '0.93', '0.94', '0.95', '0.96', '0.97', '0.98', '0.99', '1']

arrival_counts_filename = 'data/2008_five_minute_counts.csv'

moving_average_filename = 'data/ma_arrivals/arrivals_k_%s.txt'
exponential_moving_average_filename = 'data/ema_arrivals/arrivals_alpha_%s.txt'

def read_file(filename):
    with open(filename, 'r') as f:
        return [float(line) for line in f]

def abs_diff(file1, file2):
    return [abs(file1[i] - file2[i]) for i in range(len(file1))]

def compute_k_min(arrival_counts):
    k_diff = []
    for k in range(1,51):
        ma_filename = moving_average_filename % k
        ma_counts = read_file(ma_filename)
        diff = abs_diff(arrival_counts, ma_counts)
        k_tuple = (k, sum(diff))
        k_diff.append(k_tuple)
        print 'k=%s  diff=%s' % k_tuple
    k_min = min(k_diff, key = lambda x: x[1])
    print '\nk=%s minimizes k with total absolute difference = %s\n\n' % k_min

def compute_alpha_min(arrival_counts):
    alpha_diff = []
    for alpha in alpha_values:
        ema_filename = exponential_moving_average_filename % alpha
        ema_counts = read_file(ema_filename)
        diff = abs_diff(arrival_counts, ema_counts)
        alpha_tuple = (alpha, sum(diff))
        alpha_diff.append(alpha_tuple)
        print 'alpha=%s  diff=%s' % alpha_tuple
    alpha_min = min(alpha_diff, key = lambda x: x[1])
    print '\nalpha=%s minimizes alpha with total absolute difference = %s\n\n' % alpha_min


def main():
    arrival_counts = read_file(arrival_counts_filename)
    compute_k_min(arrival_counts)
    compute_alpha_min(arrival_counts)


if __name__ == "__main__":
    if not os.path.isfile('diff.py'):
        print 'Please run in the same directory as diff.py'
        exit()
    main()
