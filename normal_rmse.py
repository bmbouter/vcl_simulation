import math
import os
import re

from scipy.stats import norm


DATA_DIR = '/home/bmbouter/Documents/Research/matlab/OLD_increasing_traffic_intensity/'


def get_result_data():
    # pattern_match = re.compile('n_sum\.png.*({.*})') # n_sum
    pattern_match = re.compile('s_sum\.png.*({.*})') # s_sum
    result_data = {}
    for root, sub_folders, filenames in os.walk(DATA_DIR):
        for filename in filenames:
            if filename == 'results.txt':
                test_name = root.split(os.sep)[-1]
                full_path = os.path.join(root, filename)
                intensity = int(re.findall(r'\d+', test_name)[0])
                with open(full_path, 'r') as file_handle:
                    full_text = file_handle.read()
                for match in pattern_match.finditer(full_text):
                    data = eval(match.group(1))
                    result_data[intensity] = data
    return result_data


def RMSE(residual):
    squared_residual = map(lambda x: x ** 2, residual)
    mean_residual = sum(squared_residual) / len(squared_residual)
    return math.sqrt(mean_residual)


def main():
    all_data = get_result_data()
    for intensity, x_y_data in all_data.iteritems():
        errors = []
        for x, y in x_y_data.iteritems():
            # y_norm = norm(intensity, math.sqrt(intensity)).pdf(x) # n_sum
            y_norm = norm(intensity + 1, math.sqrt(intensity)).pdf(x)  # s_sum
            errors.append(y_norm - y)
        print 'intensity %s has RMSE %s' % (intensity, RMSE(errors))


if __name__ == "__main__":
    if not os.path.isfile('normal_rmse.py'):
        print 'Please run in the same directory as normal_rmse.py'
        exit()
    main()