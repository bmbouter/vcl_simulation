import math
import os
import re

from scipy.stats import norm


DATA_DIR = '/home/bmbouter/Documents/Research/matlab/OLD_increasing_traffic_intensity/'


def get_result_data():
    # gauss_match = re.compile('n_sum\.png.*({.*})') # n_sum
    gauss_match = re.compile('s_sum\.png.*({.*})') # s_sum
    sim_match = re.compile('server probabilities in system.*(\[.*\])')  # s_sum
    result_data = {'gauss': {}, 'sim': {}}
    for root, sub_folders, filenames in os.walk(DATA_DIR):
        for filename in filenames:
            if filename == 'results.txt':
                test_name = root.split(os.sep)[-1]
                full_path = os.path.join(root, filename)
                intensity = int(re.findall(r'\d+', test_name)[0])
                with open(full_path, 'r') as file_handle:
                    full_text = file_handle.read()
                for match in gauss_match.finditer(full_text):
                    data = eval(match.group(1))
                    result_data['gauss'][intensity] = data
            elif filename == 'sim_output.txt':
                test_name = root.split(os.sep)[-1]
                full_path = os.path.join(root, filename)
                intensity = int(re.findall(r'\d+', test_name)[0])
                with open(full_path, 'r') as file_handle:
                    full_text = file_handle.read()
                for match in sim_match.finditer(full_text):
                    data = eval(match.group(1))
                    result_data['sim'][intensity] = data
    return result_data


def RMSE(residual):
    squared_residual = map(lambda x: x ** 2, residual)
    mean_residual = sum(squared_residual) / len(squared_residual)
    return math.sqrt(mean_residual)


def main():
    all_data = get_result_data()
    for intensity, x_y_data in all_data['gauss'].iteritems():
        errors_vs_norm = []
        for x, y in x_y_data.iteritems():
            # y_norm = norm(intensity, math.sqrt(intensity)).pdf(x) # n_sum
            y_norm = norm(intensity + 1, math.sqrt(intensity)).pdf(x)  # s_sum
            errors_vs_norm.append(y_norm - y)
        print 'intensity %s has RMSE vs Norm: %s' % (intensity, RMSE(errors_vs_norm))

    for intensity, y_data in all_data['sim'].iteritems():
        errors_vs_gauss = []
        for x_value in range(len(y_data)):
            sim_value = y_data[x_value]
            try:
                gauss_value = all_data['gauss'][intensity][x_value]
            except KeyError:
                gauss_value = 0
            errors_vs_gauss.append(sim_value - gauss_value)
        print 'intensity %s has RMSE vs Sim: %s' % (intensity, RMSE(errors_vs_gauss))


if __name__ == "__main__":
    if not os.path.isfile('normal_rmse.py'):
        print 'Please run in the same directory as normal_rmse.py'
        exit()
    main()