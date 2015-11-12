import math
from os import listdir, sep
from pprint import pprint

original_filename = '/home/bmbouter/Documents/Research/vcl_simulation/data/2008_five_minute_counts.csv'
directory_path = '/home/bmbouter/Documents/Research/vcl_simulation/data/ma_arrivals'  # moving average
#directory_path = '/home/bmbouter/Documents/Research/vcl_simulation/data/ema_arrivals'  # exponential moving average
#directory_path = '/home/bmbouter/Documents/Research/vcl_simulation/data/auto_regressive'  # AR(2) and mixed-AR(2)
#directory_path = '/home/bmbouter/Documents/Research/vcl_simulation/data/reserve_arrivals'  # reserve
#directory_path = '/home/bmbouter/Documents/Research/vcl_simulation/arhmm'  # HMM(3)-AR(5)


original = open(original_filename, 'r')
original_list = []
for line in original:
    original_list.append(float(line.strip()))

original_list = original_list[:(len(original_list) / 2)]
residual_list = []

for filename in listdir(directory_path):
    if not filename.endswith('.txt'):
        continue
    prediction_file = open(directory_path + sep + filename, 'r')

    if 'arhmm' in directory_path or 'reserve_arrivals' in directory_path:
        prediction_list = []
    else:
        prediction_list = [0]

    for line in prediction_file:
        #prediction_list.append(float(line.strip()))
        prediction_list.append(math.ceil(float(line.strip())))

    if 'arhmm' not in directory_path and 'reserve_arrivals' not in directory_path:
        del prediction_list[-1]

    prediction_list = prediction_list[:(len(prediction_list) / 2)]
    sse_list = []
    over_accumulator = []
    under_accumulator = []
    for actual, prediction in zip(original_list, prediction_list):
        sse_list.append(math.pow(actual - prediction, 2))
        if actual > prediction:
            under_accumulator.append(actual - prediction)
        if prediction > actual:
            over_accumulator.append(prediction - actual)
    over = sum(over_accumulator)
    under = sum(under_accumulator)
    residual = sum(sse_list)
    residual_list.append((residual, over, under, filename))

sorted_residual_list = sorted(residual_list)
print 'smallest residual is: %s\n\n' % sorted_residual_list[0][3]
print 'entire sorted list:'
pprint(sorted_residual_list)
