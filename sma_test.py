import copy

from rpy2.rinterface import NARealType
import rpy2.robjects as robjects
from appsim.n_step_predictors import MovingAverageNStepPredictor

r = robjects.r
r.library('TTR')
SMA = r['SMA']

data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
#data = [0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
k = 2

int_data = robjects.IntVector(data)

predictions = SMA(int_data, n=k)
prediction_list = list(predictions)

p = MovingAverageNStepPredictor(k)
p_data = []

copy_data = copy.copy(data)
copy_data.insert(0, None)
for i, item in enumerate(copy_data):
    p_data.append(p.predict_n_steps(item, 2))

for i, item in enumerate(prediction_list):
    print '%s: %s' % (i, item)

prediction_list.insert(0, 0)
del prediction_list[-1]
for i, item in enumerate(prediction_list):
    if isinstance(item, NARealType):
        prediction_list[i] = 0

print '--------------------'
print '--------------------'

for i, item in enumerate(prediction_list):
    print '%s: %s' % (i, item)

print '--------------------'
print '--------------------'

for i, item in enumerate(p_data):
    print '%s: %s' % (i, item)