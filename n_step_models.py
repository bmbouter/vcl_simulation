import copy
import errno
import math
import operator
import os
import tempfile

import matplotlib.pyplot as plt
import rpy2.robjects as robjects

from appsim.sim import DataDrivenReservePolicySim
from appsim.feature import FeatureFlipper
from appsim.n_step_predictors import (AutoregressiveNStepPredictor, ExponentialMovingAverageNStepPredictor,
                                      MovingAverageNStepPredictor, ReserveNStepPredictor)
from common import fixed_startup_delay
from data.day_cluster.segment_traffic_by_day import get_arrivals_per_time_period, get_clean_arrivals


three_hundred_second_startup_delay = fixed_startup_delay(300)


class AbstractModel(object):

    base_filepath = '/home/bmbouter/Documents/Research/vcl_simulation/data/n_step_images/'
    model_specific_path = None

    def __init__(self, n, arrivals):
        """
        Define the model

        :param n: The number of inspection periods in a boot period
        :type n: integer

        :param arrivals: The arrival counts per inspection period
        :type arrivals: list of integers
        """
        self.n = n
        self.arrivals = arrivals
        self.inspection_time = 300 / n

    def train(self):
        """
        Find which training parameter from _get_training_iterator() minimizes the residual

        Each dictionary of _get_training_iterator() is passed as kwargs to the _train_one() method. Once all parameters
        are evaluated, the parameter which minimizes the residual is found and returned.

        Three residual calculation techiniques are considered. The minimizing parameter is found independently for each
        residual type.

        :return: A dictionary of the parameters which minimize each residual method.
        :rtype: dict
        """
        residuals = []
        for parameter_dict in self._get_training_iterator():
            result_dict = self._train_one(**parameter_dict)
            residuals.append({'params': parameter_dict, 'result': result_dict})
        return self._find_min(residuals)

    def _find_min(self, residuals):
        min_residuals = {
            'simple': reduce(lambda x, y: x if x['result']['simple']['sse'] < y['result']['simple']['sse'] else y, residuals),
            'non_overlap': reduce(lambda x, y: x if x['result']['non_overlap']['sse'] < y['result']['non_overlap']['sse'] else y, residuals),
            'sliding_window': reduce(lambda x, y: x if x['result']['sliding_window']['sse'] < y['result']['sliding_window']['sse'] else y, residuals),
        }

        results = {}
        for key in min_residuals.keys():
            results[key] = {'params': min_residuals[key]['params'], 'result': copy.deepcopy(min_residuals[key]['result'][key])}
        return results

    def _train_one(self, **kwargs):
        """
        train the model with a value from _get_training_iterator().

        :return: A dictionary containing the residual calculations
        :rtype: dict
        """
        residuals = {}
        self.predictions = self._get_predictions(**kwargs)
        del self.predictions[-1]
        assert len(self.predictions)  == len(self.arrivals)
        assert sum(self.arrivals) == 175481
        #print 'inspection_time=%s   n=%s, k=%s' % (inspection_time, n, k)
        residuals['simple'] = self.simple_residual()
        #print 'simple:  over_sum=%(overestimate_sum)s,  under_sum=%(underestimate_sum)s,  sse=%(sse)s' % residual_dict
        residuals['non_overlap'] = self.n_step_residual_non_overlapping()
        #print 'non_overlapping:  over_sum=%(overestimate_sum)s,  under_sum=%(underestimate_sum)s,  sse=%(sse)s' % residual_dict
        residuals['sliding_window'] = self.n_step_residual_sliding()
        #print 'sliding window:  over_sum=%(overestimate_sum)s,  under_sum=%(underestimate_sum)s,  sse=%(sse)s\n' % residual_dict
        return residuals

    def _get_predictions(self, **param_dict):
        """
        Return the arrival count predictions for time slots 300 / n seconds long.

        :return:  A list of predictions
        :rtype: list of float or integers
        """
        predictions = []
        predictor = self.predictor_cls(param_dict)
        predictions.append(predictor.predict_n_steps(None, self.n))
        for arrival in self.arrivals:
            predictions.append(predictor.predict_n_steps(arrival, self.n))
        return predictions

    def _get_training_iterator(self):
        """
        Iterates through parameters that should be used for training

        The _train_one_train() method will be called once for each item returned by this iterator. A subclass is
        required to implement this.

        :return: An iterator of values that should be trained on.
        :rtype: iterator
        """
        raise NotImplementedError

    def get_first_half_year_arrivals_and_pred(self):
        """
        Return the first half of arrival and cleaned predictions

        :returns: A tuple of (self.arrivals, self.predictions) where each attribute is only the first half of
                  the data.
        :rtype: (list, list)
        """
        arrivals = self.arrivals[:(len(self.arrivals) / 2)]
        pred = self.predictions[:(len(self.predictions) / 2)]
        return (arrivals, pred)

    def _compute_residual(self, pred, arrivals):
        residual_data = {}
        pred = map(lambda x: math.ceil(x), pred)
        error = map(operator.sub, pred, arrivals)
        residual_data['overestimate_sum'] = sum(filter(lambda x: x > 0, error))
        residual_data['underestimate_sum'] = sum(filter(lambda x: x < 0, error))
        residual_data['sse'] = sum(map(lambda x: pow(x, 2), error))
        return residual_data

    def simple_residual(self):
        arrivals, pred = self.get_first_half_year_arrivals_and_pred()
        for i in range(len(pred)):
            pred[i] = math.ceil(pred[i])
        return self._compute_residual(pred, arrivals)

    def n_step_residual_non_overlapping(self):
        arrivals, pred = self.get_first_half_year_arrivals_and_pred()
        pred_summed = [0] * 52560
        arrivals_summed = [0] * 52560
        n = (len(arrivals) / 52560)
        index = -1
        for i in range(len(arrivals)):
            if i % n == 0:
                index = index + 1
            arrivals_summed[index] = arrivals_summed[index] + arrivals[i]
            pred_summed[index] = math.ceil(pred_summed[index] + pred[i])
        return self._compute_residual(pred_summed, arrivals_summed)

    def n_step_residual_sliding(self):
        arrivals, pred = self.get_first_half_year_arrivals_and_pred()
        pred_summed = []
        arrivals_summed = []
        n = (len(arrivals) / 52560)
        for i in range(len(arrivals)):
            arrivals_summed.append(sum(arrivals[i:n+i]))
            pred_summed.append(math.ceil(sum(pred[i:n+i])))
        return self._compute_residual(pred_summed, arrivals_summed)


class MovingAverageModel(AbstractModel):

    predictor_cls = MovingAverageNStepPredictor

    def __init__(self, n, arrivals, param_min=None):
        if param_min is None:
            self.k_range = range(1, 51)
        else:
            self.k_range = [param_min['k']]
        super(MovingAverageModel, self).__init__(n, arrivals)

    def _get_training_iterator(self):
        for k in self.k_range:
            yield {'k': k}


class ExponentialMovingAverageModel(AbstractModel):

    predictor_cls = ExponentialMovingAverageNStepPredictor

    def __init__(self, n, arrivals, param_min=None):
        if param_min is None:
            # self.alpha_range = [round(0.01 * ((2 * i) + 1), 2) for i in range(50)]
            self.alpha_range = [round(0.01 * (i+1), 2) for i in range(100)]
        else:
            self.alpha_range = [param_min['alpha']]
        super(ExponentialMovingAverageModel, self).__init__(n, arrivals)

    def _get_training_iterator(self):
        for alpha in self.alpha_range:
            yield {'alpha': alpha}


class AutoregressiveModel(AbstractModel):

    predictor_cls = AutoregressiveNStepPredictor

    def __init__(self, n, arrivals, **kwargs):
        super(AutoregressiveModel, self).__init__(n, arrivals)

    def _get_training_iterator(self):
        int_vector_arrivals = robjects.IntVector(self.arrivals)
        ar_training_results =  robjects.r['ar'](int_vector_arrivals, order_max=2)
        ar_trained_coeff = ar_training_results[1]
        yield {'coeff': [round(ar_trained_coeff[1], 4), round(ar_trained_coeff[0], 4)]}


class ReserveModel(AbstractModel):

    predictor_cls = ReserveNStepPredictor

    def __init__(self, n, arrivals, param_min=None):
        if param_min is None:
            self.R_range = [round(0.05 * (i+1), 2) for i in range(70)]
        else:
            self.R_range = [param_min['R']]
        super(ReserveModel, self).__init__(n, arrivals)

    def _get_training_iterator(self):
        for R in self.R_range:
            yield {'R': R}


class AbstractResidualCalculator(object):

    base_path = '/home/bmbouter/Documents/Research/vcl_simulation/data/n_step_images/'

    def _get_param_values_tuple(self):
        raise NotImplementedError()

    def compute_residuals(self, boot_time=300):
        """
        Compute the residuals for multiple n-step prediction

        :param boot_time: the amount of seconds to divide into n steps
        :type boot_time: int

        :return: residuals
        """
        results_file = open(self.directory_path('results.csv'), 'w')
        residuals_file = open(self.directory_path('residuals.csv'), 'w')

        points_inspection_time = []
        points_mean = []
        points_hour_99 = []
        points_day_99 = []
        points_week_99 = []
        points_utilization = []
        param_values = []

        results_header = 'inspection_time, %s, mean_utilization, total_provisioned_time, wait_time_by_hour_99, wait_time_by_day_99, wait_time_by_week_99, wait_times_year_99_percentile, wait_time_mean' % self.param_key
        print results_header
        results_file.write(results_header + '\n')

        residuals_header = 'inspection_time, simple_min_%s, simple_min_value, non_overlap_min_%s, non_overlap_min_value, sliding_window_min_%s, sliding_window_min_value' % (self.param_key, self.param_key, self.param_key)
        residuals_file.write(residuals_header + '\n')

        for inspection_time in range(boot_time, 19, -1):
            n = boot_time / float(inspection_time)
            if int(n) != n:
                continue
            n = int(n)

            # count arrivals by size inspection_time
            clean_arrivals = get_clean_arrivals()
            arrivals_per_period = get_arrivals_per_time_period(clean_arrivals, inspection_time)

            model = self.model_cls(n, arrivals_per_period)

            # force these values
            #param_min = {'k': 12}
            #param_min = {'alpha': 0.13}
            #param_min = {'coeff': [0.3345, 0.4095]}
            #param_min = {'R': 2.0}
            #model = self.model_cls(n, arrivals_per_period, param_min=param_min)

            model_min = model.train()

            print 'inspection_time=%s, simple_min: %s=%s, non_overlap_min: %s=%s, sliding_window_min: %s=%s' % (
                inspection_time,
                model_min['simple']['params'], model_min['simple']['result']['sse'],
                model_min['non_overlap']['params'], model_min['non_overlap']['result']['sse'],
                model_min['sliding_window']['params'], model_min['sliding_window']['result']['sse'],
            )

            residuals_line = '%s, %s, %s, %s, %s, %s, %s\n' % (
                inspection_time,
                model_min['simple']['params'][self.param_key], model_min['simple']['result']['sse'],
                model_min['non_overlap']['params'][self.param_key], model_min['non_overlap']['result']['sse'],
                model_min['sliding_window']['params'][self.param_key], model_min['sliding_window']['result']['sse'],
            )
            residuals_file.write(residuals_line)

            param_min = model_min['sliding_window']['params']
            model = self.model_cls(n, arrivals_per_period, param_min=param_min)
            model_min = model.train()

            with tempfile.NamedTemporaryFile(delete=False) as predictions_file:
                for item in model.predictions:
                    if int(item) == item:
                        item = int(item)
                    predictions_file.write('%s\n' % item)
            # print 'R predictions file: %s' % predictions_file.name

            # with tempfile.NamedTemporaryFile(delete=False) as arrivals_file:
            #     for item in model.arrivals:
            #         if int(item) == item:
            #             item = int(item)
            #         arrivals_file.write('%s\n' % item)
            # print 'arrivals file: %s' % arrivals_file.name

            predictor = self.predictor_cls(param_min)
            FeatureFlipper.set_n_step_predictor(predictor)
            result = self._simulate(predictions_file.name, inspection_time)

            # save results
            points_inspection_time.append(inspection_time)
            param_values.append(param_min)
            points_hour_99.append(result['wait_times_by_hour']['wait_times_99percentile'])
            points_day_99.append(result['wait_times_by_day']['wait_times_99percentile'])
            points_week_99.append(result['wait_times_by_week']['wait_times_99percentile'])
            points_mean.append(result['wait_times_batch_mean'])
            points_utilization.append(result['mean_utilization'])

            results_line = "%s, %s, %f, %f, %f, %f, %f, %f, %f" % (
                inspection_time,
                param_min[self.param_key],
                result['mean_utilization'],
                result['total_provisioned_time'],
                result['wait_times_by_hour']['wait_times_99percentile'],
                result['wait_times_by_day']['wait_times_99percentile'],
                result['wait_times_by_week']['wait_times_99percentile'],
                result['wait_times_year_99_percentile'],
                result['wait_times_batch_mean']
            )
            print results_line
            results_file.write(results_line + '\n')

        # inspection time plots
        point_labels = ['%s=%s' % (self.param_key, param_min[self.param_key]) for param_min in param_values]
        self.make_plot(points_inspection_time, points_hour_99, 'Inspection Time', 'Wait Time 99th Percentile per Hour', 'wait_time_hourly_99th', point_labels)
        self.make_plot(points_inspection_time, points_day_99, 'Inspection Time', 'Wait Time 99th Percentile per Day', 'wait_time_daily_99th', point_labels)
        self.make_plot(points_inspection_time, points_week_99, 'Inspection Time', 'Wait Time 99th Percentile per Week', 'wait_time_weekly_99th', point_labels)
        self.make_plot(points_inspection_time, points_mean, 'Inspection Time', 'Wait Time Mean', 'wait_time_mean', point_labels)
        self.make_plot(points_inspection_time, points_utilization, 'Inspection Time', 'Utilization Mean', 'utilization_mean', point_labels)

        # pareto plots
        point_labels = ['%s=%s, IT=%s' % (self.param_key, data[0][self.param_key], data[1]) for data in zip(param_values, points_inspection_time)]
        self.make_plot(points_hour_99, points_utilization, 'Wait Time 99th Percentile per Hour', 'Mean Utilization', 'pareto_wait_time_hourly_99th', point_labels)
        self.make_plot(points_day_99, points_utilization, 'Wait Time 99th Percentile per Day', 'Mean Utilization', 'pareto_wait_time_daily_99th', point_labels)
        self.make_plot(points_week_99, points_utilization, 'Wait Time 99th Percentile per Week', 'Mean Utilization', 'pareto_wait_time_weekly_99th', point_labels)
        self.make_plot(points_mean, points_utilization, 'Wait Time Mean', 'Mean Utilization', 'pareto_wait_time_mean', point_labels)

    def _simulate(self, predictions_filename, inspection_time):
            reserve = DataDrivenReservePolicySim()
            users_data_file_path = '/home/bmbouter/Documents/Research/vcl_simulation/data/2008_year_arrivals.txt'
            return reserve.run(predictions_filename, users_data_file_path, 2, inspection_time, three_hundred_second_startup_delay, 300)

    @classmethod
    def directory_path(self, filename):
        if self.model_specific_path is None:
            raise NotImplementedError
        dir_path = self.base_path + self.model_specific_path
        try:
            os.makedirs(dir_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        return dir_path + filename

    def make_plot(self, x, y, x_label, y_label, filename_snippet, point_labels=[]):
        filename = self.directory_path('%s.png' % filename_snippet)
        fig = plt.figure()
        ax = fig.add_subplot(111)
        plt.plot(x, y, 'ro')
        plt.title('%s vs %s' % (y_label, x_label))
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        for i, label in enumerate(point_labels):
            ax.annotate(label, xy=(x[i], y[i]))
        plt.savefig(filename)
        plt.close()


class MovingAverageResidualCalculator(AbstractResidualCalculator):

    model_specific_path = 'moving_average/'

    model_cls = MovingAverageModel
    predictor_cls = MovingAverageNStepPredictor
    param_key = 'k'


class ExponentialMovingAverageResidualCalculator(AbstractResidualCalculator):

    model_specific_path = 'exponential_moving_average/'

    model_cls = ExponentialMovingAverageModel
    predictor_cls = ExponentialMovingAverageNStepPredictor
    param_key = 'alpha'


class AutoregressiveResidualCalculator(AbstractResidualCalculator):

    model_specific_path = 'autoregressive/'

    model_cls = AutoregressiveModel
    predictor_cls = AutoregressiveNStepPredictor
    param_key = 'coeff'


class ReserveResidualCalculator(AbstractResidualCalculator):

    model_specific_path = 'reserve/'

    model_cls = ReserveModel
    predictor_cls = ReserveNStepPredictor
    param_key = 'R'


if __name__ == "__main__":
    if not os.path.isfile('n_step_models.py'):
        print 'Please run in the same directory as n_step_models.py'
        exit()

    #MovingAverageResidualCalculator().compute_residuals()
    #ExponentialMovingAverageResidualCalculator().compute_residuals()
    #AutoregressiveResidualCalculator().compute_residuals()
    #ReserveResidualCalculator().compute_residuals()
