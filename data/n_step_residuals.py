import math
import operator
import os
import tempfile

import matplotlib.pyplot as plt
import rpy2.robjects as robjects
from rpy2.rinterface import NARealType

from appsim.sim import DataDrivenReservePolicySim
from appsim.feature import FeatureFlipper
from appsim.n_step_predictors import MovingAverageNStepPredictor
from common import fixed_startup_delay
from data.day_cluster.segment_traffic_by_day import get_arrivals_per_time_period, get_clean_arrivals


three_hundred_second_startup_delay = fixed_startup_delay(300)


class AbstractModel(object):

    base_filepath = '/home/bmbouter/Documents/Research/vcl_simulation/data/n_step_images/'
    model_specific_path = None

    def __init__(self, arrivals):
        self.arrivals = arrivals

    def train(self):
        raise NotImplementedError

    @classmethod
    def directory_path(cls):
        if cls.model_specific_path is None:
            raise NotImplementedError
        return cls.base_filepath + cls.model_specific_path

    def clean(self, data):
        """
        Cleans the data passed in and returns it.

        :returns: a list of cleaned data
        :rtype: list
        """
        prediction_list = list(data)

        # shift the predictions by 1
        prediction_list.insert(0, 0)
        del prediction_list[-1]

        # replace NA values with 0
        for i, item in enumerate(prediction_list):
            if isinstance(item, NARealType):
                prediction_list[i] = 0
        return prediction_list

    def get_first_half_year_pred_and_arrivals(self):
        """
        Return the first half of arrival and cleaned predictions

        :returns: A tuple of (self.arrivals, self.cleaned_predictions) where each attribute is only the first half of
                  the data.
        :rtype: (list, list)
        """
        arrivals = self.arrivals[:(len(self.arrivals) / 2)]
        pred = self.cleaned_predictions[:(len(self.cleaned_predictions) / 2)]
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
        pred, arrivals = self.get_first_half_year_pred_and_arrivals()
        return self._compute_residual(pred, arrivals)

    def n_step_residual_non_overlapping(self):
        pred, arrivals = self.get_first_half_year_pred_and_arrivals()
        pred_summed = [0] * 52560
        arrivals_summed = [0] * 52560
        n = (len(arrivals) / 52560)
        index = -1
        for i in range(len(arrivals)):
            if i % n == 0:
                index = index + 1
            pred_summed[index] = pred_summed[index] + pred[i]
            arrivals_summed[index] = arrivals_summed[index] + arrivals[i]
        return self._compute_residual(pred_summed, arrivals_summed)

    def n_step_residual_sliding(self):
        pred, arrivals = self.get_first_half_year_pred_and_arrivals()
        pred_summed = []
        arrivals_summed = []
        n = (len(arrivals) / 52560)
        for i in range(len(arrivals)):
            arrivals_summed.append(sum(arrivals[i:n+i]))
            pred_summed.append(sum(pred[i:n+i]) / float(n))
        return self._compute_residual(pred_summed, arrivals_summed)


class MovingAverageModel(AbstractModel):

    model_specific_path = 'moving_average/'

    def train(self, k_range=range(1, 51)):
        self.residuals_simple = {}
        self.residuals_non_overlap = {}
        self.residuals_sliding_window = {}
        n = (len(self.arrivals) / 105120)
        inspection_time = 300 / n
        r = robjects.r
        r.library('TTR')
        SMA = r['SMA']
        int_vector_arrivals = robjects.IntVector(self.arrivals)
        for k in k_range:
            predictions = SMA(int_vector_arrivals, n=k)
            self.cleaned_predictions = self.clean(predictions)
            assert len(predictions)  == len(self.arrivals)
            assert len(self.cleaned_predictions) == len(self.arrivals)
            assert sum(self.arrivals) == 175481
            #print 'inspection_time=%s   n=%s, k=%s' % (inspection_time, n, k)
            residual_dict = self.simple_residual()
            #print 'simple:  over_sum=%(overestimate_sum)s,  under_sum=%(underestimate_sum)s,  sse=%(sse)s' % residual_dict
            self.residuals_simple[k] = residual_dict
            residual_dict = self.n_step_residual_non_overlapping()
            #print 'non_overlapping:  over_sum=%(overestimate_sum)s,  under_sum=%(underestimate_sum)s,  sse=%(sse)s' % residual_dict
            self.residuals_non_overlap[k] = residual_dict
            residual_dict = self.n_step_residual_sliding()
            #print 'sliding window:  over_sum=%(overestimate_sum)s,  under_sum=%(underestimate_sum)s,  sse=%(sse)s\n' % residual_dict
            self.residuals_sliding_window[k] = residual_dict
        points_x = []
        points_y = []
        min_k = None
        for k in self.residuals_simple.keys():
            points_x.append(k)
            points_y.append(self.residuals_simple[k]['sse'])
            if min_k == None:
                min_k = {'simple': k, 'non_overlap': k, 'sliding_window': k}
            else:
                if self.residuals_simple[k]['sse'] < self.residuals_simple[min_k['simple']]['sse']:
                    min_k['simple'] = k
                if self.residuals_non_overlap[k]['sse'] < self.residuals_non_overlap[min_k['non_overlap']]['sse']:
                    min_k['non_overlap'] = k
                if self.residuals_sliding_window[k]['sse'] < self.residuals_sliding_window[min_k['sliding_window']]['sse']:
                    min_k['sliding_window'] = k
        plt.plot(points_x, points_y, 'ro')
        plt.title('Moving Average Training Data SSE vs k, Inspection Time = %s' % inspection_time)
        plt.xlabel('k')
        plt.ylabel('SSE')
        if not os.path.isdir(self.directory_path()):
            os.mkdir(self.directory_path())
        filename = self.directory_path() + 'inspection_time_%s.png' % inspection_time
        plt.savefig(filename)
        plt.close()
        return min_k


def moving_average_reserve_capacity(inspection_time, reserve_capacity_data_file):
    reserve = DataDrivenReservePolicySim()
    users_data_file_path = '/home/bmbouter/Documents/Research/vcl_simulation/data/2008_year_arrivals.txt'
    return reserve.run(reserve_capacity_data_file, users_data_file_path, 2, inspection_time, three_hundred_second_startup_delay, 300)
    #reserve.run(reserve_capacity_data_file, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)


def compute_residuals(model_cls, boot_time=300):
    """
    Compute the residuals for multiple n-step prediction

    :param model_cls: The class to be used
    :type model_cls: subclass of AbstractModel
    :param boot_time: the number of second to measure residuals in the future against
    :type boot_time: int

    :return: residuals
    """
    points_inspection_time = []
    points_mean = []
    points_hour_99 = []
    points_day_99 = []
    points_week_99 = []
    points_utilization = []
    k_values = []

    print 'inspection_time,k,mean_utilization,total_provisioned_time,wait_time_by_hour_99,wait_time_by_day_99,wait_time_by_week_99,wait_times_year_99_percentile,wait_time_mean'

    # loop over boot_time / n where n = [1, 2, ... , boot_time]
    for inspection_time in range(boot_time, 9, -1):
        # for inspection_time in range(boot_time, 24, -1):
        # for inspection_time in [150]:
        n = boot_time / float(inspection_time)
        if int(n) != n:
            continue
        n = int(n)

        # count arrivals by size inspection_time
        clean_arrivals = get_clean_arrivals()
        arrivals_per_period = get_arrivals_per_time_period(clean_arrivals, inspection_time)

        # split list into two parts (training and test)
        #if len(arrivals_per_period) % 2 != 0:
        #    raise RuntimeError('Arrivals list cannot be split evenly!')
        #split_point = len(arrivals_per_period) / 2
        #arrivals_training = arrivals_per_period[:split_point]
        #arrivals_test = arrivals_per_period[split_point:]

        # train model for inspection_time
        # estimate arrivals based on trained model
        model = model_cls(arrivals_per_period)
        min_k = model.train()

        print 'inspection_time = %s,  %s' % (inspection_time, min_k)

        #k = 12
        k = min_k['simple']
        k = 12
        min_k = model.train(k_range=[k])

        with tempfile.NamedTemporaryFile(delete=False) as predictions_file:
            for item in model.cleaned_predictions:
                if int(item) == item:
                    item = int(item)
                predictions_file.write('%s\n' % item)
        # print 'predictions file: %s' % predictions_file.name

        # with tempfile.NamedTemporaryFile(delete=False) as arrivals_file:
        #     for item in model.arrivals:
        #         if int(item) == item:
        #             item = int(item)
        #         arrivals_file.write('%s\n' % item)
        # print 'arrivals file: %s' % arrivals_file.name

        predictor = MovingAverageNStepPredictor(k)
        FeatureFlipper.set_n_step_predictor(predictor)
        result = moving_average_reserve_capacity(inspection_time, predictions_file.name)

        # save results
        points_inspection_time.append(inspection_time)
        k_values.append(k)
        points_hour_99.append(result['wait_times_by_hour']['wait_times_99percentile'])
        points_day_99.append(result['wait_times_by_day']['wait_times_99percentile'])
        points_week_99.append(result['wait_times_by_week']['wait_times_99percentile'])
        points_mean.append(result['wait_times_batch_mean'])
        points_utilization.append(result['mean_utilization'])

        print "%s,%s,%f,%f,%f,%f,%f,%f,%f" % (
            inspection_time,
            k,
            result['mean_utilization'],
            result['total_provisioned_time'],
            result['wait_times_by_hour']['wait_times_99percentile'],
            result['wait_times_by_day']['wait_times_99percentile'],
            result['wait_times_by_week']['wait_times_99percentile'],
            result['wait_times_year_99_percentile'],
            result['wait_times_batch_mean']
        )

    # inspection time plots
    point_labels = ['k=%s' % k for k in k_values]
    make_plot(points_inspection_time, points_hour_99, 'Inspection Time', 'Wait Time 99th Percentile per Hour', 'wait_time_hourly_99th', point_labels)
    make_plot(points_inspection_time, points_day_99, 'Inspection Time', 'Wait Time 99th Percentile per Day', 'wait_time_daily_99th', point_labels)
    make_plot(points_inspection_time, points_week_99, 'Inspection Time', 'Wait Time 99th Percentile per Week', 'wait_time_weekly_99th', point_labels)
    make_plot(points_inspection_time, points_mean, 'Inspection Time', 'Wait Time Mean', 'wait_time_mean', point_labels)
    make_plot(points_inspection_time, points_utilization, 'Inspection Time', 'Utilization Mean', 'utilization_mean', point_labels)

    # pareto plots
    point_labels = ['k=%s, IT=%s' % data for data in zip(k_values, points_inspection_time)]
    make_plot(points_hour_99, points_utilization, 'Wait Time 99th Percentile per Hour', 'Mean Utilization', 'pareto_wait_time_hourly_99th', point_labels)
    make_plot(points_day_99, points_utilization, 'Wait Time 99th Percentile per Day', 'Mean Utilization', 'pareto_wait_time_daily_99th', point_labels)
    make_plot(points_week_99, points_utilization, 'Wait Time 99th Percentile per Week', 'Mean Utilization', 'pareto_wait_time_weekly_99th', point_labels)
    make_plot(points_mean, points_utilization, 'Wait Time Mean', 'Mean Utilization', 'pareto_wait_time_mean', point_labels)


def make_plot(x, y, x_label, y_label, filename, point_labels=[]):
    wait_time_filename = '/home/bmbouter/Documents/Research/vcl_simulation/data/n_step_images/waiting_times/%s.png'
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.plot(x, y, 'ro')
    plt.title('%s vs %s' % (y_label, x_label))
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    for i, label in enumerate(point_labels):
        ax.annotate(label, xy=(x[i], y[i]))
    plt.savefig(wait_time_filename % filename)
    plt.close()


if __name__ == "__main__":
    if not os.path.isfile('n_step_residuals.py'):
        print 'Please run in the same directory as n_step_residuals.py'
        exit()

    compute_residuals(MovingAverageModel)
