import operator


class MovingAverageNStepPredictor(object):

    def __init__(self, param_dict):
        self.k = param_dict['k']
        self.data = []

    def predict_n_steps(self, last_arrival_count, n):
        if last_arrival_count is not None:
            self.data.append(last_arrival_count)
        copied_data = self.data[:]
        if len(self.data) > self.k:
            self.data.pop(0)
            assert len(self.data) == self.k
        for i in range(n):
            data_to_add = copied_data[-self.k:]
            if len(data_to_add) < self.k:
                return 0
            the_sum = sum(data_to_add) / float(len(data_to_add))
            copied_data.append(the_sum)
        n_step_list = copied_data[-n:]
        total_sum = sum(n_step_list)
        return total_sum


class ExponentialMovingAverageNStepPredictor(object):

    def __init__(self, param_dict):
        self.alpha = param_dict['alpha']
        self.last_prediction = None

    def predict_n_steps(self, last_arrival_count, n):
        if last_arrival_count is None:
            return 0
        if self.last_prediction is None:
            self.last_prediction = last_arrival_count
            return last_arrival_count
        data = [last_arrival_count]
        for i in range(n):
            predicted_value = data[-1] * self.alpha + self.last_prediction * (1 - self.alpha)
            data.append(predicted_value)
            self.last_prediction = predicted_value
        assert len(data) == n + 1
        self.last_prediction = data[1]
        n_step_list = data[-n:]
        total_sum = sum(n_step_list)
        return total_sum


class AutoregressiveNStepPredictor(object):

    def __init__(self, param_dict):
        self.coefficients = param_dict['coeff']
        self.data = []

    def predict_n_steps(self, last_arrival_count, n):
        if last_arrival_count is not None:
            self.data.append(last_arrival_count)
        copied_data = self.data[:]
        if len(self.data) > len(self.coefficients):
            self.data.pop(0)
            assert len(self.data) == len(self.coefficients)
        for i in range(n):
            data_to_autoregress = copied_data[-len(self.coefficients):]
            if len(data_to_autoregress) < len(self.coefficients):
                return 0
            next_prediction = sum(map(operator.mul, data_to_autoregress, self.coefficients))
            copied_data.append(next_prediction)
        n_step_list = copied_data[-n:]
        total_sum = sum(n_step_list)
        return total_sum


class ReserveNStepPredictor(object):

    def __init__(self, param_dict):
        self.R = param_dict['R']

    def predict_n_steps(self, last_arrival_count, n):
        return self.R * n
