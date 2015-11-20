class MovingAverageNStepPredictor(object):

    def __init__(self, k):
        self.k = k
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
