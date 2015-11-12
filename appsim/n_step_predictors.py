class MovingAverageNStepPredictor(object):

    def __init__(self, k):
        self.k = k
        self.data = []

    def predict_n_steps(self, last_arrival_count, n):
        if len(self.data) > n:
            self.data.pop(0)
        copied_data = self.data[:]
        self.data.append(last_arrival_count)
        for i in range(n):
            data_to_add = copied_data[-n:]
            if len(data_to_add) == 0:
                return 0
            the_sum = sum(data_to_add) / float(len(data_to_add))
            copied_data.append(the_sum)
        n_step_list = copied_data[-n:]
        total_sum = sum(n_step_list)
        return total_sum
