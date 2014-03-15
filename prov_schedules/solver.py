class VMReservation(object):
    def __init__(self, start_index, end_index, density):
        self.start_index = start_index
        self.end_index = end_index
        self.density = density

    def __str__(self):
        return 'VMReservation(start_index=%s, end_index=%s, density=%s)' % (self.start_index, self.end_index, self.density)

    def get_time_in_seconds(self):
        start_time = self.start_index * 300
        stop_time = (self.end_index + 1) * 300
        return (start_time, stop_time, )

class NoMinimumScheduleSolverMultipleD(object):
    def solve(self, capacity_curve, density):
        self.vm_schedule = []
        self.required_capacity = capacity_curve[:]
        while max(self.required_capacity) > density:
            self._find_and_remove_consecutive_density_in_capacity_cover_curve(density)
        if max(self.required_capacity) > 0:
            self._find_and_remove_consecutive_density_in_capacity_under_curve(density)
        return self.vm_schedule

    def _find_and_remove_consecutive_density_in_capacity_cover_curve(self, density):
        block_started = False
        block_start_index = None
        block_end_index = None
        found_any_blocks = False
        for index in range(len(self.required_capacity)):
            if self.required_capacity[index] > 0 and block_started:
                self.required_capacity[index] = self.required_capacity[index] - density
                block_end_index = index
            elif self.required_capacity[index] > 0 and not block_started:
                self.required_capacity[index] = self.required_capacity[index] - density
                found_any_blocks = True
                block_started = True
                block_start_index = index
                block_end_index = index
            elif self.required_capacity[index] <= 0 and block_started:
                block_started = False
                self.vm_schedule.append(VMReservation(block_start_index, block_end_index, density))
        if block_started:
            self.vm_schedule.append(VMReservation(block_start_index, block_end_index, density))
        return found_any_blocks

    def _find_and_remove_consecutive_density_in_capacity_under_curve(self, density):
        block_started = False
        block_start_index = None
        block_end_index = None
        found_any_blocks = False
        for index in range(len(self.required_capacity)):
            if self.required_capacity[index] >= density and block_started:
                self.required_capacity[index] = self.required_capacity[index] - density
                block_end_index = index
            elif self.required_capacity[index] >= density and not block_started:
                self.required_capacity[index] = self.required_capacity[index] - density
                found_any_blocks = True
                block_started = True
                block_start_index = index
                block_end_index = index
            elif not self.required_capacity[index] >= density and block_started:
                block_started = False
                self.vm_schedule.append(VMReservation(block_start_index, block_end_index, density))
        if block_started:
            self.vm_schedule.append(VMReservation(block_start_index, block_end_index, density))
        return found_any_blocks
