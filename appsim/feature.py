class FeatureFlipper(object):
    """
    A simple feature flipper object with static methods returning feature
    booleans.
    """

    n_step_predictor = None

    @classmethod
    def set_n_step_predictor(cls, n_step_predictor):
        cls.n_step_predictor = n_step_predictor

    @classmethod
    def get_n_step_predictor(cls):
        if cls.n_step_predictor is None:
            raise RuntimeError('Cannot do N-step prediction without a predictor being selected')
        return cls.n_step_predictor

    @staticmethod
    def departure_estimation_cls():
        from appsim.scaler.departure import (EmpiricalDeparture, GammaAlpha24Beta5Departure,
                                             GammaDeparture, NoDepartureEstimation)
        return NoDepartureEstimation
        #return GammaDeparture
        #return GammaAlpha24Beta5Departure
        #return EmpiricalDeparture

    @staticmethod
    def add_capacity_for_waiting_users():
        return True

    @staticmethod
    def loss_assumption():
        """If True customers who are blocked leave. If False, they wait in a FIFO queue."""
        return False
