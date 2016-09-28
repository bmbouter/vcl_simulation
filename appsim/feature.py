class PredictorNotSetException(Exception):
    pass


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
            raise PredictorNotSetException('Cannot do N-step prediction without a predictor being selected')
        return cls.n_step_predictor

    @staticmethod
    def departure_estimation_cls():
        from appsim.scaler.departure import (EmpiricalDeparture, Fixed120Departure,
                                             Fixed200Departure, Fixed500Departure,
                                             GammaAlpha24Beta5Departure,
                                             GammaAlpha416_66Beta1_2Departure,
                                             GammaAlpha66_66Beta3Departure,
                                             GammaDeparture,
                                             NoDepartureEstimation)
        return NoDepartureEstimation
        # return GammaDeparture
        # return GammaAlpha24Beta5Departure
        # return EmpiricalDeparture
        # return Fixed120Departure
        # return Fixed200Departure
        # return Fixed500Departure
        # return GammaAlpha66_66Beta3Departure
        # return GammaAlpha416_66Beta1_2Departure

    @staticmethod
    def add_capacity_for_waiting_users():
        return True

    @staticmethod
    def loss_assumption():
        """If True customers who are blocked leave. If False, they wait in a FIFO queue."""
        return False
