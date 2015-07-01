from appsim.scaler.departure import (EmpiricalDeparture, GammaDeparture,
                                     NoDepartureEstimation)


class FeatureFlipper(object):
    """
    A simple feature flipper object with static methods returning feature
    booleans.
    """

    @staticmethod
    def departure_estimation_cls():
        return NoDepartureEstimation
        #return GammaDeparture
        #return EmpiricalDeparture

    @staticmethod
    def add_capacity_for_waiting_users():
        return True
