import re
from subprocess import check_output

from appsim.user import active_users


gamma_P_slots = [0.045049231818548634, 0.05066505639066426,
                 0.05072477399184465, 0.04949755864427831,
                 0.047713578767771984, 0.04566400428020913,
                 0.043495077745995696, 0.041289340561019836,
                 0.03909651074129541, 0.036947580467831684,
                 0.03486208616336873, 0.032852199935571626,
                 0.03092518824911926, 0.029084966287112417,
                 0.02733312069235078, 0.025669604311499644,
                 0.02409322034698482, 0.022601966722532934,
                 0.021193285023312577, 0.01986424272654744,
                 0.01861166784416786, 0.01743224902808574,
                 0.016322610239744108, 0.015279366453420477,
                 0.01429916506964607, 0.013378716469502344,
                 0.012514816259974082, 0.01170436112807305,
                 0.010944359760639462, 0.010231939946608063,
                 0.009564352724517939, 0.00893897424631365,
                 0.008353305882360555, 0.007804972980249068,
                 0.007291722602903653, 0.006811420503564533,
                 0.0063620475418428215, 0.005941695702892934,
                 0.0055485638482790966, 0.005180953300426871,
                 0.0048372633411917125, 0.004515986687927317,
                 0.004215704996631841, 0.003935084430616046,
                 0.0036728713241456433, 0.0034278879632431117,
                 0.0031990284999621954, 0.0029852550117065424,
                 0.002785593713341501, 0.002599131326775649,
                 0.0024250116102272727, 0.002262432047431205,
                 0.0021106406954906525, 0.001968933188864327,
                 0.0018366498960398895, 0.0017131732247306552,
                 0.0015979250709034303, 0.0014903644065666429,
                 0.0013899850009923482, 0.0012963132698898892,
                 0.0012089062469741224, 0.0011273496723610583,
                 0.0010512561922659056, 0.0009802636645615012,
                 0.000914033564870379, 0.0008522494880036548,
                 0.0007946157397183289, 0.000740856013936644,
                 0.0006907121507523758, 0.0006439429707361686,
                 0.0006003231812424103, 0.0005596423506113042,
                 0.0005217039463500837, 0.0004863244335651504,
                 0.000453332430101176, 0.00042256791502301287,
                 0.0003938814872508809, 0.0003671336713282938,
                 0.0003421942674650922, 0.00031894174315463505,
                 0.00029726266381443345, 0.00027705116004325567,
                 0.0002582084292250433, 0.0002406422693407945,
                 0.00022426664297415237, 0.00020900126961475985,
                 0.00019477124447579003, 0.00018150668214854,
                 0.00016914238351781945, 0.00015761752445726828,
                 0.0001468753649138945, 0.00013686297707630172,
                 0.00012753099140142812, 0.00011883335935040933,
                 0.00011072713175561128, 0.00010317225180814708]


gamma_synthetic_P_slots = [0.9999999599056155, 4.544428027505281e-08,
                           2.626778814530667e-27, 2.4104744371510626e-49,
                           1.5272980954811043e-72, 2.2230864600878693e-96,
                           1.2736883902207908e-120, 3.831447164455917e-145,
                           7.189139965749505e-170, 9.404323586092787e-195,
                           9.254200297810214e-220, 7.232395623883432e-245,
                           4.672734569037646e-270, 2.572976120842042e-295,
                           1.2366e-320, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                           0.0, 0.0, 0.0, 0.0]


empirical_P_slots = [0.04574924074353286, 0.030564111400693563,
                     0.03293342236198764, 0.06907618411700088,
                     0.05677730630882891, 0.061634393779481766,
                     0.056454218450470633, 0.04332608180584574,
                     0.035647360372197213, 0.0317487668813406,
                     0.030833351282658798, 0.03116720873629569,
                     0.05175867490899692, 0.044499967691214164,
                     0.03784435780903354, 0.014452796863893854,
                     0.014032782648028087, 0.014054321838585305,
                     0.014097400219699744, 0.013128136644624895,
                     0.012363495379843626, 0.011760398044241498,
                     0.011760398044241498, 0.013149675835182114,
                     0.022766924418980334, 0.0182760031878002,
                     0.015314364486182609, 0.005492493592090809,
                     0.005363258448747496, 0.0049217050423245095,
                     0.005610959140155513, 0.005029400995110604,
                     0.004900165851767291, 0.004630925969802055,
                     0.0042755293256079435, 0.00396321106252827,
                     0.005190944924289745, 0.0047924698989811965,
                     0.0043616860878368195, 0.003489348870269455,
                     0.0033708833222047515, 0.003080104249682297,
                     0.0030908738449609064, 0.0030908738449609064,
                     0.003166261011911172, 0.0032631873694186574,
                     0.003920132681413832, 0.005858659831563529,
                     0.019923751265427444, 0.016682103086566008,
                     0.01568053072565533, 0.0008938764081245827,
                     0.0006138669308807375, 0.00048463178753742434,
                     0.0008077196458957073, 0.0006461757167165658,
                     0.0007000236931096129, 0.0005815581450449092,
                     0.0006461757167165658, 0.0007000236931096129,
                     0.0018523703879208219, 0.0014862041484481013,
                     0.0016262088870700239, 0.0005492493592090809,
                     0.0003661662394727206, 0.0004307838111443772,
                     0.0008938764081245827, 0.0009692635750748487,
                     0.0008077196458957073, 0.0010661899325823336,
                     0.001055420337303724, 0.0010123419561892863,
                     0.00045232300170159603, 0.0001400047386219226,
                     0.00023693109612940745, 9.692635750748487e-05,
                     7.5387166950266e-05, 0.0001076959527860943,
                     6.461757167165658e-05, 6.461757167165658e-05,
                     6.461757167165658e-05, 3.230878583582829e-05,
                     6.461757167165658e-05, 0.0001830831197363603,
                     0.0002907790725224546, 0.0002477006914080169,
                     0.00019385271501496973, 0.0001076959527860943,
                     0.0001076959527860943, 4.307838111443772e-05,
                     0.0001076959527860943, 6.461757167165658e-05,
                     7.5387166950266e-05, 9.692635750748487e-05,
                     0.00020462231029357917, 0.00034462704891550177]


class GammaDeparture(object):
    """
    A class for estimating the departure process as a Gamma process
    """

    @staticmethod
    def estimate(sim):
        departure_probability_accumulator = 0
        for user_id, absolute_start_time in active_users.iteritems():
            time_in_system = sim.now() - absolute_start_time
            B = time_in_system + 300
            arg_string = 'A=%f;B=%f' % (time_in_system, B)
            raw_output = check_output(["./gamma_mathematica.sh", arg_string])
            P_depart = float(raw_output.strip())
            departure_probability_accumulator += P_depart
        return departure_probability_accumulator

    @staticmethod
    def slotted_estimate(sim):
        departure_probability_accumulator = 0
        for user_id, absolute_start_time in active_users.iteritems():
            time_in_system = sim.now() - absolute_start_time
            time_in_system = int(time_in_system)
            slot_index = time_in_system / 300
            P_departure = gamma_P_slots[slot_index]
            departure_probability_accumulator += P_departure
        return departure_probability_accumulator

    @staticmethod
    def generate_table():
        P_slots = []
        for slot_start in range(0, 28800, 300):
            slot_end = slot_start + 300
            arg_string = 'A=%f;B=%f' % (slot_start, slot_end)
            raw_output = check_output(["./gamma_mathematica.sh", arg_string])
            P_depart = float(raw_output.strip())
            P_slots.append(P_depart)
        print P_slots


class GammaAlpha24Beta5Departure(object):
    """
    A class for estimating the departure process as a Gamma process
    """

    @staticmethod
    def slotted_estimate(sim):
        departure_probability_accumulator = 0
        for user_id, absolute_start_time in active_users.iteritems():
            time_in_system = sim.now() - absolute_start_time
            time_in_system = int(time_in_system)
            slot_index = time_in_system / 300
            P_departure = gamma_synthetic_P_slots[slot_index]
            departure_probability_accumulator += P_departure
        return departure_probability_accumulator

    @staticmethod
    def generate_table():
        P_slots = []
        for slot_start in range(0, 28800, 300):
            slot_end = slot_start + 300
            arg_string = 'A=%f;B=%f' % (slot_start, slot_end)
            raw_output = check_output(["./gamma_alpha_twenty_four_beta_five_mathematica.sh", arg_string])
            stripped_output = raw_output.strip()
            if '`' in stripped_output:
                stripped_output = re.sub(r"`[\d\.]+", '', stripped_output)
            if '*^' in stripped_output:
                stripped_output = stripped_output.replace('*^', 'e')
            P_depart = float(stripped_output)
            P_slots.append(P_depart)
        print P_slots


class EmpiricalDeparture(object):
    """
    A class for estimating the departure process empirically
    """

    @staticmethod
    def generate_table():
        f = open('../../data/2008_first_half_year_service_times.txt')
        service_times = []
        for line in f:
            service_times.append(int(line.strip()))

        total_count = len(service_times)

        P_slots = []
        for slot_start in range(0, 28800, 300):
            slot_end = slot_start + 300
            bucket_count = len(filter(lambda x: True if x >= slot_start and x < slot_end else False, service_times))
            P_depart = bucket_count / float(total_count)
            P_slots.append(P_depart)
        print P_slots

    @staticmethod
    def slotted_estimate(sim):
        departure_probability_accumulator = 0
        for user_id, absolute_start_time in active_users.iteritems():
            time_in_system = sim.now() - absolute_start_time
            time_in_system = int(time_in_system)
            slot_index = time_in_system / 300
            P_departure = empirical_P_slots[slot_index]
            departure_probability_accumulator += P_departure
        return departure_probability_accumulator


class NoDepartureEstimation(object):
    """
    A class that estimates no customers will depart
    """

    @staticmethod
    def slotted_estimate(sim):
        return 0.0