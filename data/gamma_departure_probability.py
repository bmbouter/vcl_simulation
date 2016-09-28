import re
from subprocess import check_output


def generate_table():
    P_slots = []
    for slot_start in range(0, 28800, 300):
        slot_end = slot_start + 300
        arg_string = 'A=%f;B=%f' % (slot_start, slot_end)
        raw_output = check_output(["../appsim/scaler/gamma_alpha_twenty_four_beta_five_mathematica.sh", arg_string])
        stripped_output = raw_output.strip()
        if '`' in stripped_output:
            stripped_output = re.sub(r"`[\d\.]+", '', stripped_output)
        if '*^' in stripped_output:
            stripped_output = stripped_output.replace('*^', 'e')
        P_depart = float(stripped_output)
        P_slots.append(P_depart)
    print P_slots


if __name__ == "__main__":
    generate_table()
