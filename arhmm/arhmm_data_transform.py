import os


def input_data():
    input_filename = '../data/2008_five_minute_counts.csv'
    data = []
    with open(input_filename, 'r') as f:
        for line in f:
            data.append(int(line.strip()))
    return data


def write_data(datum):
    output_filename = '2008_five_minute_counts_for_scilab.csv'
    with open(output_filename, 'w') as f:
        f.write('dates,ardata\n')
        for i, data in enumerate(datum):
            f.write('%s,%s\n' % (i, data))


def run():
    datum = input_data()
    write_data(datum)

if __name__ == "__main__":
    if not os.path.isfile('arhmm_data_transform.py'):
        print 'Please run in the same directory as reserve_capacity.py'
        exit()
    run()
