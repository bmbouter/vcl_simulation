
def print_results_header(param_name):
    print_wait_time_header(param_name)
    return
    print "%s,bp_batch_mean,bp_batch_mean_delta,bp_batch_mean_percent_error,mean_utilization,bp_by_hour_50,bp_by_hour_95,bp_by_hour_99,bp_by_hour_mean,bp_by_day_50,bp_by_day_95,bp_by_day_99,bp_by_day_mean,bp_by_week_50,bp_by_week_95,bp_by_week_99,bp_by_week_mean,bp_by_month_50,bp_by_month_95,bp_by_month_99,bp_by_month_mean,bp_by_year_50,bp_by_year_95,bp_by_year_99,bp_by_year_mean,util_by_hour_50,util_by_hour_95,util_by_hour_99,util_by_hour_mean,util_by_day_50,util_by_day_95,util_by_day_99,util_by_day_mean,util_by_week_50,util_by_week_95,util_by_week_99,util_by_week_mean,util_by_month_50,util_by_month_95,util_by_month_99,util_by_month_mean,util_by_year_50,util_by_year_95,util_by_year_99,util_by_year_mean,num_servers,ns_delta" % param_name

def print_all_results(results):
    print_wait_time(results)
    return
    r = results
    print("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (r['param'], r['bp_batch_mean'], r['bp_batch_mean_delta'], r['bp_batch_mean_percent_error'], r['mean_utilization'], r['bp_by_hour']['bp_50percentile'], r['bp_by_hour']['bp_95percentile'], r['bp_by_hour']['bp_99percentile'], r['bp_by_hour']['bp_mean'], r['bp_by_day']['bp_50percentile'], r['bp_by_day']['bp_95percentile'], r['bp_by_day']['bp_99percentile'], r['bp_by_day']['bp_mean'], r['bp_by_week']['bp_50percentile'], r['bp_by_week']['bp_95percentile'], r['bp_by_week']['bp_99percentile'], r['bp_by_week']['bp_mean'], r['bp_by_month']['bp_50percentile'], r['bp_by_month']['bp_95percentile'], r['bp_by_month']['bp_99percentile'], r['bp_by_month']['bp_mean'], r['bp_by_year']['bp_50percentile'], r['bp_by_year']['bp_95percentile'], r['bp_by_year']['bp_99percentile'], r['bp_by_year']['bp_mean'], r['util_by_hour']['util_50percentile'], r['util_by_hour']['util_95percentile'], r['util_by_hour']['util_99percentile'], r['util_by_hour']['util_mean'], r['util_by_day']['util_50percentile'], r['util_by_day']['util_95percentile'], r['util_by_day']['util_99percentile'], r['util_by_day']['util_mean'], r['util_by_week']['util_50percentile'], r['util_by_week']['util_95percentile'], r['util_by_week']['util_99percentile'], r['util_by_week']['util_mean'], r['util_by_month']['util_50percentile'], r['util_by_month']['util_95percentile'], r['util_by_month']['util_99percentile'], r['util_by_month']['util_mean'], r['util_by_year']['util_50percentile'], r['util_by_year']['util_95percentile'], r['util_by_year']['util_99percentile'], r['util_by_year']['util_mean'], r['num_servers'], r['ns_delta']))

def print_simple_results(results, timescale='yearly_mean'):
    print_wait_time(results)
    return
    if timescale == 'weekly_99_percentile':
        bp_result = results['bp_by_week']['bp_99percentile']
    else:
        bp_result = results['bp_batch_mean']
    if 'density' in results:
        print('%s,%s,%.4f,%.4f,%.4f' % (results['param'], results['density'], bp_result, results['bp_batch_mean_delta'], results['mean_utilization']))
    else:
        print('%s,%.4f,%.4f,%.4f' % (results['param'], bp_result, results['bp_batch_mean_delta'], results['mean_utilization']))

def print_wait_time_header(param_name):
    print "%s,mean_utilization,total_provisioned_time,wait_time_by_hour_99,wait_time_by_day_99,wait_time_by_week_99,wait_times_year_99_percentile,wait_time_mean" % param_name

def print_wait_time(results):
    print('%s,%.4f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f' % (results['param'], results['mean_utilization'], results['total_provisioned_time'], results['wait_times_by_hour']['wait_times_99percentile'], results['wait_times_by_day']['wait_times_99percentile'], results['wait_times_by_week']['wait_times_99percentile'], results['wait_times_year_99_percentile'], results['wait_times_batch_mean']))
