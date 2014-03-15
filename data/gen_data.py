from random import expovariate

lamda = 5
mu = 2

f = open('./fixed_lambda_mu_user_schedule.csv', 'w')
for i in range(1000000):
    f.write('%s,%s\n' % (expovariate(lamda), expovariate(mu)))
