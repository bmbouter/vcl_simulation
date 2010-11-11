from scipy import integrate
from numpy import linspace, zeros
#import pylab as p

from scaler import Scale

def dP_dt(p, t, lam, mu):
    """Returns the set of ordinary differential equations for M/M/C Queues
    describing the current size in population probability of the M/M/C
    birth-death process

    parameters:
    p -- a list of probabilities.  Each index represents the probability the
        system will have index customers in service.  For example, and initial
        state of p = [1, 0, 0] would be a system size of C=2, with a
        probability there are 0 customers in the system.
    t -- the current eq time.  This variable is required by the odeint solver
    lam -- the Poisson arrival process parameter
    mu -- the Poisson service time process parameter

    """
    size = len(p)
    if size == 1:
        return [0]
    elif size == 2:
        return [(mu * p[1]) - (lam * p[0]), (lam * p[0]) - (mu * p[1])]
    else:
        X = zeros(size)
        for i in range(1,size-1):
            X[i] =  (lam * p[i-1]) + ((i+1) * mu * p[i+1]) - (lam * p[i]) - (i * mu * p[i])
        X[0] = (mu * p[1]) - (lam * p[0])
        X[size - 1] = ((lam * p[size - 2]) - ((size-1) * mu * p[size - 1]))
        return X

class OdePolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler policy attempts to provision based on blocking probability
    predicted from a series of birth death differential equations for an
    M/M/C queue.  A threshold is used to request and release server
    resources from the cluster.  A delta value is also used to determine
    how far in the future to evaluate the blocking probability.

    """

    def __init__(self, sim, scale_rate, startup_delay,
            shutdown_delay, delta, worst_bp, lamda, mu):
        """Initializes an OdePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        delta -- the number of the seconds in the future the scaler should
            manage the blocking probability
        worst_bp -- ensure the blocking probability does exceed this value
            during the time interval t = [now, now + delta]
        lamda -- the parameter to a Poisson distribution (in seconds)
            which defines the arrival process
        mu -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process

        """

        self.delta = delta
        self.worst_bp = worst_bp
        self.lamda = lamda
        self.mu = mu
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay=startup_delay,
                             shutdown_delay=shutdown_delay)

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        This policy scales the cluster such that the blocking
        probability is managed.  The scaler iterativly solves the set M/M/C 
        ordinary differential equations for different sizes of C until
        blocking probability of the system size C is less than worst_bp
        along the time interval t=[now, now + delta].

        """
        
        #raw_input('scaler logic running with:\ncluster.capacity = %s\ncluster.used = %s' % (self.sim.cluster.capacity, self.sim.cluster.capacity - self.sim.cluster.n))
        #print('scaler logic running with:\ncluster.capacity = %s\ncluster.used = %s' % (self.sim.cluster.capacity, self.sim.cluster.capacity - self.sim.cluster.n))
        done = False
        current_C = self.sim.cluster.capacity
        found_valid_cap = False
        iterations = 0
        while done is False:
            print 'iterations = %s, time = %s, cluster_capacity_now = %s' % (iterations, self.sim.now(), self.sim.cluster.capacity)
            iterations = iterations + 1
            t = linspace(0, self.delta, self.delta * 10)
            initial_conditions = self.get_initial_conditions(current_C)
            X, infodict = integrate.odeint(dP_dt, initial_conditions, t, args=(self.lamda, self.mu), full_output=True)
            max_observed_bp = max(X.T[-1])
            ###
            #styles = ['b-' ,'g-', 'r-', 'm-', 'k-', 'b--' ,'g--', 'r--', 'm--', 'k--', 'b-.' ,'g-.', 'r-.', 'm-.', 'k-.', 'b:' ,'g:', 'r:', 'm:', 'k:']
            #f1 = p.figure()
            #for i, v in enumerate(X.T):
            #    p.plot(t, v, styles[i], label='p%s'%i)
            ###  single blocking probability only  -> p.plot(t, X.T[-1], styles[2], label='p_block')
            #p.grid()
            #p.legend(loc='best')
            #p.xlabel('time')
            #p.ylabel('probability')
            #p.title('Forward differential equations with N=%s' % current_C)
            #f1.savefig('ode_policy.png')
            ###
            if found_valid_cap is True:
                #raw_input('looking for the crossover point.  capacity = %s' % current_C)
                #print('looking for the crossover point.  capacity = %s' % current_C)
                if max_observed_bp > self.worst_bp:
                    perfect_cap = current_C + self.sim.cluster.density
                    #raw_input('FOUND cap = %s' % perfect_cap)
                    #print('FOUND cap = %s' % perfect_cap)
                    done = True
                else:
                    current_C = current_C - self.sim.cluster.density
            else:
                #raw_input('still looking for a valid_cap point.  capacity = %s' % current_C)
                #print('still looking for a valid_cap point.  capacity = %s' % current_C)
                if max_observed_bp <= self.worst_bp:
                    current_C = current_C - self.sim.cluster.density
                    found_valid_cap = True
                else:
                    current_C = current_C + self.sim.cluster.density
        if perfect_cap > self.sim.cluster.capacity:
            overprov_by = perfect_cap - self.sim.cluster.capacity
            servers_to_start = overprov_by / self.sim.cluster.density
            servers_to_stop = 0
        elif perfect_cap < self.sim.cluster.capacity:
            underprov_by = self.sim.cluster.capacity - perfect_cap
            servers_to_start = 0
            servers_to_stop = underprov_by / self.sim.cluster.density
        else:
            servers_to_start = 0
            servers_to_stop = 0
        #raw_input('returning start= %s, stop = %s' % (servers_to_start, servers_to_stop))
        #if servers_to_start != 0 or servers_to_stop != 0:
        #    import time
        #    print('returning start = %s, stop = %s' % (servers_to_start, servers_to_stop))
        #    time.sleep(20)
        #print('returning start = %s, stop = %s' % (servers_to_start, servers_to_stop))
        return servers_to_start, servers_to_stop


    def get_initial_conditions(self, capacity):
        """Return an array for the current system customers and capacity

        Parameters:
        capacity -- the maximum size (in customers) of the system

        """
        #print 'capacity = %s' % capacity
        #print 'self.sim.cluster.capacity = %s' % self.sim.cluster.capacity
        #print 'self.sim.cluster.n = %s' % self.sim.cluster.n
        #print 'self.lamda = %s' % self.lamda
        #print 'self.mu = %s' % self.mu 
        p0 = zeros(capacity + 1)
        p0[min(self.sim.cluster.capacity - self.sim.cluster.n, capacity)] = 1
        return p0
