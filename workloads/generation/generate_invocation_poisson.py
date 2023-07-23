from re import I
from interarrivals import *
# from requests import *
import pandas as pd

def generate_invocation_csv(timestamp,dag_name,num_invocations, filename):
    df = pd.DataFrame(zip(timestamp,dag_name,num_invocations), columns=['timestamp','dag_name','num_invocations'])
    # * Make all workloads start from 0 (for fair starts of all harvestvms).
    df['timestamp'] -= df.timestamp.min()
    df.to_csv("workloads/invocation/{}.csv".format(filename), index=False)

def generate_poisson_list(mean_exp, load, length):
    gen_exp = PoissonArrivalGenerator(mean_exp * 1.0 / load)
    # Generate arrival patterns for exponential
    arrival_exp = [int(gen_exp.__next__())]
    for i in range(length):
        if i==0:
            pass
        else:
            arrival_time = int(gen_exp.__next__() + arrival_exp[i-1])
            # print(X_bi)
            # Make it integer!
            arrival_exp.append(int(arrival_time))
    return arrival_exp
            
def main():
    import argparse
    global basic_serial_header
    global single_serial
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('-m', '--mean', dest='mean', action='store',
                      help='Mean for Poisson Arrival (in ms)', default=1000)
    parser.add_argument('-n', '--total_requests', dest='total_requests', action='store',
                      help='Total number of requests', default=512)
    parser.add_argument('-i', '--invoc', dest='invocations', action='store',
                      help='Number of invocations per requests', default=1)
    parser.add_argument('-s', '--stages', dest='stages', action='store',
                      help='Number of stages per DAG', default=4)                      
    parser.add_argument('-j', '--filename', dest='json_file', action='store',
                      help='Name of the json file for the output', default='test_s2_m170_t1000')
    parser.add_argument('-l', '--load', dest='load', action='store',
                      help='Multiplicative load', default=1)
                                                
    opts = parser.parse_args()
    total_num_requests = int(opts.total_requests)
    total_num_invocations = total_num_requests * int(opts.invocations)
    # num_stages = int(opts.stages)
    # assert not total_num_invocations % num_stages, f"{total_num_invocations=} not divisable by {num_stages=}"

    # num_inital_invocations = total_num_invocations

    num_stages = int(opts.stages)
    # assert not total_num_invocations % num_stages, f"{total_num_invocations=} not divisable by {num_stages=}"

    # num_inital_invocations = total_num_invocations // int(opts.stages)
    loads = int(opts.load)

    # num_inital_invocations = total_num_invocations // int(opts.stages)
    num_inital_invocations = total_num_invocations
    # Making the initial invocation a constant, since this would make the calculation for failure rate fairer. 
    # If we are changing the number of initial invocation but fixing the total invocations, longer chain would have fewer invocations and is more susceptible to randomness.
    # Downside: having longer chain would mean that we have a linear total number of invocations, which translates to potentially higher resource demand         
    # timestamp_list = generate_poisson_list(opts.mean / 3.0 * (num_stages + 2), 1, num_inital_invocations)
    timestamp_list = generate_poisson_list(opts.mean / 3.0 * (num_stages + 2), loads, num_inital_invocations)

    opts.json_file = 'test_s{}_m170_t1000'.format(opts.stages)
    
    # generate_invocation_csv(timestamp_list, [opts.json_file]*num_inital_invocations, 
    #     [int(opts.invocations)]*num_inital_invocations, 
    #     'test_harvest_json{}_invoke{}_poisson{}'.format(opts.json_file, str(total_num_requests), opts.mean))
    
    generate_invocation_csv(timestamp_list, [opts.json_file]*num_inital_invocations, 
        [int(opts.invocations)]*num_inital_invocations, 
        'test_harvest_json{}_invoke{}_poisson{}_load{}'.format(opts.json_file, str(total_num_requests), opts.mean, loads))
    # opts.output += '_s{}_m{}_t{}.json'.format(opts.nstages, opts.mem, opts.time)
    # f = open(opts.output, "w")
    # f.write(output)
    # f.close()


if __name__ == "__main__":
    main()