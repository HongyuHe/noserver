# Sample Usage:
'''
# Generate a serial dag with 10 stages
python generate_serial_dag_config.py -s 10
# Dump it into invocation/test_harvest_s10_invoke10_t100_poisson.csv
python generate_invocation_poisson.py 
'''

# TODO:
- Enable a better way to sample from the given trace distribution of execution time
- Check what's a reasonable interarrival mean 
