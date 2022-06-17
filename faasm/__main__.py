"""Console script."""
from pprint import pprint
import pandas as pd
import argparse
import sys
import numpy as np
import copy
import json
from pprint import pprint

from __init__ import __version__
from model.cluster import *
from simulation import *

def main():
    rps = 1
    runtime_milli = 1e3 # 1s
    memory_mib = 1000
    iat_milli = int(1e3 / rps)
    duration_minute = 1
    num_invocations = int(np.ceil(duration_minute * 60 * 1e3 / iat_milli))

    num_workers = 2
    num_functions = 2

    clock = Clock()
    nodes = [
        Node(name=f"worker-{i}", num_cores=16, memory_mib=64 * 2**10) 
        for i in range(num_workers)
    ]
    functions = [Function(name=f"func-{i}") for i in range(num_functions)]

    cluster = Cluster(clock, nodes, functions)

    inv = 0
    prev_arrival, next_arrival = 0, 0 
    for t in range(duration_minute * 60 * 1000 + 1):
        # print(t)
        if t == next_arrival:
            # log.info(f"{next_arrival=}", {'clock': clock.now()})
    
            func_idx = inv%num_functions
            request = Request(
                timestamp=clock.now(),
                dest=functions[func_idx].name,
                duration=runtime_milli,
                memory=memory_mib
            )
            cluster.accept(request)
            log.info('', {'clock':clock.now()})

            next_arrival += iat_milli
            inv += 1
        
        if t % AUTOSCALING_PERIOD_MILLI == 0:
            '''Autoscaling round every 2s.'''
            cluster.autoscaler.evaluate()

        if t % CRI_ENGINE_PULLING == 0:
            cluster.reconcile()

        if not t % 10000: 
            log.info('', {'clock':clock.now()})
        clock.inc(1)

if __name__ == "__main__":
    sys.exit(main())
