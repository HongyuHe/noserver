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


def main():
    runtime_milli = 1e3 # 1s
    memory_mib = 1000
    rps = 1
    iat_milli = 1e3 / rps
    duration_minute = 1
    num_invocations = int(np.ceil(duration_minute * 60 * 1e3 / iat_milli))

    num_workers = 2
    num_functions = 2

    clock = Clock()
    worker = Node(num_cores=16, memory_mib=64 * 2**10)
    nodes = [copy.deepcopy(worker) for _ in range(num_workers)]

    functions = [Function(name=f"func-{i}") for i in range(num_functions)]
    cluster = Cluster(nodes=nodes, functions=functions)

    for inv in range(num_invocations):
        func_idx = inv%num_functions
        request = Request(
            timestamp=clock.now(),
            dest=functions[func_idx].name,
            duration=runtime_milli,
            memory=memory_mib
        )
        
        clock.inc(iat_milli)
        cluster.accept(request)

if __name__ == "__main__":
    sys.exit(main())
