"""Console script."""
import numpy as np

from model.cluster import *
from simulation import *


def main():
    rps = 1
    runtime_milli = 1e3  # 1s
    memory_mib = 1000
    iat_milli = int(1e3 / rps)
    duration_minute = 1
    num_invocations = int(np.ceil(duration_minute * 60 * 1e3 / iat_milli))

    num_workers = 2
    num_functions = 2

    clock = Clock()
    nodes = [
        Node(name=f"worker-{i}", num_cores=16, memory_mib=64 * 2 ** 10)
        for i in range(num_workers)
    ]
    functions = [Function(name=f"func-{i}") for i in range(num_functions)]

    cluster = Cluster(clock, nodes, functions)

    inv = 0
    prev_arrival, next_arrival = 0, 0
    # * Current granularity: 1 ms.
    t = -1
    # while not cluster.is_finished(num_invocations):
    #     t += 1
    for t in range(duration_minute * 60 * 1000 + 1):
        if t == next_arrival and inv < num_invocations:
            # log.info(f"{next_arrival=}", {'clock': clock.now()})

            func_idx = inv % num_functions
            request = Request(
                index=inv,
                timestamp=clock.now(),
                dest=functions[func_idx].name,
                duration=runtime_milli,
                memory=memory_mib
            )
            cluster.accept(request)
            log.info('', {'clock': clock.now()})

            next_arrival += iat_milli
            inv += 1

        if t % AUTOSCALING_PERIOD_MILLI == 0:
            '''Autoscaling round every 2s.'''
            cluster.autoscaler.evaluate()

        if t % CRI_ENGINE_PULLING == 0:
            cluster.reconcile()

        cluster.run()

        if not t % 10000:
            log.info('', {'clock': clock.now()})
        clock.inc(1)

    cluster.dump()
    return


if __name__ == "__main__":
    sys.exit(main())
