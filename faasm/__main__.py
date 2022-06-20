"""Console script."""
import sys

import numpy as np

from model.cluster import *
import simulation as sim


def run_rps_mode():
    runtime_milli = 1e3  # 1s
    memory_mib = 170
    num_functions = 1
    num_workers = 1

    clock = sim.Clock()
    functions = [Function(name=f"func-{i}") for i in range(num_functions)]
    nodes = [
        Node(name=f"worker-{i}", num_cores=16, memory_mib=64 * 2 ** 10)
        for i in range(num_workers)
    ]
    cluster = Cluster(clock, nodes, functions)

    rps_start = 1
    rps_end = 20
    rps_step = 1
    rps_slot_sec = 60
    invocation_duration_milli = (rps_step - rps_start + 1) * rps_slot_sec * 1000

    inv_index = 0
    for rps in range(rps_start, rps_end + 1, rps_step):
        print('RPS=', rps)
        sim.state.rps = rps
        iat_milli = int(1e3 / rps)
        next_arrival = 0
        for t in range(rps_slot_sec * 1000):
            if t == next_arrival:
                func_idx = inv_index % num_functions
                request = Request(
                    index=inv_index,
                    timestamp=clock.now(),
                    rps=rps,
                    dest=functions[func_idx].name,
                    duration=runtime_milli,
                    memory=memory_mib
                )
                cluster.accept(request)
                sim.log.info(f"Invocation {inv_index}", {'clock': clock.now()})

                next_arrival += iat_milli
                inv_index += 1

            cluster.run()

            if not t % 10000:
                sim.log.info('Clock', {'clock': clock.now()})
            clock.inc(1)

    while not cluster.is_finished(inv_index):
        cluster.run()
        clock.inc(1)

    cluster.dump()
    return


def run_test_mode():
    rps = 1
    runtime_milli = 1e3  # 1s
    memory_mib = 170
    iat_milli = int(1e3 / rps)
    duration_minute = 1
    num_invocations = int(np.ceil(duration_minute * 60 * 1e3 / iat_milli))

    num_workers = 1
    num_functions = 2

    clock = sim.Clock()
    functions = [Function(name=f"func-{i}") for i in range(num_functions)]
    nodes = [
        Node(name=f"worker-{i}", num_cores=16, memory_mib=64 * 2 ** 10)
        for i in range(num_workers)
    ]
    cluster = Cluster(clock, nodes, functions)

    inv = 0
    next_arrival = 0
    # * Current granularity: 1 ms.
    t = -1
    while not cluster.is_finished(num_invocations):
        t += 1
        # for t in range(duration_minute * 60 * 1000 + 1):
        if t == next_arrival and inv < num_invocations:
            # log.info(f"{next_arrival=}", {'clock': clock.now()})

            func_idx = inv % num_functions
            request = Request(
                index=inv,
                timestamp=clock.now(),
                rps=rps,
                dest=functions[func_idx].name,
                duration=runtime_milli,
                memory=memory_mib
            )
            cluster.accept(request)
            sim.log.info('', {'clock': clock.now()})

            next_arrival += iat_milli
            inv += 1

        cluster.run()

        if not t % 10000:
            sim.log.info('', {'clock': clock.now()})
        clock.inc(1)

    cluster.dump()


def main(mode):
    if mode == 'rps':
        run_rps_mode()
    elif mode == 'test':
        run_test_mode()
    return


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
