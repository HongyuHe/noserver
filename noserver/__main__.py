"""Console script."""
import sys
import time
import networkx as nx
import cloudpickle

from . import simulation as sim
from .system.cluster import *

request_config = sim.FLAGS.config.request


def run_trace_mode():
    sim.log.info(f"Loading workflows from {sim.FLAGS.trace}")
    with open(sim.FLAGS.trace, "rb") as f:
        trace_dags: List[nx.DiGraph] = cloudpickle.load(f)
        trace_dags = sim.rng.choices(trace_dags, k=1000)
        # trace_dags = [dag for dag in trace_dags if dag.nodes['F0']['dag_name'] != 'bundled_dag_240']
        # trace_dags = sim.interleave_lists(trace_dags, trace_dags)
        # # trace_dags = trace_dags + trace_dags
        # sim.rng.shuffle(trace_dags)
        total_flows = len(trace_dags)
    sim.log.info(f"Loaded {total_flows} DAGs.")

    arrival_times = sim.generate_exp_arrival_times_milli(sim.FLAGS.rps, total_flows)

    num_workers = sim.FLAGS.vm
    num_cores = sim.FLAGS.cores

    clock = sim.Clock()
    functions = []
    dags = {}
    # ! DAG name should be part of the function name when there are multiple DAGs.
    for dag in trace_dags:
        for func, attributes in dag.nodes(data=True):
            assert (
                attributes["vcpu"] <= sim.FLAGS.cores
            ), f"Function exceeded worker core count!"
            functions.append(
                Function(
                    name=attributes["dag_name"] + "-" + func,
                    # * Currently, `Function` only carries name and vcpu.
                    vcpu=attributes["vcpu"],
                )
            )
            dags[attributes["dag_name"]] = dag

    nodes = [
        Node(
            name=f"node-{i}",
            num_cores=num_cores,
            memory_mib=192 * 2**10,
            start_time=clock.now(),
        )
        for i in range(num_workers)
    ]
    cluster = Cluster(clock, nodes, functions, dags)

    flow_id = 0
    invocation_idx = 0
    sim.state.rps = sim.FLAGS.rps

    while True:
        ts = clock.now()

        """Invoking new requests."""
        if ts == arrival_times[invocation_idx]:
            """Constructing flow"""
            # ^ Invoke every DAG only once for now.
            # ! This will lead to cold start on EVERY invocation!!!
            # TODO: Technically, parallel invocations are of the SAME functions!!!
            dag: nx.DiGraph = trace_dags[flow_id]
            sim.state.add_flow(flow_id, dag)
            # ~ Assume DAGs are single-rooted.
            roots = [n for n, d in dag.in_degree() if d == 0]
            assert len(roots) == 1, f"DAG has >1 root!"
            root_func = roots[0]

            request = Request(
                flow_id=flow_id,
                dag_name=dag.nodes[root_func]["dag_name"],
                duration=min(
                    dag.nodes[root_func]["duration_milli"],
                    request_config.MAX_DURATION_SEC * 1000,
                ),
                memory=dag.nodes[root_func]["memory_mib"],
                arrival_time=clock.now(),
                rps=sim.state.rps,
                dest=f"{dag.nodes[root_func]['dag_name']}-{root_func}",
            )
            cluster.ingress_accept(request)
            sim.log.info(
                f"(main) Invoked root function {request.req_id}", {"clock": clock.now()}
            )

            flow_id += 1
            invocation_idx += 1
            if invocation_idx == total_flows:
                break
            elif arrival_times[invocation_idx - 1] != arrival_times[invocation_idx]:
                # ! Only increase the clock if the next timestamp is not the same as the current one.
                clock.inc(1)
        else:
            clock.inc(1)

        cluster.run()
    # > End of load generation loop.

    while not cluster.is_finished():
        cluster.run()
        clock.inc(1)

    cluster.dump()
    return


def run_benchmark_mode():
    dag: nx.DiGraph = sim.generate_dag(
        "gen_dag",
        width=sim.FLAGS.width,
        depth=sim.FLAGS.depth,
        duration_milli=1000,
        memory_mib=170,
    )

    # * Below is the total number of calls to the DAG as a whole.
    total_flows = (
        sim.FLAGS.invocations // sim.FLAGS.depth
        if sim.FLAGS.width == 1
        else sim.FLAGS.invocations // sim.FLAGS.width
    )
    sim.log.info(f"Total number of flows: {total_flows}")
    sim.log.info(f"Actual number of invocations: {total_flows*dag.number_of_nodes()}")

    arrival_times = sim.generate_exp_arrival_times_milli(sim.FLAGS.rps, total_flows)

    num_workers = 0
    num_cores = 40

    clock = sim.Clock()
    functions = []
    # ! DAG name should be part of the function name when there are multiple DAGs.
    for func, attributes in dag.nodes(data=True):
        functions.append(
            Function(
                name=func,
                # * Currently, `Function` only carries name and vcpu.
                vcpu=attributes["vcpu"],
            )
        )

    nodes = [
        Node(
            name=f"node-{i}",
            num_cores=num_cores,
            memory_mib=192 * 2**10,
            start_time=clock.now(),
        )
        for i in range(num_workers)
    ]
    cluster = Cluster(clock, nodes, functions, {"gen_dag": dag})

    flow_id = 0
    invocation_idx = 0
    # TODO: Get rid of this.
    sim.state.rps = sim.FLAGS.rps
    roots = [n for n, d in dag.in_degree() if d == 0]

    while True:
        ts = clock.now()

        """Invoking new requests."""
        if ts == arrival_times[invocation_idx]:
            """Constructing flow"""
            sim.state.add_flow(flow_id, dag)
            for func in roots:
                request = Request(
                    flow_id=flow_id,
                    dag_name="gen_dag",
                    arrival_time=clock.now(),
                    rps=sim.state.rps,
                    dest=func,
                    duration=dag.nodes[func]["duration_milli"],
                    memory=dag.nodes[func]["memory_mib"],
                )
                cluster.ingress_accept(request)
                sim.log.info(
                    f"(main) Invoked root function {request.req_id}",
                    {"clock": clock.now()},
                )

            flow_id += 1
            invocation_idx += 1
            if invocation_idx == total_flows:
                break
            elif arrival_times[invocation_idx - 1] != arrival_times[invocation_idx]:
                # ! Only increase the clock if the next timestamp is not the same as the current one.
                clock.inc(1)
        else:
            clock.inc(1)

        cluster.run()
    # > End of load generation loop.

    while not cluster.is_finished():
        cluster.run()
        clock.inc(1)

    cluster.dump()
    return


def run_dag_mode():
    dags = sim.load_dags(
        f"./workloads/dags/test_parallel_s{sim.FLAGS.stages}_m170_t1000.json",
        display=sim.FLAGS.display,
    )
    inv_df = pd.read_csv(
        f"./workloads/invocation/test_harvest_parallel_jsontest_parallel_s{sim.FLAGS.stages}_m170_t1000_invoke{sim.FLAGS.invocation}_poisson1000.csv"
    ).sort_values(by="timestamp")

    num_workers = 32
    num_cores = 32

    clock = sim.Clock()
    functions = []
    for _, dag in dags.items():
        for func, _ in dag.nodes(data=True):
            functions.append(Function(name=func))

    nodes = [
        Node(
            name=f"node-{i}",
            num_cores=num_cores,
            memory_mib=64 * 2**10,
            start_time=clock.now(),
        )
        for i in range(num_workers)
    ]
    cluster = Cluster(clock, nodes, functions, dags)

    last_ts = inv_df.iloc[-1]["timestamp"]
    records = iter(inv_df.iterrows())
    # * Load the first invocation.
    record = next(records)
    prev_ts = 0
    flow_id = -1
    inv_count = 0

    # * Adjust clock to 1ms prior to the first timestamp.
    clock.inc(inv_df.iloc[0]["timestamp"] - 1)
    while True:
        ts = clock.now()
        if ts == record[1]["timestamp"]:
            """Invoking new requests."""
            record = record[1]
            num_invocations = record["num_invocations"]
            inv_count += num_invocations
            rps = round(inv_count / (ts - prev_ts + 1), 3)
            sim.state.rps = rps
            prev_ts = ts

            dag: nx.DiGraph = dags[record["dag_name"]]
            roots = [n for n, d in dag.in_degree() if d == 0]

            for _ in range(num_invocations):
                """Constructing flow"""
                flow_id += 1
                sim.state.add_flow(flow_id, dag)
                for func in roots:
                    request = Request(
                        flow_id=flow_id,
                        dag_name=record["dag_name"],
                        arrival_time=clock.now(),
                        rps=rps,
                        dest=func,
                        duration=dag.nodes[func]["duration_milli"],
                        memory=dag.nodes[func]["memory_mib"],
                    )
                    cluster.ingress_accept(request)
                    sim.log.info(
                        f"Invoked root function {func} of {record['dag_name']}",
                        {"clock": clock.now()},
                    )

            # * If not the last record, load the next one from invocation pattern.
            if ts == last_ts:
                break

            record = next(records)
            if ts != record[1]["timestamp"]:
                # ! Only increase the clock if the next timestamp is not the same as the current one.
                clock.inc(1)
        else:
            clock.inc(1)

        cluster.run()
    # > End of load generation loop.

    while not cluster.is_finished():
        cluster.run()
        clock.inc(1)

    cluster.dump()
    return


def run_rps_mode():
    runtime_milli = int(1e3)  # 1s
    memory_mib = 170
    num_functions = 10
    num_workers = 1
    num_cores = 16

    clock = sim.Clock()
    functions = [Function(name=f"func-{i}") for i in range(num_functions)]
    nodes = [
        Node(name=f"worker-{i}", num_cores=num_cores, memory_mib=64 * 2**10)
        for i in range(num_workers)
    ]
    cluster = Cluster(clock, nodes, functions)

    rps_start = 1
    rps_end = 18
    rps_step = 1
    rps_slot_sec = 60

    """"Generating requests."""
    inv_index = 0
    for rps in range(rps_start, rps_end + 1, rps_step):
        print("RPS=", rps)
        sim.state.rps = rps
        iat_milli = int(1e3 / rps)
        next_arrival = 0
        for t in range(rps_slot_sec * 1000):
            if t == next_arrival:
                func_idx = inv_index % num_functions
                request = Request(
                    flow_id=inv_index,
                    arrival_time=clock.now(),
                    rps=rps,
                    dest=functions[func_idx].name,
                    # * Execution time is only fully fulfilled after the server has saturated.
                    duration=runtime_milli
                    if rps > num_cores
                    else sim.rng.randint(runtime_milli - 100, runtime_milli),
                    memory=memory_mib,
                )
                cluster.ingress_accept(request)

                next_arrival += iat_milli
                inv_index += 1
                sim.log.info(f"Invocation {inv_index}", {"clock": clock.now()})

            cluster.run()

            if not t % 10000:
                sim.log.info("Clock", {"clock": clock.now()})
            clock.inc(1)

    """"Finishing the remaining requests."""
    while not cluster.is_finished():
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
    num_invocations = int(math.ceil(duration_minute * 60 * 1e3 / iat_milli))

    num_workers = 1
    num_functions = 2

    clock = sim.Clock()
    functions = [Function(name=f"func-{i}") for i in range(num_functions)]
    nodes = [
        Node(name=f"worker-{i}", num_cores=16, memory_mib=64 * 2**10)
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
                flow_id=inv,
                arrival_time=clock.now(),
                rps=rps,
                dest=functions[func_idx].name,
                duration=runtime_milli,
                memory=memory_mib,
            )
            cluster.ingress_accept(request)
            sim.log.info("", {"clock": clock.now()})

            next_arrival += iat_milli
            inv += 1

        cluster.run()

        if not t % 10000:
            sim.log.info("", {"clock": clock.now()})
        clock.inc(1)

    cluster.dump()


def main():
    match sim.FLAGS.mode:
        case "rps":
            run_rps_mode()
        case "test":
            run_test_mode()
        case "dag":
            run_dag_mode()
        case "benchmark":
            run_benchmark_mode()
        case "trace":
            run_trace_mode()
        case _:
            sim.log.error(f"Unsupported mode: {sim.FLAGS.mode}")
    return


if __name__ == "__main__":
    if sim.FLAGS.width != 1 and sim.FLAGS.depth != 1:
        sim.log.error(f"Not yet supporting hybrid worflows.")

    if (
        sim.FLAGS.width >= sim.FLAGS.invocations
        or sim.FLAGS.depth >= sim.FLAGS.invocations
    ):
        sim.log.error(f"DAG size greater > the total number of invocations")

    start_time = time.time()

    main()

    sys.exit(
        print(
            f"\n--- Simulation took {time.time() - start_time: .3f} seconds ---\n"
            "\nConfigurations:\n",
            sim.FLAGS.config,
        )
    )
