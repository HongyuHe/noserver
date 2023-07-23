import csv
from typing import *
import networkx as nx
import copy

from .. import simulation as sim
from .autoscaler import Autoscaler
from .throttler import Throttler
from .scheduler import Scheduler
from .instance import InstanceStatus
from .function import Function, Request
from .worker import Node, HarvestVM, WorkerType
from .state import State


cluster_config = sim.FLAGS.config.cluster
policy_config = sim.FLAGS.config.policy
hvm_config = sim.FLAGS.config.harvestvm

# * Every 4th HVMs (9 in total).
HVMS = [
    # '01b2ed4cc7b1',
    "26ff823a8dd5",
    "11ce77b9f010",
    "82859cd4f643",
    "4c332aa9b494",
    "e5c949bb9da9",
    "ad1387c95d15",
    "28a9e9444f41",
    "c46f41ab97dd",
]


class Cluster(object):
    def __init__(
        self,
        clock: sim.Clock,
        nodes: List[Node],
        functions: List[Function],
        dags: Dict[str, nx.DiGraph] = None,
    ):
        self.nodes = nodes
        for node in self.nodes:
            # * Inverse reference.
            node.cluster = self

        self.scheduler = Scheduler(nodes)
        self.throttler = Throttler(functions)
        self.autoscaler = Autoscaler(functions)

        # * Finished requests stats.
        self.sink = []
        # * Cluster resource stats.
        self.trace = []

        # ! Pick the first `self.num_harvestvms` for now.
        self.hvms: Set[str] = set(HVMS[: hvm_config.NUM_HVMS])
        # * Checkpoints for spawning HarvestVMs.
        # * Initialize them immediately during the 1st `run()`.
        self.hvm_ckps: Dict[str:int] = {
            hvm: clock.now() - hvm_config.HARVESTVM_SPAWN_LATENCY_MILLI
            for hvm in self.hvms
        }

        self.num_workers = len(self.nodes) + len(self.hvms)

        # * A Hack for initializing global simulation state to avoid circular imports :)
        sim.state = State(functions, self.autoscaler, self.throttler, clock, dags)

    def run(self):
        now = sim.state.clock.now()

        self.maintain_hvms(now)

        # self.run_cri_engines()

        """Invoking function instances only if there is any to be finish."""
        # if now in sim.state.request_end_times:
        self.run_instances()

        """Dispatching queued requests."""
        if now % cluster_config.DISPATCH_PERIOD_MILLI == 0:
            self.throttler.dispatch()

        """Serving ready requests released from workflows"""
        if now % cluster_config.NETWORK_DELAY_MILLI == 0:
            request = (
                next(sim.state.released_requests)
                if not sim.state.released_requests.empty()
                else None
            )
            if request:
                self.ingress_accept(request)

        """Asynchronous autoscaling round."""
        if now % cluster_config.AUTOSCALING_PERIOD_MILLI == 0:
            self.autoscaler.evaluate()

        if now % cluster_config.SCHEDULING_PERIOD_MILLI == 0:
            self.place_instances()

        """K8s control loop."""
        if now % cluster_config.CRI_ENGINE_PULLING_PERIOD_MILLI == 0:
            self.reconcile()

        if now % cluster_config.UPDATE_CONCURRENCY_PERIOD_MILLI == 0:
            self.throttler.record_concurrencies()

        """Collecting cluster metrics."""
        if now % cluster_config.MONITORING_PERIOD_MILLI == 0:
            self.monitor()

        return

    def run_instances(self):
        # ! The function `run()` has *side effect*, namely deleting itself from the cluster
        # ! if it's an HVM and dies. This will alter the iteratable during loop!!!
        # ! (Must use *shallow* copy)
        nodes = self.nodes.copy()
        for node in nodes:
            # & Time-consuming!
            node.run()
        return

    def run_cri_engines(self):
        for node in self.nodes:
            node.spawn()
            node.evict()
        return

    def maintain_hvms(self, now: int):
        if not hvm_config.USE_HARVESTVM:
            return

        existing_hvms = set()
        for node in self.nodes:
            # node.run()
            if node.kind == WorkerType.HarvestVM:
                assert (
                    node.hvm_hash not in existing_hvms
                ), f"Found duplicated HVM {node.hvm_hash}"
                existing_hvms.add(node.hvm_hash)

        missing_hvms = self.hvms - existing_hvms
        for hvm_hash in missing_hvms:
            # * Record the time at which this HarvestVM is needed.
            ckp = self.hvm_ckps[hvm_hash]
            self.hvm_ckps[hvm_hash] = now if not ckp else ckp

            # * Spawn HarvestVM if latency is met.
            if (
                now
                >= self.hvm_ckps[hvm_hash] + hvm_config.HARVESTVM_SPAWN_LATENCY_MILLI
            ):
                hvm = HarvestVM(
                    f"hvm-{hvm_hash}",
                    0,  # * The size of the HarvestVM is determined by the trace.
                    130 * 2**10,  # ? Currently fixed.
                    hvm_hash=hvm_hash,
                    start_time=now,
                )
                # * Initiate the inverse pointer (a hack).
                hvm.cluster = self
                self.nodes.append(hvm)

                # * Reset HarvestVM checkpoint.
                self.hvm_ckps[hvm_hash] = None

                sim.log.info(f"(cluster) Created {hvm.name=}", {"clock": now})

        assert (
            len(self.nodes) <= self.num_workers
        ), f"#nodes={len(self.nodes)} > {self.num_workers=}"

        # * Randomize order with new HVMs appended.
        if missing_hvms:
            sim.rng.shuffle(self.nodes)
            # ! Sync scheduler nodes after adding new nodes.
            self.scheduler.nodes = self.nodes
        return

    def ingress_accept(self, request: Request):
        now = sim.state.clock.now()
        self.throttler.hit(request)
        sim.log.info(f"(throttler) Arrival {request.req_id}", {"clock": now})
        return

    def place_instances(self):
        for func, scaler in self.autoscaler.scalers.items():
            # ! Don't use the `actual_scale` from the autoscaler as it updates slower than the throttler.
            # diff = scaler.desired_scale - scaler.actual_scale
            diff = scaler.desired_scale - self.throttler.trackers[func].get_scale()

            if diff:
                self.scheduler.schedule(func, diff)
        return

    def reconcile(self):
        for node in self.nodes:
            node.reconcile()

    def is_finished(self):
        for node in self.nodes:
            for instance in node.instances:
                if instance.status == InstanceStatus.RUNNING:
                    return False

        if len(sim.state.flows.keys()) > 0:
            return False
        return True

    def monitor(self):
        total_desired_scale = 0
        total_actual_scale = 0
        total_active_instances = 0
        total_running_instances = 0
        total_existing_instances = 0
        total_terminating_instances = 0
        # total_remaining_capacity = 0

        for func, scaler in self.autoscaler.scalers.items():
            """This is Knative's view"""
            total_desired_scale += scaler.desired_scale
            total_actual_scale += scaler.actual_scale

        cpu_utilizations = []
        mem_utilizations = []
        for node in self.nodes:
            # total_remaining_capacity += node.get_num_available_slots()
            """This is K8s's view"""
            for instance in node.instances:
                total_existing_instances += 1
                if instance.status in [InstanceStatus.RUNNING, InstanceStatus.IDLE]:
                    total_running_instances += 1
                if instance.status != InstanceStatus.TERMINATING:
                    total_active_instances += 1
                if instance.status == InstanceStatus.TERMINATING:
                    total_terminating_instances += 1
            cpu, mem = node.get_utilizations()
            cpu_utilizations.append(cpu)
            mem_utilizations.append(mem)

        record = {
            "rps": sim.state.rps,
            "timestamp": sim.state.clock.now(),
            "actual_scale": total_actual_scale,
            "desired_scale": total_desired_scale,
            "running_instances": total_running_instances,
            "active_instances": total_active_instances,
            "existing_instances": total_existing_instances,
            # 'remaining_capacity': total_remaining_capacity,
            "terminating_instances": total_terminating_instances,
            "worker_cpu_avg": sum(cpu_utilizations) / len(cpu_utilizations),
            "worker_mem_avg": sum(mem_utilizations) / len(mem_utilizations)
            + cluster_config.MEMORY_USAGE_OFFSET,
        }
        self.trace.append(record)
        return

    def drain(self, node: Node, request: Request):
        record = {
            "req_id": request.req_id,
            "flow_id": request.flow_id,
            "dag": request.dag_name,
            "node": node.name,
            "host": node.kind.name,
            "rps": request.rps,
            "arrival_time": request.arrival_time,
            "start_time": request.start_time,
            "end_time": request.end_time,
            "cpu_time": request.total_cputime,
            "latency": request.end_time - request.arrival_time - request.duration
            if not request.failed
            else float("nan"),  # * Return NaN in case of failure.
            "function": request.dest,
            "duration": request.duration,
            "memory": request.memory,
            "survival_prob": round(node.survival_prob(), 5),
            "failed": request.failed,
        }

        self.sink.append(record)
        return

    def dump(self):
        self.sink.sort(key=lambda r: r["flow_id"])

        key = f"w-{sim.FLAGS.width}_d-{sim.FLAGS.depth}_n-{sim.FLAGS.invocations}_dup-{int(policy_config.DUP_EXECUTION)}_r-{policy_config.DUP_EXECUTION_THRESHOLD}"
        with open(f"data/results/cluster_{key}.csv", "w", newline="") as f:
            headers = self.trace[0].keys()
            cw = csv.DictWriter(
                f, headers, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
            )
            cw.writeheader()
            cw.writerows(self.trace)

        with open(f"data/results/requests_{key}.csv", "w", newline="") as f:
            headers = self.sink[0].keys()
            cw = csv.DictWriter(
                f, headers, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL
            )
            cw.writeheader()
            cw.writerows(self.sink)

    def __repr__(self):
        return "Cluster" + repr(vars(self))
