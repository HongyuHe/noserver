import sys

sys.path.append("..")
import simulation as sim

from .components import *
from .function import Function, Instance, Request, Breaker

from typing import *
from pprint import pprint

CRI_ENGINE_PULLING = int(1000 / 100)  # * kubelet QPS of our the 500-node cluster.
INSTANCE_SIZE_MIB = 400  # TODO: size of firecracker.
INSTANCE_CREATION_DELAY_MILLI = 1000  # TODO: measure CRI engine delay.


# noinspection PyProtectedMember
class Node(object):
    def __init__(self, name, num_cores, memory_mib, instance_limit=500):
        self.name = name
        self.num_cores = num_cores
        self.memory_mib = memory_mib
        self.instance_limit = instance_limit

        self.cpu_registry = {core: None for core in range(self.num_cores)}
        self.runqueue = []
        self.instances: List[Instance] = []
        self.backlog = []
        self.sink = []

    def run(self):
        for instance in self.instances:
            instance.run()
        return

    def book_core(self, instance: Instance):
        if instance in self.runqueue:
            rank = self.runqueue.index(instance)
            # * FCFS: Only handle the booking if it's the first in the queue.
            if rank != 0:
                return False
            else:
                self.runqueue.remove(instance)

        avail_core = None
        for core, status in self.cpu_registry.items():
            if status is None:
                avail_core = core
        if avail_core is None:
            self.runqueue.append(instance)
            return False
        else:
            self.cpu_registry[avail_core] = instance
            return True

    def yield_core(self, instance: Instance, request: Request):
        for core, status in self.cpu_registry.items():
            if status == instance:
                self.cpu_registry[core] = instance
        self.sink.append(request)
        return

    def get_available_slots(self):
        return self.instance_limit - len(self.instances)
        # min(
        #     self.instance_limit - len(self.instances),
        #     # # TODO: Estimate based on the actual requests.
        #     # (self.memory_mib - len(self.instances)*INSTANCE_SIZE_MIB) / INSTANCE_SIZE_MIB,
        # )

    def accept(self, func, diff):
        self.backlog.append({
            'time': sim.state.clock.now(),
            'func': func,
            'quantity': diff,
        })
        # sim.log.info(f"{self.backlog=}")
        return

    def reconcile(self):
        if len(self.backlog) == 0: return
        # * Order by request time (FCFS).
        self.backlog = sorted(self.backlog, key=lambda r: r['time'])
        request = self.backlog[0]  # * Only handle the first.

        # TODO: Distinguish between cold start and normal start.
        # TODO: Also, add the burst blocking delay.
        if sim.state.clock.now() - request['time'] < INSTANCE_CREATION_DELAY_MILLI:
            # * CRI engine delay has not been fulfilled.
            return

        new_instance = Instance(func=request['func'], node=self)
        self.instances.append(new_instance)
        self.backlog.remove(request)
        # * Assume one engine can only create one pod at a time.
        if request['quantity'] > 1:
            request['quantity'] -= 1
            self.backlog.append(request)

        # TODO: Add discovery latency
        tracker: Throttler._Tracker_ = sim.state.throttler.trackers[request['func']]
        # TODO: Unify instances to one place.
        tracker.instances.append(new_instance)

        sim.log.info(f"Spawn {new_instance.func=} on {self.name}", {'clock': sim.state.clock.now()})
        return

    def __repr__(self):
        return "Node" + repr(vars(self))


class Cluster(object):
    def __init__(self, clock: sim.Clock, nodes: List[Node], functions: List[Function], ):
        self.nodes = nodes
        # self.functions = functions
        self.throttler = Throttler(functions)
        self.autoscaler = Autoscaler(functions)

        self.scheduler = Scheduler(nodes)
        self.differences: Dict[str: int] = {func.name: 0 for func in functions}

        sim.state = State(self.autoscaler, self.throttler, clock)

    def run(self):
        for node in self.nodes:
            node.run()
        self.throttler.dispatch()
        return

    def accept(self, request: Request):
        self.throttler.hit(request)
        return

    def reconcile(self):
        for func, scaler in self.autoscaler.scalers.items():
            # diff = scaler.desired_scale - scaler.actual_scale
            # ! Don't use the `actual_scale from the autoscaler as it updates slower than the throttler.
            diff = scaler.desired_scale - self.throttler.trackers[func].get_scale()
            prev_diff = self.differences[func]
            if diff != prev_diff:
                self.differences[func] = diff
                if diff != 0:
                    # * Only schedule if there is a non-zero difference
                    # * AND the difference has changed.
                    self.scheduler.schedule(func, diff - prev_diff)
            for node in self.nodes:
                node.reconcile()

    def is_finished(self, total_requests):
        total_returned = 0
        for node in self.nodes:
            total_returned += len(node.sink)
        return total_returned == total_requests

    def dump(self):
        print("\nResults:")
        for node in self.nodes:
            pprint(node.name)
            pprint(node.sink)

    def __repr__(self):
        return "Cluster" + repr(vars(self))


class Scheduler(object):
    def __init__(self, nodes: List[Node]):
        # TODO: ordering
        self.nodes = nodes

    def schedule(self, func, num):
        # TODO: bin-packing (best-fit) -- currently first available.
        if num > 0:
            """Scaling up"""
            sim.log.info(f"Schedule {num} instances of {func}", {'clock': sim.state.clock.now()})
            all_scheduled = False
            while not all_scheduled:
                for node in self.nodes:
                    slots = node.get_available_slots()
                    can_take = min(slots, num)
                    if can_take > 0:
                        node.accept(func, can_take)
                    if can_take == num:
                        all_scheduled = True
                        break
                    else:
                        num -= can_take
        else:
            # TODO: Scaling down
            sim.log.info("[Unimplemented] Scaling down")

        return

    def __repr__(self):
        return "Scheduler" + repr(vars(self))
