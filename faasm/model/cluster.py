import sys

sys.path.append("..")
import simulation as sim

from .components import *
from .function import Function, Instance, Request, Breaker

from typing import *
from collections import OrderedDict
from pprint import pprint

CRI_ENGINE_PULLING = int(1000 / 100)  # * kubelet QPS of our the 500-node cluster.
INSTANCE_SIZE_MIB = 400  # TODO: size of firecracker.
INSTANCE_CREATION_DELAY_MILLI = 1000  # TODO: measure CRI engine delay.

INSTANCE_GRACE_PERIOD_SEC = 40  # Default K8s grace period (30s) + empirical delay (10s).


# noinspection PyProtectedMember
class Node(object):
    def __init__(self, name, num_cores, memory_mib, instance_limit=500):
        self.cluster: Cluster

        self.name = name
        self.num_cores = num_cores
        self.memory_mib = memory_mib
        self.instance_limit = instance_limit

        self.cpu_registry = OrderedDict({core: None for core in range(self.num_cores)})
        self.instances: List[Instance] = []
        self.runqueue = []
        self.bindings = []

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
                self.cpu_registry[core] = None

        self.cluster.drain(self, request)
        return

    def get_available_slots(self):
        return self.instance_limit - len(self.instances)

    def bind(self, func, num):
        self.bindings.append({
            'time': sim.state.clock.now(),
            'func': func,
            'quantity': num,
        })
        return

    def kill(self, func, num):
        total = 0
        for instance in self.instances:
            # NOTE: Killing instances is NOT balanced across workers for now.
            if instance.func == func:  # and instance.idle:
                total += 1

        remaining = max(num - total, 0)
        self.bind(
            func,
            num if remaining == 0 else total,
        )
        return remaining

    def reconcile(self):
        if len(self.bindings) == 0: return
        # * Order by request time (FCFS).
        self.bindings = sorted(self.bindings, key=lambda r: r['time'])
        binding = self.bindings[0]  # * Only handle the first.

        tracker: Throttler._Tracker_ = sim.state.throttler.trackers[binding['func']]

        if binding['quantity'] > 0:
            """Creating new instances."""
            # TODO: Distinguish between cold start and normal start.
            # TODO: Also, add the burst blocking delay.
            if sim.state.clock.now() - binding['time'] < INSTANCE_CREATION_DELAY_MILLI:
                # * CRI engine delay has not been fulfilled.
                return

            self.bindings.remove(binding)  # * Dequeue binding request.
            new_instance = Instance(func=binding['func'], node=self)
            self.instances.append(new_instance)

            # * Assume one engine can only create one pod at a time.
            if binding['quantity'] > 1:
                binding['quantity'] -= 1
                self.bindings.append(binding)  # * Add the remaining back.

            # TODO: Add discovery latency
            tracker.instances.append(new_instance)

            sim.log.info(f"Spawn {new_instance.func=} on {self.name}", {'clock': sim.state.clock.now()})

        elif binding['quantity'] > 0:
            """Taking down instances."""
            terminating = []
            deadline = sim.state.clock.now() + INSTANCE_GRACE_PERIOD_SEC * 1000
            updated_instance_list = self.instances
            for instance in self.instances:
                # * Garbage-collect all expired instances.
                if instance.terminating and instance.deadline == sim.state.clock.now():
                    updated_instance_list.remove(instance)
                    tracker.instances.remove(instance)

                # * Terminate required # of instances.
                if instance.idle and not instance.terminating:
                    if len(terminating) < binding['quantity']:
                        instance.terminating = True
                        instance.deadline = deadline
                        terminating.append(instance)

            self.instances = updated_instance_list

            # * Manually sync throttler's view.
            # They should share the same set of instances but just in case.
            for instance in terminating:
                throttler_instance = tracker.instances[tracker.instances.index(instance)]
                throttler_instance.terminating = True
                throttler_instance.deadline = deadline

            self.bindings.remove(binding)  # * Dequeue binding request.
            remaining = binding['quantity'] - len(terminating)
            if remaining > 0:
                binding['quantity'] = remaining
                self.bindings.append(binding)  # * Add the remaining back.
        else:
            raise RuntimeError("Zero binding")
        return

    def __repr__(self):
        return "Node" + repr(vars(self))


class Cluster(object):
    def __init__(self, clock: sim.Clock, nodes: List[Node], functions: List[Function], ):
        self.nodes = nodes
        for node in self.nodes:
            # * Inverse reference.
            node.cluster = self

        self.scheduler = Scheduler(nodes)
        self.throttler = Throttler(functions)
        self.autoscaler = Autoscaler(functions)

        self.differences: Dict[str: int] = {func.name: 0 for func in functions}
        self.sink = []

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
        return total_requests == len(self.sink)

    def drain(self, node: Node, request: Request):
        total_desired_instances = 0
        total_running_instances = 0

        for _, scaler in self.autoscaler.scalers.items():
            total_desired_instances += scaler.desired_scale

        for node in self.nodes:
            # ! This is an updated view, which might be different
            # from that of the throttler (Knative).
            total_running_instances += len(node.instances)

        # for _, tracker in self.throttler.trackers.items():
        #     # ! This is the same view as that of the Knative.
        #     total_running_instances += tracker.get_scale()
        record = {
            'index': request.index,
            'arrival': request.arrival,
            'start_time': request.start_time,
            'end_time': request.end_time,
            'latency': request.end_time - request.arrival - request.duration,
            'running_instances': total_running_instances,
            'desired_instances': total_desired_instances,
            'function': request.dest,
            'node': node.name,
            'duration': request.duration,
            'memory': request.memory,
        }

        self.sink.append(record)
        return

    def dump(self):
        print("\nResults:")
        self.sink.sort(key=lambda r: r['index'])
        for record in self.sink:
            print(record)
        # pprint(self.sink, compact=True, width=190)

    def __repr__(self):
        return "Cluster" + repr(vars(self))


class Scheduler(object):
    def __init__(self, nodes: List[Node]):
        # TODO: ordering
        self.nodes = nodes

    def schedule(self, func, num):
        # TODO: bin-packing (best-fit) -- currently first available.
        if num > 0:
            sim.log.info(f"Bind {num} instances of {func}", {'clock': sim.state.clock.now()})
            """
            Bind new instances to nodes
            (! Potential deadlock)
            """
            all_scheduled = False
            while not all_scheduled:
                for node in self.nodes:
                    slots = node.get_available_slots()
                    can_take = min(slots, num)
                    if can_take > 0:
                        node.bind(func, can_take)
                    if can_take == num:
                        all_scheduled = True
                        break
                    else:
                        num -= can_take
        else:
            sim.log.info(f"Destroy {num} instances of {func}", {'clock': sim.state.clock.now()})
            """
            Destroy instances
            (! Potential deadlock)
            """
            all_scheduled = False
            while not all_scheduled:
                for node in self.nodes:
                    remaining = node.kill(func, num)
                    if remaining == 0:
                        all_scheduled = True
                    else:
                        num = remaining
        return

    def __repr__(self):
        return "Scheduler" + repr(vars(self))
