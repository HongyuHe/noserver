import math
from dataclasses import dataclass
from typing import *

if TYPE_CHECKING:
    from .cluster import Cluster
from collections import OrderedDict
from enum import Enum
import copy

from .. import simulation as sim
from .throttler import Throttler
from .instance import Instance, InstanceStatus

node_config = sim.FLAGS.config.node
hvm_config = sim.FLAGS.config.harvestvm


# HVM_SURVIVAL_MODEL_PATH = './data/harvestvm/models/survival_ecdf_app1.pkl'
HVM_SURVIVAL_MODEL_PATH = "./data/harvestvm/models/app01_kmf.pkl"
HVM_CORES_TABLE_PATH = "./data/harvestvm/models/cores_table.pkl"

with open(HVM_SURVIVAL_MODEL_PATH, "rb") as f_s, open(
    HVM_CORES_TABLE_PATH, "rb"
) as f_c:
    import pickle

    global survival_model, cores_table
    survival_model = pickle.load(f_s)
    cores_table = pickle.load(f_c)


class WorkerType(Enum):
    NormalVM = 1
    HarvestVM = 2


@dataclass
class SchedulingBinding(object):
    """Controller binding object (≈ K8s)"""

    sched_time: int
    touched_time: int
    func: str
    quantity: int


# noinspection PyProtectedMember
class Node(object):
    """A worker/VM running on bare-metal."""

    def __init__(
        self,
        name: str,
        num_cores: int,
        memory_mib: int,
        start_time: int,
        max_num_instances: int = node_config.MAX_NUM_INSTANCES,
    ):
        self.name = name
        self.start_time = start_time
        # * ` (1-node_config.INFRA_CPU_OVERHEAD_RATIO)` is the actually available CPU time,
        # * after subtracting the infra overhead.
        self.num_cores = int(num_cores * (1 - node_config.INFRA_CPU_OVERHEAD_RATIO))
        self.memory_mib = memory_mib
        self.max_num_instances = max_num_instances

        self.cpu_registry: OrderedDict[int, Instance] = OrderedDict(
            {core: None for core in range(self.num_cores)}
        )
        self.instances: List[Instance] = []
        self.creation_queue: List[Instance] = []
        self.eviction_queue: List[Instance] = []
        self.num_instances_created_sec = 0
        self.num_instances_evicted_sec = 0

        # ! The instances in the `runqueue` is not explicitly dispatched.
        # * Instead, upon each `run() -> serve()`, all instances (either running and queued) will
        # * try to `book_cores()` for themselves, which gets them out of `runqueue` if successful.
        self.runqueue: List[Instance] = []
        # * Spliting workqueue of k8s' controller into two:
        self.controller_workqueue: List[SchedulingBinding] = []

        self.base_hazard_milli = 0
        self.kind: WorkerType = WorkerType.NormalVM

        # * A hacky inverse reference to the cluster
        # * for draining finished requests, initialized in `Cluster`.
        self.cluster: Cluster = None

    def run(self):
        self.spawn()
        self.evict()

        for instance in self.instances:
            instance.run()
        return

    def get_utilizations(self):
        occupancy = 0
        for _, status in self.cpu_registry.items():
            if status is not None:
                occupancy += 1
        cpu_utilization = (
            occupancy / self.num_cores * 100 if self.num_cores > 0 else 0
        )  # * For HarvestVMs.

        memory_used = 0
        for instance in self.instances:
            # * Consider both terminating and running instances.
            if instance.hosted_job is not None:
                memory_used += (
                    instance.hosted_job.memory + node_config.JOB_MEMORY_OVERHEAD_MIB
                )
            else:
                memory_used += node_config.INSTANCE_SIZE_MIB
        memory_usage = memory_used / self.memory_mib * 100
        return cpu_utilization, memory_usage

    def get_available_core_ids(self):
        avail_cores = []
        for core, status in self.cpu_registry.items():
            if status is None:
                avail_cores.append(core)
        return avail_cores

    def book_cores(self, instance: Instance):
        """Allocate cores for an instance to run.

        :param instance: {Instance}
        :return: {bool}
        """
        if instance not in self.runqueue:
            self.runqueue.append(instance)

        rank = self.runqueue.index(instance)
        """$$$ Kernel sched: FCFS -- Only handle the booking if it's the first in the queue."""
        if rank == 0:
            self.runqueue.remove(instance)
        else:
            return False

        requested_num_cores = instance.vcpu
        avail_cores = self.get_available_core_ids()
        if len(avail_cores) < requested_num_cores:
            # * No enough available cores, put request back to the FRONT of the queue.
            self.runqueue.insert(0, instance)
            return False
        else:
            for core in avail_cores[:requested_num_cores]:
                # * Limit the number of cores by the requested quantity.
                self.cpu_registry[core] = instance
            return True

    def yield_cores(self, instance: Instance):
        for core, residence in self.cpu_registry.items():
            if residence == instance:
                self.cpu_registry[core] = None
        return

    def bind(self, func, num):
        """Add a new binding object.

        :param func: {str} Name of the function.
        :param num: {int} Number of instances requested.
        :return: {None}
        """
        self.controller_workqueue.append(
            SchedulingBinding(sim.state.clock.now(), None, func, num)
        )
        return

    def preempt(self, instances: List[Instance], context_switch=False):
        """Does preemption over specified `instances`.
        This is different from the `kill()` method in the following ways:
            1. It doesn't go through the scheduling process via {Scheduler}.
            2. It only removes the `instances` on this node, while the {Scheduler} will
            consider other nodes as well.

        :param instances: {List[Instance]} Instances to remove from the node.
        :param context_switch: {bool} Soft preemption that kicks instances to the run queue instead of killing them.
        :raises RuntimeError: Any preemption target not found.
        :raises RuntimeError: Ended less instances than specified.
        """
        total_matched_instances = 0
        for instance in instances:
            index = self.instances.index(instance)
            if index < 0:
                raise RuntimeError(f"Preemption target not found: {instance=}")
            else:
                total_matched_instances += 1

            if not context_switch:
                # ! Stop the execution, otherwise will cause infinite loop
                instance.stop(preempted=True)
                # ! But do NOT delete instances here. It's the job of the reconciliation loop.
                # self.instances.remove(instance)
            else:
                instance.halt()
                # * Kick the instance out to the `runqueue`.
                # ? Should it be at the front or back?
                self.runqueue.append(instance)

            if not context_switch:
                deadline = (
                    sim.state.clock.now()
                    + hvm_config.PREEMPTION_NOTIFICATION_SEC * 1000
                )

                # # * Manually sync throttler's view.
                # tracker: Throttler._Tracker_ = sim.state.throttler.trackers[instance.func]
                # # * They should share the same set of instances but just in case.
                # throttler_instance = tracker.instances[tracker.instances.index(instance)]
                # throttler_instance.status = InstanceStatus.TERMINATING
                # # ! During the notification period, the job is stopped immediately for now.
                # # TODO: deadline := PREEMPTION_NOTIFICATION_SEC + INSTANCE_GRACE_PERIOD_SEC
                # throttler_instance.deadline = deadline

                instance.status = InstanceStatus.TERMINATING
                instance.deadline = deadline

        if len(instances) != total_matched_instances:
            raise RuntimeError(
                f"{len(instances)-total_matched_instances} preemption targets not found!"
            )
        return

    def kill(self, func, num):
        """Remove instances from the node.

        :param func: {str} Name of the function.
        :param num: {int} Number of instances to remove.
        :return: {int} Remaining function to be removed.
        """
        total_matched_instances = 0

        for instance in self.instances:
            # ! Killing instances is currently NOT balanced across workers.
            if instance.func == func and instance.status == InstanceStatus.IDLE:
                total_matched_instances += 1

        if total_matched_instances == 0:
            # * All instances of this function on this node have gone
            # * -> Clear up staled bindings.
            for binding in self.controller_workqueue:
                if binding.func == func and binding.quantity <= 0:
                    self.controller_workqueue.remove(binding)
            # sim.log.warn(f"No matching instances to delete for {func}")
            return num

        remaining = max(num - total_matched_instances, 0)
        self.bind(
            func,
            -num if remaining == 0 else -total_matched_instances,
        )
        return remaining

    def spawn(self):
        """Creates new instaces."""
        now = sim.state.clock.now()
        if now % 1000 == 0:
            self.num_instances_created_sec = 0

        rate_limit = 3
        if self.num_instances_created_sec >= rate_limit:
            return

        n_created = 0
        for instance in self.creation_queue:
            if now >= instance.start_time:
                n_created += 1
                self.num_instances_created_sec += 1

                self.instances.append(instance)
                tracker: Throttler._Tracker_ = sim.state.throttler.trackers[
                    instance.func
                ]
                tracker.instances.append(instance)
            else:
                # * Instances are in time order, so the following instances won't meet the delay either.
                break

            if self.num_instances_created_sec >= rate_limit:
                break
        # * Update the queue.
        self.creation_queue = self.creation_queue[n_created:]

        # if n_created > 0:
        #     sim.log.info(f"(node) Spawned {n_created} instances on {self.name}", {'clock': now})
        return

    def evict(self):
        """Garbage-collects all expired instances."""
        # ? Maybe control concurrency here as well
        now = sim.state.clock.now()
        if now % 1000 == 0:
            self.num_instances_evicted_sec = 0

        rate_limit = 3
        if self.num_instances_evicted_sec >= rate_limit:
            return

        n_removed = 0
        for instance in self.eviction_queue:
            if now >= instance.deadline:
                n_removed += 1
                self.num_instances_evicted_sec += 1
                tracker: Throttler._Tracker_ = sim.state.throttler.trackers[
                    instance.func
                ]
                self.instances.remove(instance)
                tracker.instances.remove(instance)
            else:
                # * Instances are in time order, so the following instances won't meet the grace period either.
                break

            if self.num_instances_evicted_sec >= rate_limit:
                break
        # * Update the queue.
        self.eviction_queue = self.eviction_queue[n_removed:]

        # if n_removed > 0:
        #     sim.log.info(f"(node) Evicted {n_removed} instances on {self.name}", {'clock': now})
        return

    def is_cold_start(self, func):
        for instance in self.instances:
            if instance.func == func and instance.status == InstanceStatus.RUNNING:
                return False
        return True

    def get_num_available_slots(self):
        # running_instances = [instance for instance in self.instances
        #                      if instance.status == InstanceStatus.RUNNING]
        return min(
            self.max_num_instances,
            max(0, self.max_num_instances - (len(self.instances))),
        )

    def reconcile(self):
        """Control loop (≈ k8s control loops)."""
        now = sim.state.clock.now()

        # * Limit the concurrency of instance creation/deletion.
        instance_creation_budget = self.max_num_instances - len(
            self.instances
        )  # min(self.max_num_instances-len(self.instances), node_config.INSTANCE_CREATION_CONCURRENCY)
        instance_deletion_budget = 100  # node_config.INSTANCE_DELETION_CONCURRENCY

        for binding in self.controller_workqueue:
            if instance_creation_budget <= 0 and instance_deletion_budget <= 0:
                break

            if binding.quantity > 0 and instance_creation_budget > 0:
                """Creating new instances."""

                # * Add CRI delays.
                cri_delay = (
                    node_config.COLD_INSTANCE_CREATION_DELAY_MILLI
                    if self.is_cold_start(binding.func)
                    else node_config.WARM_INSTANCE_CREATION_DELAY_MILLI
                )

                num_new_instances = min(binding.quantity, instance_creation_budget)
                assert num_new_instances > 0
                # * Update creation budget for this round of reconciliation.
                instance_creation_budget -= num_new_instances

                # * Update the binding.
                binding.quantity -= num_new_instances
                if binding.quantity == 0:
                    self.controller_workqueue.remove(binding)

                """Add new instances to creation queue."""
                new_instances = [
                    Instance(
                        node=self,
                        func=binding.func,
                        start_time=now + cri_delay,
                        # * Get the required compute from the central state.
                        vcpu=sim.state.functions[binding.func].vcpu,
                    )
                    for _ in range(num_new_instances)
                ]
                self.creation_queue += new_instances

            elif binding.quantity < 0 and instance_deletion_budget > 0:
                """Taking down instances."""
                # * Control deletion concurrency.
                num_to_terminate = min(-binding.quantity, instance_deletion_budget)
                terminated_instances = []
                deadline = now + node_config.INSTANCE_GRACE_PERIOD_SEC * 1000

                for instance in self.instances:
                    # * Terminate the requested number of idle `func` instances.
                    if (
                        instance.func == binding.func
                        and instance.status == InstanceStatus.IDLE
                    ):
                        assert (
                            instance.hosted_job is None
                        ), f"{instance} hosts a job while being idle!"
                        if len(terminated_instances) < num_to_terminate:
                            instance.status = InstanceStatus.TERMINATING
                            instance.deadline = deadline
                            terminated_instances.append(instance)
                            # sim.log.info(f"(node) Terminate {instance.func=} on {self.name}", {'clock': now})
                        else:
                            # * Terminated enough number of instances.
                            break

                # * Update terminating instances.
                self.eviction_queue += terminated_instances
                # * Update deletioin budget for this round of reconciliation.
                instance_deletion_budget -= len(terminated_instances)

                # # * Manually sync throttler's view.
                # # * They should share the same set of instances but just in case.
                # for instance in terminated_instances:
                #     throttler_instance = tracker.instances[tracker.instances.index(instance)]
                #     throttler_instance.status = InstanceStatus.TERMINATING
                #     throttler_instance.deadline = deadline

                remaining = num_to_terminate - len(terminated_instances)
                if remaining > 0:
                    # ! Add back a NEGATIVE quantity
                    binding.quantity = -remaining
                elif remaining == 0:
                    self.controller_workqueue.remove(
                        binding
                    )  # * Dequeue binding request.
                else:
                    raise RuntimeError("Terminated more instances than requested")

            elif binding.quantity == 0:
                raise RuntimeError("Zero binding object")
        return

    def _compact_cpu_registry(self):
        """Removes holes between cpu slots, making empty entries only at the tail.
        TODO: Could be imporved.
        TODO: Test.
        """
        old_registry = copy.deepcopy(self.cpu_registry)
        self.cpu_registry = OrderedDict({core: None for core in range(self.num_cores)})

        nxt_core = 0
        for _, instance in old_registry.items():
            if instance is not None:
                self.cpu_registry[nxt_core] = instance
                nxt_core += 1
        return

    @property
    def hazard(self):
        """Hazard in *milliseconds*"""
        return self.base_hazard_milli

    def survival_prob(self):
        return 1.0

    def __repr__(self):
        return self.name


class HarvestVM(Node):
    def __init__(
        self,
        name: str,
        num_cores: int,
        memory_mib: int,
        start_time: int,
        hvm_hash: str = None,
        max_num_instances: int = node_config.MAX_NUM_INSTANCES,
        base_hazard_s=hvm_config.BASE_HAZARD,
    ):
        self.start_time = start_time

        # * Simulate a particular HVM if specified.
        self.hvm_hash = sim.FLAGS.hvm if not hvm_hash else hvm_hash
        if self.hvm_hash:
            if not self.hvm_hash in cores_table.keys():
                sim.log.error(f"Harvest VM {self.hvm_hash} not found")
            else:
                sim.log.info(f"(hvm) simulate {self.hvm_hash} from the trace")

        self.cores_schedule = self.get_cores_schedule()

        # * Overwrite the initial cores with values from the trace.
        num_cores = self._get_harvest_core_count()
        super().__init__(name, num_cores, memory_mib, start_time, max_num_instances)

        # * Overwirte base hazard.
        self.base_hazard_milli = base_hazard_s / 1000
        self.cumulative_harzard = 0
        self.kind: WorkerType = WorkerType.HarvestVM

        self.survival_pred_ckp = self.start_time
        self.harvest_ckp = self.start_time

        return

    def die(self):
        # * Preempt all its instances.
        self.preempt(self.instances)
        # * Remove itself from the cluster s.t. no new requests can be scheduled.
        self.cluster.nodes.remove(self)
        return

    def run(self):
        now = sim.state.clock.now()

        """Deciding if to die according to trace."""
        is_dead = False
        if now >= self.survival_pred_ckp + hvm_config.SURVIVAL_PREDICT_PERIOD_MILLI:
            self.survival_pred_ckp = now

            u = sim.rng.uniform(0, 1)
            prob = self.survival_prob()
            if u > prob or self.num_cores == 0:
                # if False:
                sim.log.info(f"(hvm) HarvestVM ({self.name}) died", {"clock": now})
                self.die()
                is_dead = True
            else:
                super().run()
        else:
            super().run()

        """(if alive) Harvesting resources according to trace."""
        if (
            not is_dead
            and hvm_config.ENABLE_HARVEST
            and now >= self.harvest_ckp + hvm_config.HARVEST_PERIOD_MILLI
        ):
            self.harvest()
            # * Update checkpoint for scheduling the next harvest.
            self.harvest_ckp = now
        return

    def get_cores_schedule(self):
        hvm = (
            self.hvm_hash if self.hvm_hash else sim.rng.choice(list(cores_table.keys()))
        )
        return cores_table[hvm]

    def _get_harvest_core_count(self):
        """Get the cores available to the HarvestVM according to the trace."""
        # * Convert to seconds, which is the granularity of the trace.
        lifetime_sec = (sim.state.clock.now() - self.start_time) // 1000
        avail_cores = self.cores_schedule[lifetime_sec % len(self.cores_schedule)]
        return avail_cores

    def survival_prob(self):
        global survival_model
        # * Convert ms to hr.
        return survival_model.predict(
            (sim.state.clock.now() - self.start_time) / 3600_000.0
        )

    def harvest(self):
        harvest_cores = self._get_harvest_core_count()
        diff = harvest_cores - self.num_cores
        if diff == 0:
            return

        if diff > 0:
            sim.log.info(f"(hvm) Grow: {self.num_cores} -> {harvest_cores}")
            """Growing CPU entries."""
            for i in range(len(self.cpu_registry), harvest_cores):
                self.cpu_registry[i] = None
        elif diff < 0:
            sim.log.info(f"(hvm) Shrink: {self.num_cores} -> {harvest_cores}")
            """Shrinking CPU entries."""
            num_cores_to_remove = -diff
            num_available_cores = len(self.get_available_core_ids())
            num_instances_to_preempt = num_cores_to_remove - num_available_cores
            instances_to_preempt = []

            # * Context-switch out jobs to shrink core number.
            if num_instances_to_preempt > 0:
                # * Need to context-switch out some running instances in order to shrink compute size.
                running_instances = [
                    instance
                    for instance in self.cpu_registry.values()
                    if instance is not None
                ]
                assert num_instances_to_preempt <= len(
                    running_instances
                ), f"{num_instances_to_preempt=}>cpu_instances={len(running_instances)}"

                """$$$ Instance eviction policy."""
                instances_to_preempt = sim.rng.choices(
                    running_instances, k=num_instances_to_preempt
                )
                self.preempt(instances_to_preempt, context_switch=True)

            # * Remove preempted instances from CPUs.
            for core, instance in self.cpu_registry.items():
                if instance in instances_to_preempt:
                    self.cpu_registry[core] = None
            # * Move empty registries to the tail.
            self._compact_cpu_registry()
            # * Delete CPUs backword (w/o worrying about the instances
            # * on them, since they have been context-switched out).
            new_cpu_registry = copy.deepcopy(self.cpu_registry)
            for core in range(
                self.num_cores - 1, self.num_cores - 1 - num_cores_to_remove, -1
            ):
                del new_cpu_registry[core]
            self.cpu_registry = new_cpu_registry

        assert len(self.cpu_registry) == harvest_cores
        self.num_cores = harvest_cores
        return

    @property
    def hazard(self):
        return self.hazard_function(sim.state.clock.now())

    # TODO: Add dynamics.
    def hazard_function(self, t):
        return self.base_hazard_milli

    def get_cumulative_harzard(self):
        now = sim.state.clock.now()
        self.cumulative_harzard += self.hazard_function(now)
        return self.cumulative_harzard

    def survival_function(self):
        return math.exp(-self.get_cumulative_harzard())

    def __repr__(self):
        return self.name
