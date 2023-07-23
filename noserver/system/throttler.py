from typing import *
from dataclasses import replace

from .. import simulation as sim
from .function import *
from .instance import *
from ..policy import loadbalance


policy_config = sim.FLAGS.config.policy


class Throttler(object):
    def __init__(self, functions: List[Function]):
        # * Centralized queue.
        self.breaker = Breaker("Throttler", 10_000)
        # * One tracker for each function.
        self.trackers: Dict[str, Throttler._Tracker_] = {
            function.name: self._Tracker_(function) for function in functions
        }

    def handle(self, request: Request):
        # * Only try the instances of the destination.
        tracker = self.trackers[request.dest]
        lb_policies = {
            "first_available": loadbalance.first_available,
            "least_loaded": loadbalance.least_loaded,
        }
        policy = policy_config.LOAD_BALANCE
        assert policy in lb_policies.keys(), f"{policy} not supported!"

        return lb_policies[policy](tracker, request)

    def hit(self, request: Request):
        tracker = self.trackers[request.dest]
        tracker_has_capacity = tracker.breaker.has_slots()

        reexec = False
        completion_rate = (
            sim.state.flows[request.flow_id].get_completion_rate()
            if request.flow_id in sim.state.flows.keys()
            else 1
        )  # * If the flow has been deleted, it means all requests have been fired.

        if (
            policy_config.DUP_EXECUTION
            and completion_rate >= policy_config.DUP_EXECUTION_THRESHOLD
        ):
            sim.log.info(
                f"(throttler) Re-execute {request.req_id} ({completion_rate=})"
            )
            request.num_replicas = 2
            reexec = True

        if tracker_has_capacity:
            tracker.breaker.enqueue(request)
            if reexec:
                # ! Enqueue a copy NOT the original request.
                tracker.breaker.enqueue(replace(request))
        else:
            # * Overflow to the centralized queue.
            self.breaker.enqueue(request)
            if reexec:
                self.breaker.enqueue(replace(request))

        # ! NB: update_concurrency() is not used since requests could overflow to the throttler queue.
        tracker.inc_concurrency()

        if len(tracker.instances) == 0:
            sim.log.info(
                f"Cold start occurred on {request.req_id}",
                {"clock": sim.state.clock.now()},
            )
            sim.state.autoscaler.poke(request)

        dispatched = self.handle(request)

        if dispatched:
            sim.log.info(
                f"(throttler) Dispatched {request.req_id}.",
                {"clock": sim.state.clock.now()},
            )
            tracker.dec_concurrency()
            if tracker_has_capacity:
                tracker.breaker.dequeue(request)
            else:
                self.breaker.dequeue(request)
        else:
            sim.log.info(
                f"(throttler) No compute slots to dispatch; {request.req_id} queued.",
                {"clock": sim.state.clock.now()},
            )

        return

    def dispatch(self):
        """$$$ FIFO dispatching (i.e., no priority among queued requests)."""
        # * Constantly trying to dispatch accumulated requests in both the central queue and the tracker queues.
        # * Only dispatch the top request on the queue.
        assert (
            self.breaker.empty()
        ), "There are requests overflowed to the central queue!"
        # request = self.breaker.first()
        # if request is not None:
        # # ! Loop over the `queue`s instead of the `breaker`s themselves, which are generators.
        # # for request in self.breaker.queue:
        #     # sim.log.info(f"Try to dispatch {request}", {'clock': sim.state.clock.now()})
        #     dispatched = self.handle(request)
        #     if dispatched:
        #         self.breaker.dequeue(request)

        for _, tracker in self.trackers.items():
            # ! Loop over the `queue`s instead of the `breaker`s themselves, which are generators.
            # ! Otherwise, the items are dequeued as been itereated through.
            # ! Do NOT `enqueue()` them back while iterating, which causes inf loop as
            # ! the iterator changes duration iteration.
            dispatched = False
            for request in tracker.breaker.queue:
                # request = tracker.breaker.first()
                if request is not None:
                    # for request in tracker.breaker.queue:
                    # sim.log.info(f"Try to dispatch {request.req_id}", {'clock': sim.state.clock.now()})
                    if self.handle(request):
                        tracker.breaker.dequeue(request)
                        dispatched = True
                    else:
                        # * No need to check other queued requests if the 1st one is not dispatched.
                        continue
        return dispatched

    def record_concurrencies(self):
        """Update the snapshots of queue length for each function tracker."""
        # TODO: Cut off by window length to reduce memory overhead.
        for func, tracker in self.trackers.items():
            overflowed = self.breaker.queue.count(func)
            tracker.update_concurrency(overflowed=overflowed)
        return

    def __repr__(self):
        return "Throttler" + repr(vars(self))

    class _Tracker_(object):
        def __init__(self, func: Function):
            self.breaker = Breaker(f"_Tracker_::{func.name}", 10_000)
            self.function = func
            self.instances: List[Instance] = []
            self.concurrencies = [0]

        def get_scale(self):
            running_instances = 0
            for instance in self.instances:
                # * Only exclude UNKNOWN instances.
                if instance.status in [InstanceStatus.RUNNING, InstanceStatus.IDLE]:
                    running_instances += 1
            return running_instances

        def update_concurrency(self, overflowed=0):
            # TODO: Cut the concurrency record by the window size.
            # * (could become too large when simulating for a long time)
            self.concurrencies.append(len(self.breaker.queue) + overflowed)
            return

        def inc_concurrency(self):
            self.concurrencies[-1] += 1
            sim.log.info(
                f"(throttler) Concurrency inc to {self.concurrencies[-1]}",
                {"clock": sim.state.clock.now()},
            )
            return

        def dec_concurrency(self):
            self.concurrencies[-1] -= 1
            sim.log.info(
                f"(throttler) Concurrency dec to {self.concurrencies[-1]}",
                {"clock": sim.state.clock.now()},
            )
            return

        def __repr__(self):
            return "_Tracker_" + repr(vars(self))
