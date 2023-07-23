from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .worker import Node

from enum import Enum
import datetime

from .. import simulation as sim
from .function import *


cluster_config = sim.FLAGS.config.cluster


class InstanceStatus(Enum):
    UNKNOWN = 0
    IDLE = 1
    HALTED = 2
    RUNNING = 3
    TERMINATING = 4


class Instance(object):
    def __init__(self, func: str, node: Node, start_time=None, vcpu=1):
        # ^ `func` here is NOT an `Instance` but a string.
        self.func: str = func
        self.node: Node = node
        self.vcpu = vcpu

        self.deadline = None
        self.hosted_job = None

        self.start_time = sim.state.clock.now() if start_time is None else start_time
        # * Start with unkown status to be discovered.
        self.status: InstanceStatus = InstanceStatus.IDLE
        self.discovery_ckp = self.start_time
        """Constants"""
        self.capacity = 1  # self.concurrency_limit
        self.breaker = Breaker(f"Instance {self.func}", self.capacity)

    def serve(self, request: Request):
        """Books CPU resources for the `request`.
        :param request: Request to be serviced.
        """
        self.hosted_job = request
        # * Regardless of whether there's sufficient CPU, the instance is running
        # * as it's hosting a request.
        self.status = InstanceStatus.RUNNING
        successful = self.node.book_cores(self)
        if successful:
            if not request.is_running:
                request.start()
            sim.log.info(
                f"(instance) Serving {request.req_id} on {self.node.name}",
                {"clock": sim.state.clock.now()},
            )
        return

    def reserve(self, request: Request):
        assert (
            request.dest == self.func
        ), f"(instance) Destination mismatch for {self.func}!"

        if self.status == InstanceStatus.TERMINATING:
            return False

        elif self.status == InstanceStatus.IDLE and self.breaker.has_slots():
            # assert not self.breaker.queue, f"(instance) Idling with requests in queue!"

            self.breaker.enqueue(request)
            self.serve(request)
            return True

        elif self.status == InstanceStatus.RUNNING and self.breaker.has_slots():
            # ! Currently, this would never happen since the local queue length is 1 (only for the hosted job).
            sim.log.info(f"(instance) Reserved a slot for {request.req_id}")
            self.breaker.enqueue(request)
            return True
        else:
            # sim.log.info("No free slots")
            return False

    def run(self):
        """Continues the hosted job."""
        if self.hosted_job is not None:
            assert (
                self.status == InstanceStatus.RUNNING
            ), f"Instance hosting a job while being {self.status}"

            request: Request = self.hosted_job
            if not request.is_running:
                # * Try to book the node (again).
                self.serve(self.hosted_job)
            else:
                residual = request.run()
                if residual <= 0:
                    self.stop()

        # * Update the status of the instance only after a certain delay.
        elif self.status == InstanceStatus.UNKNOWN:
            if (
                sim.state.clock.now() - self.discovery_ckp
                > cluster_config.DISCOVERY_DELAY_MILLI
            ):
                self.status = InstanceStatus.IDLE

        elif self.status == InstanceStatus.IDLE:
            # * Load the next job (None if the queue is empty).
            next_request = self.breaker.first()
            if next_request is not None:
                self.serve(next_request)
        return

    def stop(self, preempted=False):
        self.node.yield_cores(self)

        for i, request in enumerate(self.breaker):
            request: Request = request
            if i == 0 and not preempted:
                assert (
                    request == self.hosted_job
                ), f"(instance) 1st request ({request.req_id}) in the queue is not the hosted job ({self.hosted_job.req_id})!"

            request.stop(*self.node.get_utilizations())
            self.breaker.dequeue(request)
            self.node.cluster.drain(self.node, request)

            if not request.failed:
                sim.log.info(
                    f"(instance) Finished {request.req_id} (duration={datetime.timedelta(seconds=request.duration/1000)})",
                    {"clock": sim.state.clock.now()},
                )
            else:
                sim.log.info(
                    f"(instance) Failed {request.req_id}",
                    {"clock": sim.state.clock.now()},
                )

            # ! If preempted, continue stopping ALL requests sent to this instance.
            if not preempted:
                break

        self.hosted_job = self.breaker.first()
        if self.hosted_job is None:
            # ! Set to `UNKNOWN` that models the sync delay.
            self.status = InstanceStatus.UNKNOWN
            self.discovery_ckp = sim.state.clock.now()
        else:
            # * The newly hosted job will start in the next `run()`.
            # * (is has to book cores again.)
            self.status = InstanceStatus.RUNNING
        return

    def halt(self):
        # ! Let the instance remain running.
        # TODO: Use `HALTED` status?
        if self.status == InstanceStatus.RUNNING:
            self.node.yield_cores(self)
            # * Stop hosted job.
            self.hosted_job.is_running = False

            sim.log.info(
                f"(instance) Halted ({self.hosted_job.req_id})",
                {"clock": sim.state.clock.now()},
            )
        return

    def __repr__(self):
        return "Instance-" + self.func
