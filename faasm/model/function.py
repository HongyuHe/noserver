import sys
sys.path.append("..")
import simulation as sim

import random

class Request(object):
    def __init__(self, index, timestamp, rps, dest, duration, memory):
        self.index = index
        self.arrival = timestamp
        self.rps = rps
        self.dest = dest
        self.duration = duration
        self.memory = memory

        self.start_time = None
        self.end_time = None
        self.is_running = False

    def start(self):
        self.start_time = sim.state.clock.now()
        self.is_running = True
        return

    def stop(self):
        if not self.is_running:
            raise RuntimeError(f"Try to finish request not yet started!")

        self.end_time = sim.state.clock.now()
        self.is_running = False
        return

    def run(self):
        if not self.is_running:
            raise RuntimeError(f"Try to run request not yet started!")
        sofar = sim.state.clock.now() - self.start_time
        residual = self.duration - sofar
        return residual

    def __repr__(self):
        return "Request" + repr(vars(self))


class Function(object):
    def __init__(self, name, concurrency_limit=1):
        self.name = name
        self.concurrency_limit = concurrency_limit

    def __repr__(self):
        return "Function" + repr(vars(self))


class Instance(object):
    def __init__(self, func, node):
        self.func = func
        self.node = node
        # self.memory_mib = memory_mib
        # self.duration_milli = duration

        self.idle = True
        self.hosted_job = None
        self.start_time = sim.state.clock.now()

        self.terminating = False
        self.deadline = None

        self.capacity = 2  # self.concurrency_limit
        self.queuepoxy = Breaker(f"Instance {self.func}", self.capacity)

    def serve(self, request: Request):
        self.idle = False
        self.hosted_job = request
        has_spare_core = self.node.book_core(self)
        if has_spare_core:
            if not request.is_running:
                request.start()
            sim.log.info(f"Instance: serving {request} on {self.node.name}")
        # else:
            # sim.log.info(f"Instance: {self.node.name} does not have spare cores")
        return

    def reserve(self, request: Request):
        if request.dest != self.func:
            raise RuntimeError("Destination mismatch!")

        if self.terminating:
            return False

        if self.idle:
            self.serve(request)
            self.queuepoxy.enqueue(request)
            return True
        else:
            if self.queuepoxy.has_slots():
                sim.log.info("Reserved a slot")
                # request.duration *= 0.9
                self.queuepoxy.enqueue(request)
                return True
            else:
                # sim.log.info("No free slots")
                return False

    def run(self):
        if self.hosted_job is not None:
            request: Request = self.hosted_job
            if not request.is_running:
                # * Try to book the node (again).
                self.serve(self.hosted_job)
            else:
                residual = request.run()
                if residual <= 0:
                    self.finish(request)

        if self.idle and not self.terminating:
            # * Load the next job (None if there the queue is empty).
            next_request = next(self.queuepoxy.first())
            if next_request is not None:
                # self.hosted_job = next_request
                self.serve(next_request)
        return

    def finish(self, request: Request):
        request.stop()
        self.queuepoxy.dequeue(request)
        self.node.yield_core(self, request)
        self.hosted_job = None
        self.idle = True

        sim.log.info(f"Finished {request}", {'clock': sim.state.clock.now()})
        return

    def __repr__(self):
        return "Instance" + repr(vars(self))


class Breaker(object):
    def __init__(self, owner: str, capacity: int):
        self.owner = owner
        self.queue = []
        self.capacity = capacity

    def has_slots(self):
        return self.capacity > len(self.queue)

    def first(self):
        if len(self.queue) > 0:
            yield self.queue[0]
        else:
            yield None

    def rand(self):
        if len(self.queue) > 0:
            yield self.queue[random.randint(0, len(self.queue)-1)]
        else:
            yield None

    def enqueue(self, request: Request):
        # sim.log.info(f"Enqueue {request.dest}")

        if len(self.queue) < self.capacity:
            self.queue.append(request)
            return True
        else:
            sim.log.fatal(f"{self.owner} Breaker overload")
            return False

    def dequeue(self, request: Request):
        if request in self.queue:
            # sim.log.info(f"Dequeue {request.dest}")
            self.queue.remove(request)
        return

    def __repr__(self):
        return "Breaker" + repr(vars(self))
