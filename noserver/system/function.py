from dataclasses import dataclass

from .. import simulation as sim

SYSTEM_TAX_MILLI = 5


def get_system_tax(node_cpu_utilisation, node_mem_usage):
    return sim.rng.randint(
        SYSTEM_TAX_MILLI, int(SYSTEM_TAX_MILLI * (100 + node_cpu_utilisation) / 100)
    )


@dataclass
class Request(object):
    flow_id: int
    rps: int
    dest: str
    duration: int
    memory: int
    dag_name: str

    arrival_time: int = None
    start_time: int = None
    end_time: int = None
    # * Accumulated CPU time of this request.
    total_cputime: int = 0
    # * The timestamp at which this request is run -> Compute `cpu_time`
    last_run_ts: int = None
    is_running: bool = False
    failed: bool = False
    num_replicas: int = 1
    req_id: str = None

    def __post_init__(self):
        self.req_id = f"{self.flow_id}-{self.dest}"

    def start(self):
        now = sim.state.clock.now()
        # * The `start_time` has been set when the host (Instance) is halted on the runqueue.
        self.start_time = now if not self.start_time else self.start_time
        # * Update the last timestamp doing work in CPUs.
        self.last_run_ts = now
        self.is_running = True

        # * Register the timestamp of the finishing event.
        # * (this time will be wrong if preemption/halting happens, but in that case, it's just a nop loop).
        # expected_endtime = now + (self.duration-self.total_cputime)
        # sim.state.request_end_times.append(expected_endtime)
        return

    def stop(self, node_cpu_utilization: float, node_mem_usage: float):
        """Stops execution.

        :param node_cpu_utilization: {float} CPU utilization at the ending time.
        :param node_mem_usage: {float} Memory usage at the ending time.
        :raises RuntimeError: Try to finish a request not yet started
        """
        now = sim.state.clock.now()
        # if now == 8003:
        #     print(f"{self.req_id} having 8003 ended {self.start_time=}")
        #     # print(sorted(sim.state.request_finish_times))
        # assert now in sim.state.request_end_times,\
        #     f"Finish time {now} of {self.req_id} ({self.duration=},{self.start_time=},{self.total_cputime=}) was not registered ({sim.state.request_end_times})!"
        # # * Deregister the finish time.
        # sim.state.request_end_times.remove(now)

        # TODO: Try without the following tax stuff.
        system_tax = get_system_tax(node_cpu_utilization, node_mem_usage)
        self.end_time = now + system_tax
        if not self.start_time or self.total_cputime < self.duration:
            # * Probabily was preempted.
            self.failed = True

        # * Free a dependency counter for all its successors.
        sim.state.dereference(self)
        self.is_running = False
        return

    def run(self):
        """Continues the execution.

        :raises RuntimeError: Try to run request not yet started.
        :return: {int} Remaining time.
        """
        if not self.is_running:
            raise RuntimeError(f"Try to run request not yet started!")

        now = sim.state.clock.now()
        self.total_cputime += now - self.last_run_ts
        self.last_run_ts = now
        residual = self.duration - self.total_cputime
        return residual

    def __repr__(self):
        return "Request: " + self.req_id


@dataclass
class Function(object):
    # ^ In the codebase `function` is an instance of `Function`,
    # ^ while `func` usually is just a string literal.
    name: str
    vcpu: int = 1
    concurrency_limit: int = 1

    def __repr__(self):
        return "Function" + repr(vars(self))


class Breaker(object):
    def __init__(self, owner: str, capacity: int):
        self.owner = owner
        self.queue = []
        self.capacity = capacity

    def has_slots(self):
        return self.capacity > len(self.queue)

    def empty(self):
        return len(self.queue) == 0

    def first(self):
        if len(self.queue) > 0:
            return self.queue[0]
        else:
            return None

    def rand(self):
        if len(self.queue) > 0:
            return self.queue[sim.rng.randint(0, len(self.queue) - 1)]
        else:
            return None

    def enqueue(self, request: Request):
        # sim.log.info(f"Enqueue {request.req_id}")

        if len(self.queue) < self.capacity:
            self.queue.append(request)
            return True
        else:
            sim.log.fatal(f"{self.owner} Breaker overload")
            return False

    def dequeue(self, request: Request):
        if request in self.queue:
            # sim.log.info(f"Dequeue {request.req_id}")
            self.queue.remove(request)
        return

    def __next__(self):
        if self.queue:
            request = self.queue[0]
            self.queue = self.queue[1:]
            return request
        else:
            raise StopIteration

    def __iter__(self):
        return self

    def __repr__(self):
        return "Breaker: " + self.owner
