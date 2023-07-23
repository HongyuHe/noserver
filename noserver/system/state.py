import networkx as nx
from typing import *

from .. import simulation as sim
from .function import *
from .instance import *
from .autoscaler import Autoscaler
from .throttler import Throttler

request_config = sim.FLAGS.config.request


class State(object):
    def __init__(
        self,
        functions: List[Function],
        autoscaler: Autoscaler,
        throttler: Throttler,
        clock: sim.Clock,
        dags: Dict[str, nx.DiGraph] = None,
    ):
        self.functions = {function.name: function for function in functions}
        self.autoscaler: Autoscaler = autoscaler
        self.throttler: Throttler = throttler
        self.clock = clock
        self.dags = dags
        self.rps = 0

        self.flows: Dict[int, State._Flow_] = {}
        self.released_requests = Breaker(owner="State", capacity=int(1e6))
        self.finished_requests = []
        self.failed_requests = []

        # * The times of request-finishing events.
        self.request_end_times = []

    def add_flow(self, flow_id: int, dag: nx.DiGraph):
        self.flows[flow_id] = self._Flow_(dag)
        return

    def dereference(self, request: Request):
        sim.log.info(
            f"(state) Dereferenced {request.req_id}", {"clock": sim.state.clock.now()}
        )
        # ! Make DAG mode a must.
        if not self.dags:
            return

        if not request.flow_id in self.flows.keys():
            # * Address double-deletion in case of parallel invocations.
            # *  A failed request will cause the entire flow to be deleted.
            # *  In the mean time, when some inflight requests finished/failed,
            # *  they will try to use the `flow_id` of the deleted flow again.
            return

        if request.failed:
            sim.log.info(f"(state) {request.req_id} failed.")
            # ! Do NOT mark failed requests as finished.
            self.failed_requests.append(request.req_id)
            # * Check if this's the last chance of execution before deleting the flow.
            if (
                self.failed_requests.count(request.req_id)
                + self.finished_requests.count(request.req_id)
                == request.num_replicas
            ):
                # * Delete the flow of the failed request.
                del self.flows[request.flow_id]
            return
        else:
            self.finished_requests.append(request.req_id)

        # if self.finished_requests.count(request.req_id) > 1:
        #     # * It was a redundant execution.
        #     return
        # else:
        #     # * Mark request as finished to prevent double deref.
        #     self.finished_requests.append(request.req_id)

        flow = self.flows[request.flow_id]
        dag: nx.DiGraph = self.dags[request.dag_name]

        # if set(flow.leaves).issubset(set(self.finished_requests)):
        # ! Do not use the below since there could be released requests still queued/in flight.
        if sum(flow.counters.values()) == 0:
            # * Delete completed flow for more efficient checking of finishing condition.
            del self.flows[request.flow_id]

        successors = (
            dag.successors(request.dest)
            if sim.FLAGS.mode != "trace"
            else dag.successors(f"F{request.dest.split('F')[1]}")
        )
        for successor in successors:
            # * Free a dependency counter for all the successors.
            flow.counters[successor] -= 1

            # * If all predecessors have finished (dereferenced), enqueue the subsequent request.
            if flow.counters[successor] == 0:
                self.released_requests.enqueue(
                    Request(
                        flow_id=request.flow_id,
                        dag_name=request.dag_name,
                        arrival_time=self.clock.now(),
                        rps=-999,  # ! Don't know the actual rps but shouldn't matter here.
                        dest=successor
                        if sim.FLAGS.mode != "trace"
                        else f"{dag.nodes[successor]['dag_name']}-{successor}",
                        duration=min(
                            dag.nodes[successor]["duration_milli"],
                            request_config.MAX_DURATION_SEC * 1000,
                        ),
                        memory=dag.nodes[successor]["memory_mib"],
                    )
                )
        return

    @dataclass(init=False)
    class _Flow_(object):
        # * Number of dependencies used for duplicated execution.
        num_dependencies: int
        # * Counters: function -> # of unfinished predecessors,
        # * i.e., dependences that needed to finish before lauching this function.
        # * (similar to https://dl.acm.org/doi/10.1145/3503222.3507717)
        counters: Dict[str, int]

        leaves: List[str] = None

        def __init__(self, dag: nx.DiGraph):
            self.counters = {
                func: len(list(dag.predecessors(func))) for func in dag.nodes
            }
            self.leaves = {x for x in dag.nodes() if dag.out_degree(x) == 0}
            self.num_dependencies = dag.number_of_edges()

        def get_completion_rate(self):
            num_unfinished = sum(self.counters.values())
            return (
                (self.num_dependencies - num_unfinished) / self.num_dependencies
                if self.num_dependencies > 0
                else 0
            )
