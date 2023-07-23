from __future__ import annotations
from typing import *

if TYPE_CHECKING:
    from ..system.throttler import Throttler
    from ..system.cluster import Node
    from ..system.instance import Instance, InstanceStatus

from .. import simulation as sim
from ..system.function import Request


def first_available(tracker: Throttler._Tracker_, request: Request):
    """LB: First available"""
    for instance in tracker.instances:
        reserved = instance.reserve(request)
        if reserved:
            return True
    return False


def least_loaded(tracker: Throttler._Tracker_, request: Request):
    """LB: Least loaded

    :param tracker: _description_
    :param request: _description_
    :return: _description_
    """
    # * First, find the least-loaded (LL) node.
    func_nodes = set()
    for instance in tracker.instances:
        func_nodes.add(instance.node)
    func_nodes: List[Node] = list(func_nodes)
    # TODO: Use better metrics than just cpu + mem.
    func_nodes.sort(key=lambda node: sum(node.get_utilizations()))
    func_nodes.sort(key=lambda node: len(node.runqueue))
    ll_node = func_nodes[0] if func_nodes else None  # * Could be a cold start.

    # * Then, try to get an idle instance on the LL node.
    idle_instance = None
    for instance in tracker.instances:
        if instance.status == InstanceStatus.IDLE and instance.node == ll_node:
            idle_instance = instance
            break

    # * Finally, try to dispatch it.
    if idle_instance:
        reserved = idle_instance.reserve(request)
        assert (
            reserved == True
        ), "(loadbalance) Failed to reserve a spot on an available instance!"
        return True
    else:
        # * No idle instance available even on the LL node -> then every node is the same (it's gonna be cold start anyway)
        # ! No need to check instance-level queueues, because the instances all have a queue length of 1 (same as AWS).
        for instance in tracker.instances:
            reserved = instance.reserve(request)
            if reserved:
                sim.log.info(
                    f"(loadbalance) Dispatched {request.req_id}",
                    {"clock": sim.state.clock.now()},
                )
                return True
    return False
