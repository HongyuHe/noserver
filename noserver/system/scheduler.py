from typing import *

from .. import simulation as sim
from .worker import Node


class Scheduler(object):
    def __init__(self, nodes: List[Node]):
        self.nodes = nodes

    def schedule(self, func, num):
        """Binding new function instances to nodes.

        :param func: {str} Name of the function to be scheduled.
        :param num: {int} Number of instances requested.
        :return: {int} Remaining unscheduled quantity.
        """
        # TODO: Implement k8s bin-packing (best-fit).
        # TODO: Extract policy choice.
        total_nodes = len(self.nodes)
        # sim.rng.shuffle(self.nodes)
        i = sim.rng.randint(0, total_nodes - 1)
        worst_case = abs(num) * total_nodes
        attempts = 0
        # binding_decisions = {node.name:0 for node in self.nodes}

        if num > 0:
            """
            Bind new instances to nodes
            """
            while attempts < worst_case:
                node = self.nodes[i % total_nodes]
                if node.get_num_available_slots() > 0:
                    node.bind(func, 1)
                    num -= 1
                if num == 0:
                    break
                attempts += 1
                i += 1
            # if num > 0:
            #     sim.log.info(f"(scheduler) {num} unscheduled new instances", {'clock': sim.state.clock.now()})
            return num
        else:
            """
            Destroy instances
            """
            quantity = -num
            while attempts < worst_case:
                node = self.nodes[i % total_nodes]
                remaining = node.kill(func, 1)
                if remaining == 0:
                    quantity -= 1
                if quantity == 0:
                    break
                attempts += 1
                i += 1
            # if quantity > 0:
            #     sim.log.info(f"(scheduler) {quantity} instances remain to be removed", {'clock': sim.state.clock.now()})
            return quantity

    def __repr__(self):
        return "Scheduler" + repr(vars(self))
