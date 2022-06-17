import sys
sys.path.append("..") 
import simulation as sim

from .components import AUTOSCALING_PERIOD_MILLI, Throttler, Autoscaler, StateMachine
from .function import Function, Instance, Request

from typing import *

CRI_ENGINE_PULLING = int(1000/100) # * kubelet QPS of our the 500-node cluster.
INSTANCE_SIZE_MIB = 400 # TODO: size of firecracker.
INSTANCE_CREATION_DELAY_MILLI = 1000

class Node(object):
    def __init__(self, name, num_cores, memory_mib, instance_limit=200):
        self.name = name
        self.num_cores = num_cores
        self.memory_mib = memory_mib
        self.instance_limit = instance_limit
        
        self.instances = []
        self.backlog = []

    def get_available_slots(self):
        return self.instance_limit - len(self.instances)
        # min(
        #     self.instance_limit - len(self.instances),
        #     # # TODO: Estimate based on the actual requests.
        #     # (self.memory_mib - len(self.instances)*INSTANCE_SIZE_MIB) / INSTANCE_SIZE_MIB, 
        # )
    
    def accept(self, func, num):
        self.backlog.append({
            'time': sim.state.clock.now(),
            'func': func,
            'quantity': num,
        })
        # sim.log.info(f"{self.backlog=}")
        return
    
    def reconcile(self):
        if len(self.backlog) == 0: return
        # * Order by request time.
        self.backlog = sorted(self.backlog, key=lambda r: r['time']) 
        request = self.backlog[0] # * Only handle the first.

        if sim.state.clock.now() - request['time'] < INSTANCE_CREATION_DELAY_MILLI:
            # * CRI engine delay has not been fulfilled.
            return
        
        self.instances.append(request['func'])
        self.backlog.remove(request)
        # * Assume one engine can only create one pod at a time.
        if request['quantity'] > 1:
            request['quantity'] -= 1
            self.backlog.append(request)
        
        # TODO: Add instance to pod tracker (latency!!!)
        tracker: Throttler._Tracker_ = sim.state.throttler.trackers[request['func']]
        tracker.instances.append(
            Instance(func=request['func'], node=self.name),
        )
        return
        
        

    def __repr__(self):
        return "Node" + repr(vars(self))

class Cluster(object):
    def __init__(self, clock: sim.Clock, nodes: List[Node], functions: List[Function],):
        self.nodes = nodes
        # self.functions = functions
        self.throttler = Throttler(functions)
        self.autoscaler = Autoscaler(functions)

        self.scheduler = Scheduler(nodes, functions)

        sim.state = StateMachine(self.autoscaler, self.throttler, clock)
    
    def accept(self, request: Request):
        self.throttler.handle(request)
    
    def reconcile(self):
        # pass
        for func, scaler in self.autoscaler.scalers.items():
            self.scheduler.reconcile(
                    # (func, # of required instances)
                    func, scaler.desired_scale - scaler.actual_scale)
            for node in self.nodes:
                node.reconcile()

    def __repr__(self):
        return "Cluster" + repr(vars(self))   


class Scheduler(object):
    def __init__(self, nodes: List[Node], functions: List[Function]):
        # TODO: ordering
        self.backlog: Dict[str: int] = {func.name: 0 for func in functions}
        self.nodes = nodes
        
    def reconcile(self, func, scale):
        old_scale = self.backlog[func]
        if old_scale != scale:
            self.backlog[func] = scale
            sim.log.info(f"{self.backlog=}")
        
        self.schedule()
        return

    def schedule(self):
        # TODO: bin-packing (best-fit).
        for func, num in self.backlog.items():
            if num == 0: continue
            # ! Potential deadlock.
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

            self.backlog[func] = 0
        return  
    
    def __repr__(self):
        return "Scheduler" + repr(vars(self)) 
