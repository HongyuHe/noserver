import sys
sys.path.append("..") 

from .components import *
from simulation import *

class Cluster(object):
    def __init__(self, nodes, functions, scheduling_policy='bin-packing'):
        self.nodes = nodes
        self.functions = functions
        self.throttler = Throttler(functions)
        self.autoscaler = Autoscaler(functions)
        
        self.routes = {func.name:'throttle' for func in self.functions} # func_name: <direct> | <throttle>
        self.scheduling_latency_milli = 10 # 10ms per e2e placement.
    
    def accept(self, request):
        if self.routes[request.dest] == 'throttle':
            log.info(f"Throttle {request}")
            
            is_cold = self.throttler.enqueue(request)
            if is_cold:
                log.info("Proxy mode")
            else:
                log.info("[Not implemented] Serve mode")
        else:
            log.info(f"Route {request}")

    def __repr__(self):
        return "Cluster" + repr(vars(self))     
            
class Node(object):
    def __init__(self, num_cores, memory_mib, instance_limit=200):
        self.num_cores = num_cores
        self.memory_mib = memory_mib
        self.instance_limit = instance_limit
        
        self.instances = []
        self.memory_mib_usage = 0
        self.cpu_usage = 0

    def __repr__(self):
        return "Node" + repr(vars(self))