import sys
sys.path.append("..") 

from .components import *
from simulation import *

class Cluster(object):
    def __init__(self, nodes, functions: Function, scheduling_policy='bin-packing'):
        self.nodes = nodes
        self.functions = functions
        self.throttler = Throttler(functions)
        self.autoscaler = Autoscaler(functions)
    
    def accept(self, request: Request):
        self.throttler.handle(request)

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

class InstanceEngine(object):
    def __init__(self):
        pass

    def __repr__(self):
        return "InstanceEngine" + repr(vars(self))