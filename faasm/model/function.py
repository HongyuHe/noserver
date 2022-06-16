import sys
sys.path.append("..") 

from simulation import *
from .components import *

class Request(object):
    def __init__(self, timestamp, dest, duration, memory):
        self.start = timestamp
        self.dest = dest
        self.duration = duration
        self.memory = memory
        
        self.end = None
        
    def __repr__(self):
        return "Request" + repr(vars(self))
class Function(object):
    def __init__(self, name, concurrency_limit=1, lb_policy='round-robin'):
        self.name = name
        self.concurrency_limit = concurrency_limit
        self.lb_policy = lb_policy # Envoy proxy LB (service-level).
        
        self.instances = []
    
    def __repr__(self):
        return "Funciton" + repr(vars(self))
    
    def get_slots(self):
        available_slots = 0
        for instance in self.instances:
            available_slots += instance.capacity - len(instance.request_queue)
        return available_slots
    
    def get_scale(self):
        return len(self.instances)

class Instance(object):
    def __init__(self, name, duration, memory_mib):
        self.name = name
        self.memory_mib = memory_mib
        self.duration_milli = duration

        self.idle = True
        self.job_start = 0
        self.age_milli = 0

        self.capacity = 10 + self.concurrency_limit
        self.breaker = Breaker(self.name, self.capacity)
    
    def reserve(self, request: Request):
        if len(self.request_queue) > self.capacity:
            log.info("No free slots")
            return False
        else:
            if self.idle:
                #TODO: Dequeue + run()
                self.idle = False
                self.serve(request)
            else:
                log.info("Reserved a slot")
                self.breaker.enqueue(request)
            
            return True
    
    def serve(self, request: Request):
        #TODO
        log.info(f"[Not implemented] Serving {request}")

    
    def __repr__(self):
        return "Instance" + repr(vars(self))