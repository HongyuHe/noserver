from lib2to3.pytree import Node
import sys
sys.path.append("..") 

from simulation import *
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
    def __init__(self, func, node: Node):
        self.func = func
        self.node = node
        # self.memory_mib = memory_mib
        # self.duration_milli = duration

        self.idle = True
        self.job_start = 0
        self.age_milli = 0

        self.capacity = 10 + 1 # self.concurrency_limit
        self.breaker = Breaker(f"Instance {self.func}", self.capacity)
    
    def reserve(self, request: Request):
        if not self.breaker.has_slots():
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

class Breaker(object):
    def __init__(self, owner: str, capacity: int):
        self.owner = owner
        self.queue = []
        self.capacity = capacity

    def has_slots(self):
        return self.capacity > len(self.queue)
        
    def enqueue(self, request: Request):
        log.info(f"Enqueue {request.dest}")

        if len(self.queue) < self.capacity:
            self.queue.append(request)
        else:
            log.fatal(f"{self.owner} Breaker overload")
    
    def dequeue(self, request: Request):
        log.info(f"Dequeue {request}")
        self.queue.remove(request)

    def __repr__(self):
        return "Breaker" + repr(vars(self))