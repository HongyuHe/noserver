from .function import *
from typing import *

class Autoscaler(object):
    def __init__(self, functions, metric='concurrency', agg_method='average'):
        self.functions = functions
        self.metric = metric
        self.agg_method = agg_method
        
        self.coldstart_latency_milli = 3e3 # Cold start penalty 3s. (warm:  1-2ms)
    
    def update_scales(self):
        pass

    def coldstart(self, func: Function):
        pass 
        # TODO: Invoke InstanceEngine!

    
    def __repr__(self):
        return "Autoscaler" + repr(vars(self))

class Throttler(object):
    def __init__(self, functions: List[Function]):
        self.breaker = Breaker('Throttler', 10_000)
        self.trackers = {func.name: self._Tracker_(func) for func in functions}
            
    def handle(self, request: Request):
        log.info(f"Handle {request}")
        tracker = self.trackers[request.dest]
        tracker.breaker.enqueue(request)

        if len(tracker.instances) == 0:
            log.info(f"Cold start occurred on {request.dest}")
            # TODO: Poke Autoscaler!
        
        processed = False
        for instance in tracker.instances:
            processed = instance.reserve(request)
        
        if not processed:
            log.warn(f"No capacity to process {request}")   


            
    def __repr__(self):
        return "Throttler" + repr(vars(self))
    
    class _Tracker_(object):
        def __init__(self, func: Function):
            self.breaker = Breaker(f"_Tracker_::{func.name}", 10_000)
            self.function = func
            self.instances: List[Instance] = []
        def __repr__(self):
            return "_Tracker_" + repr(vars(self))


class Breaker(object):
    def __init__(self, owner: str, capacity: int):
        self.owner = owner
        self.queue = []
        self.capacity = capacity

    def enqueue(self, request: Request):
        log.info(f"Enqueue {request}")

        if len(self.queue) < self.capacity:
            self.queue.append(request)
        else:
            log.fatal(f"{self.owner} Breaker overload")
    
    def dequeue(self, request: Request):
        log.info(f"Dequeue {request}")
        self.queue.remove(request)

    def __repr__(self):
        return "Breaker" + repr(vars(self))

class Scheduler(object):
    def __init__(self):
        pass

    def __repr__(self):
        return "Scheduler" + repr(vars(self))