from .function import *

class Autoscaler(object):
    def __init__(self, functions, metric='concurrency', agg_method='average'):
        self.functions = functions
        self.metric = metric
        self.agg_method = agg_method
        
        self.coldstart_latency_milli = 3e3 # Cold start penalty 3s. (warm:  1-2ms)
    
    def schedule(self):
        pass
    
    def update_scales(self):
        pass
    
    def __repr__(self):
        return "Autoscaler" + repr(vars(self))

class Throttler(object):
    
    class _Breaker_():
        def __init__(self, function: Function, lb_policy: str='first-fit'):
            self.lb_policy = lb_policy # Centralised LB.
            self.function = function
            self.inflight = 0
            
        def __repr__(self):
            return "_Breaker_" + repr(vars(self))
        
        def hit(self, request):
            self.inflight += 1
            if self.function.get_slots() < self.inflight:
                ## No available slots. 
                return True
            else:
                #TODO: Add latency
                for instance in self.function.instances:
                    instance.reserve(request)
                return False
    
    def __init__(self, functions):
        self.trackers = {func.name: self._Breaker_(func) for func in functions}
        
        self.shared_queue = []
        self.capacity = 10_000
    
    def enqueue(self, request):
        if len(self.shared_queue) > self.capacity:
            # Throw for now
            raise RuntimeError('503 Throttler queue full')
        else:
            # TODO: Dequeue!
            self.shared_queue.append(request)
            return self.trackers[request.dest].hit(request)
            
            
    def __repr__(self):
        return "Throttler" + repr(vars(self))