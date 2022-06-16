import logging as log


log.basicConfig(
    level=log.INFO,
    format='(%(asctime)s) [%(levelname)s] %(message)s'
)

class Clock(object):
    def __init__(self):
        self.time_milli = 0
        
    def inc(self, duration):
        self.time_milli += duration
    
    def now(self):
        return self.time_milli
    
    def __repr__(self):
        return "Clock" + repr(vars(self))