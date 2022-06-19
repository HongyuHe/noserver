import sys

sys.path.append("..")
import simulation as sim

from .function import *
from typing import *

import math

PANIC_WINDOW_SEC = 6
STABLE_WINDOW_SEC = 60
AUTOSCALING_PERIOD_MILLI = 2000
MAX_SCALE_UP_RATE = 1000
MAX_SCALE_DOWN_RATE = 2
PANIC_THRESHOLD_PCT = 200


class Autoscaler(object):
    def __init__(self, functions: List[Function]):
        self.scalers: Dict[str: Autoscaler._Scaler_] = {
            func.name: self._Scaler_(func) for func in functions
        }

    def poke(self):
        self.evaluate(is_poked=True)
        return

    def evaluate(self, is_poked=False):
        for func, tracker in sim.state.throttler.trackers.items():
            concurrencies: List[int] = tracker.concurrencies
            actual_scale = tracker.get_scale()
            ready_pod = actual_scale if actual_scale != 0 else 1  # https://github.com/knative/serving/blob/main/pkg/autoscaler/scaling/autoscaler.go#L151
            cc_target: float = tracker.function.concurrency_limit

            max_up_scale = math.ceil(MAX_SCALE_UP_RATE * ready_pod)  # https://github.com/knative/serving/blob/main/pkg/autoscaler/scaling/autoscaler.go#L180
            max_down_scale = math.floor(ready_pod / MAX_SCALE_DOWN_RATE)

            # * NB: This is plane averaging without bucketing.
            # TODO: Bucketing for exponential decay.
            panic_cc = sum(concurrencies[-PANIC_WINDOW_SEC:]) / PANIC_WINDOW_SEC
            stable_cc = sum(concurrencies[-STABLE_WINDOW_SEC:]) / STABLE_WINDOW_SEC

            is_over_panic_threshold = panic_cc / ready_pod >= (PANIC_THRESHOLD_PCT / 100)

            # * Decide mode.
            if is_over_panic_threshold or is_poked:
                log.info(f"Start panicking", {'clock': sim.state.clock.now()})
                self.scalers[func].mode = 'panic'
                desired_scale = math.ceil(panic_cc / cc_target)
            else:
                self.scalers[func].mode = 'stable'
                desired_scale = math.ceil(stable_cc / cc_target)
            # * Clamp the scale within bounds.
            desired_scale = min(max(desired_scale, max_down_scale), max_up_scale)

            old_scale = self.scalers[func].desired_scale
            self.scalers[func].desired_scale = desired_scale
            self.scalers[func].actual_scale = tracker.get_scale()

            if old_scale != desired_scale:
                if old_scale == 0:
                    log.info(f"(cold) Desired scale ({func}) {old_scale} -> {desired_scale}", {'clock': sim.state.clock.now()})
                else:
                    log.info(f"Desired scale ({func}) {old_scale} -> {desired_scale}", {'clock': sim.state.clock.now()})

        return

    def __repr__(self):
        return "Autoscaler" + repr(vars(self))

    class _Scaler_(object):
        def __init__(self, function: Function):
            self.function = function
            self.desired_scale = 0
            self.actual_scale = 0
            self.mode = 'panic'


class Throttler(object):
    def __init__(self, functions: List[Function]):
        self.breaker = Breaker('Throttler', 10_000)
        self.trackers: Dict[str, Throttler._Tracker_] = {func.name: self._Tracker_(func) for func in functions}

    def handle(self, request: Request):
        log.info(f"Handle {request.dest}", {'clock': sim.state.clock.now()})
        tracker = self.trackers[request.dest]
        tracker.breaker.enqueue(request)
        tracker.inc_concurrency()

        if len(tracker.instances) == 0:
            log.info(f"Cold start occurred on {request.dest}", {'clock': sim.state.clock.now()})
            sim.state.autoscaler.poke()

        processed = False
        for instance in tracker.instances:
            # sim.log.info(f"{instance.func} has {tracker.get_scale()} instances", {'clock': sim.state.clock.now()})
            processed = instance.reserve(request)

        if processed:
            tracker.dec_concurrency()
            tracker.breaker.dequeue(request)
        else:
            log.warn(f"No slots! Request to {request.dest} queued.", {'clock': sim.state.clock.now()})

        return

    def dispatch(self):
        # TODO: constantly trying to dispatch accumulated requests in all the trackers.
        pass

    def __repr__(self):
        return "Throttler" + repr(vars(self))

    class _Tracker_(object):
        def __init__(self, func: Function):
            self.breaker = Breaker(f"_Tracker_::{func.name}", 10_000)
            self.function = func
            self.instances: List[Instance] = []
            self.concurrencies = [0]

        def get_scale(self):
            return len(self.instances)

        def inc_concurrency(self):
            prev = self.concurrencies[-1]
            curr = prev + 1
            if len(self.concurrencies) == 1 and self.concurrencies[0] == 0:
                self.concurrencies[0] = curr
            else:
                self.concurrencies.append(curr)
            log.info(f"Concurrency={curr}", {'clock': sim.state.clock.now()})
            return

        def dec_concurrency(self):
            self.concurrencies[-1] = self.concurrencies[-1] - 1

        def __repr__(self):
            return "_Tracker_" + repr(vars(self))


class StateMachine(object):
    def __init__(self, autoscaler: Autoscaler, throttler: Throttler, clock: sim.Clock):
        self.autoscaler: Autoscaler = autoscaler
        self.throttler: Throttler = throttler
        self.clock = clock
        # self.instance_engine = instance_engine

# state_machine: StateMachine = None
