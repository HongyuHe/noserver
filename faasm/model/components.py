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


class Autoscaler(object):
    def __init__(self, functions: List[Function]):
        self.scalers: Dict[str: Autoscaler._Scaler_] = {
            func.name: self._Scaler_(func) for func in functions
        }

    def poke(self, request: Request):
        tracker: Throttler._Tracker_ = sim.state.throttler.trackers[request.dest]
        concurrencies: List[int] = tracker.concurrencies
        cc_limit: float = tracker.function.concurrency_limit

        desired_scale = 0
        if len(concurrencies) >= PANIC_WINDOW_SEC:
            # * NB: This is plane averaging without bucketing.
            # TODO: Bucketing for exponential decay.
            actual_cc = sum(concurrencies[-PANIC_WINDOW_SEC:]) / PANIC_WINDOW_SEC
            desired_scale = actual_cc / cc_limit
        elif len(concurrencies) >= 1:
            actual_cc = sum(concurrencies) / len(concurrencies)
            desired_scale = actual_cc / cc_limit
        else:
            log.fatal(f"No concurrency measurements for {request.dest}")

        self.scalers[request.dest].actual_scale = 0
        self.scalers[request.dest].desired_scale = math.ceil(desired_scale)  # ! Taking the ceiling for now.
        log.info(f"Cold start ({request.dest}): 0 -> {desired_scale}")
        return

    def evaluate(self):
        for func, tracker in sim.state.throttler.trackers.items():
            concurrencies: List[int] = tracker.concurrencies
            actual_scale = tracker.get_scale()
            # https://github.com/knative/serving/blob/main/pkg/autoscaler/scaling/autoscaler.go#L151
            ready_pod = actual_scale if actual_scale != 0 else 1

            # cc_limit: float = tracker.function.concurrency_limit

            window = STABLE_WINDOW_SEC
            if self.scalers[func].mode == 'panic':
                window = PANIC_WINDOW_SEC

            window = min(window, len(concurrencies))
            actual_cc = sum(concurrencies[-window:]) / window
            desired_scale = math.ceil(actual_cc / ready_pod)

            old_scale = self.scalers[func].desired_scale
            self.scalers[func].desired_scale = desired_scale
            self.scalers[func].actual_scale = ready_pod

            # * Update mode.
            if actual_cc >= 2 * ready_pod:
                self.scalers[func].mode = 'panic'
            else:
                self.scalers[func].mode = 'stable'

            if old_scale != desired_scale:
                log.info(f"Desired scale ({func}) {old_scale} -> {desired_scale}")

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
        log.info(f"Handle {request.dest}")
        tracker = self.trackers[request.dest]
        tracker.breaker.enqueue(request)
        tracker.inc_concurrecy()

        if len(tracker.instances) == 0:
            log.info(f"Cold start occurred on {request.dest}")
            sim.state.autoscaler.poke(request)

        processed = False
        for instance in tracker.instances:
            sim.log.info(f"{instance.func} Has {tracker.get_scale()} instances")
            processed = instance.reserve(request)

        if processed:
            tracker.dec_concurrecy()
            tracker.breaker.dequeue(request)
        else:
            log.warn(f"No slots! Request to {request.dest} queued.")

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

        def inc_concurrecy(self):
            prev = self.concurrencies[-1]
            curr = prev + 1
            if len(self.concurrencies) == 1 and self.concurrencies[0] == 0:
                self.concurrencies[0] = curr
            else:
                self.concurrencies.append(curr)

        def dec_concurrecy(self):
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
