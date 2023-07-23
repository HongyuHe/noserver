import math
from typing import *

from .. import simulation as sim
from .function import *
from .instance import *
from .throttler import Throttler


autoscaler_config = sim.FLAGS.config.autoscaler


class Autoscaler(object):
    def __init__(self, functions: List[Function]):
        self.scalers: Dict[str : Autoscaler._Scaler_] = {
            function.name: self._Scaler_(function.name) for function in functions
        }

    def poke(self, request: Request):
        # * Immediately start an evaluation round.
        self.evaluate(request)
        return

    def evaluate(self, request: Request = None):
        # TODO: When scaling to zero Replicas, the last Replica will only be removed after
        # TODO(cont.): there has not been any traffic to the Revision for the entire duration of the window.
        def _compute_observed_cc(concurrencies, window):
            window = min(len(concurrencies), window)
            return sum(concurrencies[-window:]) / window

        for func, tracker in sim.state.throttler.trackers.items():
            if request is not None and request.dest != func:
                # * Poked by the throttler -> looping until found the empty function.
                # ! Make the search more efficient.
                continue

            tracker: Throttler._Tracker_ = tracker
            concurrencies: List[int] = tracker.concurrencies
            actual_scale = tracker.get_scale()
            # * https://github.com/knative/serving/blob/main/pkg/autoscaler/scaling/autoscaler.go#L151
            ready_pod = actual_scale if actual_scale != 0 else 1
            cc_target: float = tracker.function.concurrency_limit
            # * https://github.com/knative/serving/blob/main/pkg/autoscaler/scaling/autoscaler.go#L180
            max_up_scale = math.ceil(autoscaler_config.MAX_SCALE_UP_RATE * ready_pod)
            max_down_scale = math.floor(
                ready_pod / autoscaler_config.MAX_SCALE_DOWN_RATE
            )

            # ! Below is uniform averaging without bucketing (alter: exponential decay).
            # TODO: Extract policy choices.
            panic_cc = _compute_observed_cc(
                concurrencies, autoscaler_config.PANIC_WINDOW_SEC
            )
            stable_cc = _compute_observed_cc(
                concurrencies, autoscaler_config.STABLE_WINDOW_SEC
            )

            is_over_panic_threshold = panic_cc / ready_pod >= (
                autoscaler_config.PANIC_THRESHOLD_PCT / 100
            )
            if autoscaler_config.ALWAYS_PANIC or (panic_cc > 0 and actual_scale == 0):
                # * Let cold function stay panic.
                is_over_panic_threshold = True

            # * Decide mode.
            if (
                is_over_panic_threshold
                or len(concurrencies) < autoscaler_config.STABLE_WINDOW_SEC
            ):
                # sim.log.info(f"Start panicking", {'clock': sim.state.clock.now()})
                self.scalers[func].mode = "panic"
                desired_scale = math.ceil(panic_cc / cc_target)
                n_requests_in_window = sum(
                    concurrencies[-autoscaler_config.PANIC_WINDOW_SEC :]
                )
            else:
                self.scalers[func].mode = "stable"
                desired_scale = math.ceil(stable_cc / cc_target)
                n_requests_in_window = sum(
                    concurrencies[-autoscaler_config.STABLE_WINDOW_SEC :]
                )
            # * Clamp the scale within bounds.
            desired_scale = min(max(desired_scale, max_down_scale), max_up_scale)

            if desired_scale == 0:
                # * "When scaling to zero Replicas, the last Replica will only be removed after
                # * there has not been any traffic to the Revision for the entire duration of the stable window."
                # * https://knative.dev/docs/serving/autoscaling/kpa-specific/
                desired_scale = 1 if n_requests_in_window > 0 else 0

            old_scale = self.scalers[func].desired_scale
            self.scalers[func].desired_scale = desired_scale
            self.scalers[func].actual_scale = tracker.get_scale()

            if old_scale != desired_scale:
                if old_scale == 0:
                    sim.log.info(f"(autoscaler) Cold start upon {func}.")
                sim.log.info(
                    f"(autoscaler) Desired scale {func}: {old_scale} -> {desired_scale}",
                    {"clock": sim.state.clock.now()},
                )
        return

    def __repr__(self):
        return "Autoscaler" + repr(vars(self))

    @dataclass
    class _Scaler_(object):
        func: str
        desired_scale: int = 0
        actual_scale: int = 0
        mode: str = "panic"
