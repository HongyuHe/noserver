import logging


class Formatter(logging.Formatter):
    def format(self, record):
        record.clock = '\t'
        if record.args:
            record.clock = record.args.get("clock", "")
        return super().format(record)


log = logging.getLogger('')
ch = logging.StreamHandler()
# ch.setLevel(logging.INFO)
formatter = Formatter('(%(clock)s) [%(levelname)s] %(message)s')
ch.setFormatter(formatter)
logging.basicConfig(
    level=logging.INFO,
    handlers=[ch],
    #     # format='(%(asctime)s) [%(levelname)s] %(message)s'
)
# log.addHandler(ch)


state = None


class Clock(object):
    def __init__(self):
        self.time_milli = 0

    def inc(self, duration):
        self.time_milli += duration

    def now(self):
        return self.time_milli

    def __repr__(self):
        return "Clock" + repr(vars(self))
