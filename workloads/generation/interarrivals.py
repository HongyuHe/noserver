import numpy as np


class InterArrivalGenerator(object):
    def __init__(self, mean, opts=None):
        self.mean = mean

    def __next__(self):
        return 1


class PoissonArrivalGenerator(InterArrivalGenerator):
    def __next__(self):
        return np.random.exponential(self.mean)

class ConstArrivalGenerator(InterArrivalGenerator):
    def __next__(self):
        return (self.mean)

class LogNormalArrivalGenerator(InterArrivalGenerator):
    def __init__(self, mean, opts=None):
        InterArrivalGenerator.__init__(self, mean, opts)
        self.scale = float(opts["std_dev_arrival"]**2)
        # Calculate the mean of the underlying normal distribution
        self.mean = np.log(mean**2 / np.sqrt(mean**2 + self.scale))
        self.scale = np.sqrt(np.log(self.scale / mean**2 + 1))

    def __next__(self):
        return np.random.lognormal(self.mean, self.scale)


class CustomizedArrivalGenerator(InterArrivalGenerator):
    def __init__(self, mean, opts=None):
        InterArrivalGenerator.__init__(self, mean, opts)
        self.idx = 0
        self.arrival_list = (opts["arrival_list"])
        # Calculate the mean of the underlying normal distribution
        # self.mean = np.log(mean**2 / np.sqrt(mean**2 + self.scale))
        # self.scale = np.sqrt(np.log(self.scale / mean**2 + 1))

    def __next__(self):
        # if self.idx == 0:
        #     import pdb; pdb.set_trace()
        #     iat = self.arrival_list[self.idx]
        # else:
        
        if (self.idx > 0 and self.idx < len(self.arrival_list)):
            iat = self.arrival_list[self.idx] - self.arrival_list[self.idx - 1] 
        elif self.idx == 0:
            iat = self.arrival_list[self.idx]
        else:
            return -1
        if iat < 0:
            print("Error in the iat")
            exit(-1)
        self.idx += 1
        return iat

