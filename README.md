# FaaSim

FaaSim is a discrete-event simulator for modeling main-stream serverless systems.
It disaggregates the three scheduling dimensions: (1) load balancing, (2) autoscaling, and (3) placement. The algorithms of each dimension are pluggable, enabling the exploration of multiple policy choices in one system.

**NB:** This is a prototype that I developed as a proof of concept for my research. A mature version in Golang is in the making at ETH. 

(contact: Hongyu He \<honghe@inf.ethz.ch\>)

![Architecture of FaaSim](.github/figures/faasim_arch.png?raw=true "Architecture of FaaSim")

## Setup

```bash
$ pip3 install -r requirements_dev.txt
```

## Run

```bash
$ python3 faasim <rps | test>
```

## Validation

The following validations were conducted against the popular serverless platform [Knative](https://knative.dev/docs/about/testimonials/).

For the following experiments, the hardware specifications are the following:
* Cluster size: 2 nodes (1 master + 1 worker)
* Number of cores per node: 16 -> maximum theoretical throughput is 16 requests per second
* Function execution time: 1 s (50 percentile from [Azure function trace](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md))
* Function memory footprint: 170 MiB (50 percentile from [Azure function trace](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md))

### CPU Utilization

![Validation of CPU utilization](.github/figures/validate_cpu.png?raw=true "Validation of CPU Utilization")

### Memory Usage

![Validation of memory usage](.github/figures/validate_mem.png?raw=true "Validation of Memory Usage")

### Latency & Cold Start

In the following experiments, the system was not warmed up in order to preserve cold start.

* **CDF:**

![Validation of Queuing Latency](.github/figures/validate_latcdf.png?raw=true "Validation of Queuing Latency")

* **50 percentile (p50):**

![Validation of p50 Queuing Latency](.github/figures/validate_latp50.png?raw=true "Validation of p50 Queuing Latency")

* **99 percentile (p99):**

![Validation of p99 Queuing Latency](.github/figures/validate_latp99.png?raw=true "Validation of p99 Queuing Latency")

### Autoscaling

![Validation of Autoscaling](.github/figures/validate_scale.png?raw=true "Validation of Autoscaling")


---
## TODO

- [ ] Conduct validation with more functions.
- [ ] Implemement instance placement and load balancing.
- [ ] Conduct validation with more workers.
