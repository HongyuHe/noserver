# NoServer 凵
**NoServer is a full-system, queuing simulator for serverless workflows.**

### Key Features

* Supports serverless workflows, scalable to thousands function instances and hundreds of nodes.
* Simulates all layers of abstraction of a typical serverless system.
* Disaggregates the three scheduling dimensions: load balancing (LB), autoscaling (AS), and function placement (FP). 
    * Separate serverless police logic from the underlying system implementation.
* Models hyterogeneous worker types (e.g., Harvest VMs).

<p align="center">
    <img src=".github/figures/abstractions.png?raw=true" width="330">
</p>


## Overview


<!-- (contact: Hongyu He \<honghe@inf.ethz.ch\>) -->

![Architecture of NoServer](.github/figures/noserver_arch.png?raw=true "Architecture of NoServer")

The base models of each abstraction layer are the following:
* Serverless platform: [Knative](https://knative.dev/docs/)
* Cluster orchestration: [Kubernetes](https://kubernetes.io/)
* Container runtime: [containerd](https://containerd.io/)
* OS kernel: Linux

## Setup

```bash
$ pip3 install -r requirements.txt
```

## Usage

```bash
$ python3 -m noserver [flags]

Flags:
  --mode: Simulation mode to run. Available options: [test, rps, dag, benchmark, trace].
  --trace: Path to the DAG trace to simulate. Default: 'data/trace_dags.pkl'.
  --hvm: Specify a fixed Harvest VM from the trace to simulate.
  --logfile: Log file path.
  --display: Display the task DAG. (Opposite option: --nodisplay)
  --vm: Number of normal VMs. Default: 2.
  --cores: Number of cores per VM. Default: 40.
  --stages: Number of stages in the task DAG. Default: 8.
  --invocations: Total number of invocations in the task DAG. Default: 4096.
  --width: Width of the DAG. Default: 1.
  --depth: Depth of the DAG. Default: 1.
  --rps: Request per second arrival rate. Default: 1.0.
  --config: Path to a configuration file. Default: './configs/default.py'.

Note:
  • The '--mode' flag is required. You must provide a valid simulation mode.
  • Use '--display' to show the task DAG graph. Use '--nodisplay' to suppress the display.
  • To use other configurations: 
    python3 -m noserver --config=./configs/another_config.py:params
  • To override parameters:
    python3 -m noserver --mode dag --noconfig.harvestvm.ENABLE_HARVEST


```


## Validation

I conducted validation against the serverless platform [vHive](https://github.com/vhive-serverless/vHive) (a benchmark wrapper around [Knative](https://knative.dev/docs/)).

For the following experiments, the cluster specifications are the following:
* Machine type: `c220g5` on Cloudlab Winsconsin
    * Number of cores per node: 40 (2 sockets w/ 10 cores each, hyperthreads: 2)
    * Maximum theoretical throughput is around 40 requests per second.
* Cluster size: 11 nodes 
    * 1 master + 10 workers
* Function execution time: 1 s 
    * 50 percentile from [Azure function trace](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md)
* Function memory footprint: 170 MiB 
    * 50 percentile from [Azure function trace](https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md)

### Resource Utilization

![Validation of Resource Utilization](.github/figures/resources.png?raw=true "Validation of Resource Utilization")

<br>
<br>

### Real-Time Autoscaling

![Real-Time Autoscaling](.github/figures/autoscale.png?raw=true "Real-Time Autoscaling")

<br>
<br>

### Latency & Cold Start

In the following experiments, the cluster was not warmed up in order to preserve cold start.

* **50 percentile (p50):**

![Validation of p50 Queuing Latency](.github/figures/latp50.png?raw=true "Validation of p50 Queuing Latency")

<br>
<br>

* **99 percentile (p99):**

![Validation of p99 Queuing Latency](.github/figures/latp99.png?raw=true "Validation of p99 Queuing Latency")

<br>
<br>
