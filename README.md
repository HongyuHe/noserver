# FaaSm

FaaSm is a discrete-event simulator for modelling main-stream serverless systems.
It disaggregates the three scheduling dimensions: (1) load balancing, (2) autoscaling, and (3) placement. The policies of each are pluggable, enabling the exploration of multiple policy choices in one system.

![Architecture of FaaSm](.github/figures/faasm_arch.png?raw=true "Architecture of FaaSm")

## Setup

```bash
$ pip3 install -r requirements_dev.txt
```

## Run

```bash
$ python3 faasm <rps | test>
```
