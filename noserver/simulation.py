from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from system import State

import logging
import functools
import math
import json
import networkx as nx
import matplotlib.pyplot as plt
from glob import glob
import os
from pathlib import Path
import random
import sys

################################### CLI flags & Configs #####################################
from absl import flags
from ml_collections import config_flags

FLAGS = flags.FLAGS

flags.DEFINE_string(
    "mode", None, help="Simulation mode [test, rps, dag, benchmark, trace]"
)
flags.DEFINE_string(
    "trace", "data/trace_dags.pkl", help="Path to the DAG trace to simulate"
)
flags.DEFINE_string(
    "hvm", None, help="Specify a fixed Harvest VM from the trace to simulate"
)
flags.DEFINE_string("logfile", None, help="Log file path")
flags.DEFINE_boolean("display", False, help="Display task DAG (opposite: --nodisplay)")
flags.DEFINE_integer("vm", 2, help="Number of normal VMs")
flags.DEFINE_integer("cores", 40, help="Number of cores per VM")
flags.DEFINE_integer("stages", 8, help="Number of stages in task DAG")
flags.DEFINE_integer(
    "invocations", 4096, help="Total number of invocations in task DAG"
)
flags.DEFINE_integer("width", 1, help="With of the DAG")
flags.DEFINE_integer("depth", 1, help="Depth of the DAG")
flags.DEFINE_float("rps", 1.0, help="Request per second arrival rate")

flags.mark_flag_as_required("mode")

config_flags.DEFINE_config_file("config", default="./configs/default.py")

FLAGS(sys.argv)

"""
To use other configurations: 
    python noserver --config=./configs/another_config.py:params
To override parameters:
    python noserver --mode dag --noconfig.harvestvm.ENABLE_HARVEST

Example cmds:
    python -m noserver --mode benchmark --width 3 --depth 3 --rps 2 --invocations 1000 --config.policy.DUP_EXECUTION
"""

##############################################################################################


"""RNG for simulation"""
rng = random.Random(42)

"""Global simulation state."""
state: State = None

sign = functools.partial(math.copysign, 1)
# * [F0, F1, F2] -> [F0, F0, F1, F1, F2, F2]
interleave_lists = lambda l1, l2: [val for pair in zip(l1, l2) for val in pair]


def generate_exp_arrival_times_milli(rps, total):
    arrival_rate = rps / 1000.0
    iats = [int(rng.expovariate(arrival_rate)) for _ in range(total - 1)]

    now = 0
    times = [0]
    for i in range(total - 1):
        times.append(now + iats[i])
        now += iats[i]
    return times


def generate_dag(dag_name, width, depth, duration_milli, memory_mib):
    T: nx.Graph = nx.balanced_tree(width, depth)
    G: nx.DiGraph = nx.bfs_tree(T, 0)

    for node in G.nodes:
        G.nodes[node]["dag_name"] = dag_name
        # * Constant function across the generated DAG.
        G.nodes[node]["duration_milli"] = duration_milli
        G.nodes[node]["memory_mib"] = memory_mib
        G.nodes[node]["vcpu"] = 1

    relabel_mapping = {node: f"F{node}" for node in G.nodes()}
    G = nx.relabel_nodes(G, relabel_mapping)

    leaves = [x for x in G.nodes() if G.out_degree(x) == 0]

    end_node = f"F{len(G.nodes)}"
    G.add_node(
        end_node,
        dag_name=dag_name,
        duration_milli=duration_milli,
        memory_mib=memory_mib,
        vcpu=1,
    )

    for leaf in leaves:
        G.add_edge(leaf, end_node)

    return G


def load_dag(idx, dir):
    with open(dir, "r") as f:
        workload = json.load(f)

    dag_name = Path(dir).stem
    dag = nx.DiGraph(name=dag_name)
    # dag.add_nodes_from(workload['StartAt'])

    for _, (func, meta) in enumerate(workload["Functions"].items()):
        make_func_name = lambda n: f"{dag_name}-{idx}_{n}"
        dag.add_node(
            make_func_name(func),
            workload=dag_name,
            # TODO: Use probabilistic runtime.
            duration_milli=meta["DurationMilli"][0],
            memory_mib=meta["MemoryMib"][0],
        )
        for nxt in meta["Next"]:
            dag.add_edge(make_func_name(func), make_func_name(nxt))

    if list(nx.simple_cycles(dag)):
        raise ValueError("Task DAG is cyclic")
    else:
        return dag_name, dag


def load_dags(dir, display=False):
    dags = {}
    if os.path.isdir(dir):
        for j, dir in enumerate(glob(f"{dir}/*.json")):
            dag_name, dag = load_dag(j, dir=dir)
            dags[dag_name] = dag
    elif os.path.isfile(dir):
        dag_name, dag = load_dag(0, dir=dir)
        dags[dag_name] = dag
    else:
        raise ValueError("DAG path is invalid!")

    if display:
        display(dags)
    return dags


def display(dags):
    from itertools import count

    g = nx.compose_all(list(dags.values()))
    groups = set(nx.get_node_attributes(g, "workload").values())
    mapping = dict(zip(sorted(groups), count()))
    nodes = g.nodes()
    colors = [mapping[g.nodes[n]["workload"]] for n in nodes]

    pos = nx.planar_layout(g)
    ec = nx.draw_networkx_edges(g, pos, edge_color="darkgray", arrowsize=12)
    nc = nx.draw_networkx_nodes(
        g,
        pos,
        nodelist=nodes,
        node_color=colors,
        node_size=5000,
        cmap=plt.cm.jet,
        alpha=0.3,
    )
    nx.draw_networkx_labels(g, pos)
    plt.axis("off")
    plt.show()


class Formatter(logging.Formatter):
    def format(self, record):
        record.clock = "+"
        if record.args:
            record.clock = record.args.get("clock", "")
        return super().format(record)


log = logging.getLogger("noserver")
if FLAGS.logfile:
    handler = logging.FileHandler(FLAGS.logfile, mode="w")
else:
    handler = logging.StreamHandler()

# handler.setLevel(logging.INFO)
formatter = Formatter("[%(name)s @ %(clock)-5s] %(levelname)-8s | %(message)s")
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)

# logging.basicConfig(
#     filename=FLAGS.logfile,
#     # filemode='a',
#     level=logging.INFO,
#     # handlers=[ch],
#     format='[%(name)s @ %(clock)s] %(levelname)-8s | %(message)s',
# )


class Clock(object):
    def __init__(self):
        self.time_milli = 0

    def inc(self, duration):
        self.time_milli += duration

    def now(self):
        return self.time_milli

    def __repr__(self):
        return "Clock" + repr(vars(self))
