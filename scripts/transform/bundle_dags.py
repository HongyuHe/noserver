import networkx as nx
import pandas as pd
import random
random.seed(42)
from tqdm import tqdm
import math
import cloudpickle

path = "data/trace_dags.pkl"
with open(path, "rb") as f:
    original_dags = cloudpickle.load(f)
print(f"Loaded {len(original_dags)} DAGs from {path}")

MAX_CORES = 40

def divide_bundles(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

new_dags = []
total_transformed = 0
for dag in tqdm(original_dags, total=len(original_dags)):
    bundled_dag = nx.DiGraph()

    prev_funcs = []
    func_id = 0
    for layer, funcs in dict(enumerate(nx.bfs_layers(dag, 'F0'))).items():
        attributes = dict(dag.nodes[funcs[0]])
        dag_name = attributes['dag_name']
        
        if len(funcs) == 1:
            curr_func = f"F{func_id}"
            func_id += 1

            bundled_dag.add_node(
                curr_func,
                dag_name = dag_name.replace('trace', 'bundled'),
                duration_milli = attributes['duration_milli'],
                memory_mib = attributes['memory_mib'],
                vcpu = attributes['vcpu'],
            )
            if prev_funcs:
                for prev_func in prev_funcs:
                    bundled_dag.add_edge(prev_func, curr_func)
            prev_funcs = [curr_func]
        else:
            total_transformed += 1
            bundles = divide_bundles(funcs, MAX_CORES)
            curr_funcs = []
            for bundle in bundles:
                durations = []
                memories = []
                for func in bundle:
                    durations.append(dag.nodes[func]['duration_milli'])
                    memories.append(dag.nodes[func]['memory_mib'])
                
                curr_func = f"F{func_id}"
                curr_funcs.append(curr_func)
                func_id += 1
                
                # * Take the smeared averge as the accelerated runtime.
                new_duration = max(3, round(sum(durations) / len(durations)))
                # ? Take the sum of memories of parallel invocations.
                new_memory = sum(memories)
                new_vcpu = len(list(bundle))

                bundled_dag.add_node(
                    # * One function per layer.
                    curr_func,
                    dag_name = attributes['dag_name'].replace('trace', 'bundled'),
                    duration_milli = new_duration,
                    memory_mib = new_memory,
                    vcpu = new_vcpu,
                )

                if prev_funcs:
                    for prev_func in prev_funcs:
                        bundled_dag.add_edge(prev_func, curr_func)
                
            prev_funcs = curr_funcs
        
    if list(nx.simple_cycles(bundled_dag)):
        raise ValueError('Generated DAG is cyclic')
    
    new_dags.append(bundled_dag)

path = f"data/bundled_dags.pkl"
with open(path, "wb") as f:
    cloudpickle.dump(new_dags, f)

print(f"Bundled {total_transformed} out of {len(original_dags)} DAGs")
print(f"Bundled DAGs saved to {path}")