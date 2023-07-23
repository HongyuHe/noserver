import networkx as nx
import pandas as pd
import random
random.seed(42)
from tqdm import tqdm
import cloudpickle
import sys

full_transform = sys.argv[1] == '1'

path = "data/trace_dags.pkl" if not full_transform else "data/bundled_dags.pkl"
with open(path, "rb") as f:
    original_dags = cloudpickle.load(f)
print(f"Loaded {len(original_dags)} DAGs from {path}")

MAX_CORES = 40
MAX_DURATION_MINUTES = 15

new_dags = []
for dag in tqdm(original_dags, total=len(original_dags)):
    fused_dag = nx.DiGraph()
    
    total_duration = 0
    memory_max = 1
    vcpu_max = 1
    func_index = 0
    curr_func = f"F{func_index}"
    prev_func = None
    
    # fused_flag = False
    layers = dict(enumerate(nx.bfs_layers(dag, 'F0')))
    for layer, funcs in layers.items():  
        stage_duration_max = 0
        stage_memory = 0
        stage_vcpu = 0
        
        for func in funcs:
            # * Take the max duration among parallel invocations (no bundling).
            stage_duration_max = max(stage_duration_max, dag.nodes[func]['duration_milli'])
            stage_memory += dag.nodes[func]['memory_mib']
            stage_vcpu += dag.nodes[func]['vcpu']
        
        if stage_vcpu > MAX_CORES:
            scaler = stage_vcpu / MAX_CORES
            stage_duration_max *= scaler
            stage_vcpu = MAX_CORES
        
        # * Scale duration based on the resources.
        prev_total_duration = total_duration
        total_duration += stage_duration_max
        
        if total_duration > MAX_DURATION_MINUTES*60*1000:
            if layer == 0:
                raise RuntimeError(f"Exceeding duration max at root ({total_duration=})!")
            if prev_total_duration == 0:
                raise RuntimeError(f"Zero duration at ({layer=})!")
            assert vcpu_max <= MAX_CORES
            
            dag_name = dag.nodes[funcs[0]]['dag_name']
            fused_dag.add_node(
                curr_func,
                dag_name = dag_name.replace('trace', 'fused') \
                    if 'trace' in dag_name else dag_name.replace('bundled', 'wisefuse'),
                duration_milli = max(3, round(prev_total_duration)),
                memory_mib = memory_max,
                vcpu = vcpu_max,
            )
            if prev_func is not None:
                fused_dag.add_edge(prev_func, curr_func)
            
            prev_func = curr_func
            func_index += 1
            curr_func = f"F{func_index}"
            
            # * Reset counters.
            total_duration = stage_duration_max
            memory_max = stage_memory
            vcpu_max = stage_vcpu
            
            # if layer == len(layers)-1:
            #     fused_flag = True
        else:        
            memory_max = max(memory_max, stage_memory)
            vcpu_max = max(vcpu_max, stage_vcpu)
    
    # * Handle trailing nodes.
    # if fused_flag:
    if total_duration == 0:
        raise RuntimeError(f"Zero duration at trailing node!")
    assert vcpu_max <= MAX_CORES
    
    dag_name = dag.nodes[funcs[0]]['dag_name']
    fused_dag.add_node(
        curr_func,
        dag_name = dag_name.replace('trace', 'fused') \
            if 'trace' in dag_name else dag_name.replace('bundled', 'wisefuse'),
        duration_milli = max(3, round(total_duration)),
        memory_mib = memory_max,
        vcpu = vcpu_max,
    )
    if prev_func is not None:
        fused_dag.add_edge(prev_func, curr_func)

    if list(nx.simple_cycles(fused_dag)):
        raise ValueError('Generated DAG is cyclic')
    new_dags.append(fused_dag)

path = "data/fused_dags.pkl" if not full_transform else "data/wisefuse_dags.pkl"
with open(path, "wb") as f:
    cloudpickle.dump(new_dags, f)
print(f"Fused DAGs saved to {path}")