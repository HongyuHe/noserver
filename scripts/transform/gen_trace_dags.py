import networkx as nx
import pandas as pd
import random
from tqdm import tqdm
random.seed(42)

MAX_DURATION_MINUTES = 15

def generate_runtime(dag_stats):
    u = random.random()
    if u < 0.25:
        return dag_stats['duration_milli_min']
    elif u < 0.50:
        return dag_stats['duration_milli_p25']
    elif u < 0.75:
        return dag_stats['duration_milli_p75']
    else:
        return dag_stats['duration_milli_max']

def generate_trace_dag(dag_name, dag_stats):
    width = dag_stats['width']
    depth = dag_stats['depth']
    if dag_stats['width'] >= dag_stats['depth']:
        depth = 1
    else:
        width = 1
    
    T: nx.Graph = nx.balanced_tree(round(width), round(depth))
    G: nx.DiGraph = nx.bfs_tree(T, 0)
    
    for node in G.nodes:
        duration_milli = generate_runtime(dag_stats)
        
        G.nodes[node]['dag_name'] = dag_name
        G.nodes[node]['duration_milli'] = round(min(MAX_DURATION_MINUTES*60*1000,
                                                    max(3, duration_milli)))
        G.nodes[node]['memory_mib'] = 170
        G.nodes[node]['vcpu'] = 1

    relabel_mapping = {node: f"F{node}" for node in G.nodes()}
    G = nx.relabel_nodes(G, relabel_mapping)
    
    leaves = [x for x in G.nodes() if G.out_degree(x)==0]

    end_node = f"F{len(G.nodes)}"
    duration_milli = generate_runtime(dag_stats)
    G.add_node(
        end_node,
        dag_name = dag_name,
        duration_milli = round(min(MAX_DURATION_MINUTES*60*1000,
                                   max(3, duration_milli))),
        memory_mib = 170,
        vcpu = 1,
    )
    
    for leaf in leaves:
        G.add_edge(leaf, end_node)
        
    if list(nx.simple_cycles(G)):
        raise ValueError('Generated DAG is cyclic')
    return G

dag_struct_df = pd.read_csv('/Users/hongyu/Projects/notebooks/data/harvestvm/dag_structure_large.csv', index_col=['start_ts', 'dag_id', 'dag_inv_id'])

trace_dags = []
i = 1
for index, row in tqdm(dag_struct_df.iterrows(), total=dag_struct_df.shape[0]):
    dag = generate_trace_dag(f"trace_dag_{i}", row.to_dict())
    trace_dags.append(dag)
    i += 1
    # print(f"Generated {i} DAGs", end='\r')

print(f"Created {len(trace_dags)} DAGs")

import cloudpickle

path = "data/trace_dags.pkl"
# path = '/Users/hongyu/Projects/notebooks/data/harvestvm/trace_dags_large.pkl'
with open(path, "wb") as f:
    cloudpickle.dump(trace_dags, f)
print(f"Generated DAGs from the trace saved to {path}")