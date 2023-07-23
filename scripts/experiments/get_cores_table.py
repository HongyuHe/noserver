import pandas as pd
import numpy as np
import pickle


def get_cores_schedule(df):
    cores_schedule = []
    # * Offset ts by the smallest.
    df = df.copy()
    df['timestamp_s'] -= df.timestamp_s.min()
    df.sort_values(by='timestamp_s')
    
    gen_cores = iter(df.avail_cores)
    gen_ts = iter(df.timestamp_s)

    prev_cores = next(gen_cores)
    prev_ts = next(gen_ts)

    next_cores = next(gen_cores)
    next_ts = next(gen_ts)

    for i in range(df.timestamp_s.max()+1):
        if i < next_ts:
            cores_schedule.append(prev_cores)
        else:
            cores_schedule.append(next_cores)
            prev_cores, prev_ts = next_cores, next_ts
            try:
                next_cores = next(gen_cores)
                next_ts = next(gen_ts)
            except StopIteration:
                break
    assert len(cores_schedule) == df.timestamp_s.max()+1
    return cores_schedule


if __name__ == "__main__":
    resource_df = pd.read_csv('data/harvestvm/NodeResources.csv')
    resource_df['Time'] = pd.to_datetime(resource_df['Time'], unit='ns')
    resource_df['timestamp_s'] = resource_df.Time.astype(np.int64) // 10 ** 9
    # resource_df['timestamp_ns'] = resource_df.Time.astype(np.int64)
    # * Take flooring.
    resource_df['avail_cores'] = np.floor(resource_df.AvailableCores).astype(np.int64)
    resource_df = resource_df[resource_df.avail_cores>=0]
    resource_df

    ts_unit_min = float('inf')
    traces = {}
    for node in resource_df.NodeId.unique():
        tracedf = resource_df[resource_df.NodeId==node]
        ts_unit_min = min(tracedf.timestamp_ns.diff().min(), ts_unit_min)
        traces[node] = get_cores_schedule(tracedf)

    with open("./data/harvestvm/cores_table.pkl", 'wb') as f:
        pickle.dump(traces, f)