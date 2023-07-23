from ml_collections import config_dict


def get_config():
    autoscale_config = config_dict.ConfigDict()
    

    autoscale_config.cluster = config_dict.ConfigDict()
    # * Kubelet avg QPS=50, burst QPS=100 (every 20 or 10ms) between master API server for a 500-node cluster.
    # * Kubelet QPS=5 to 10 (every 200 or 100 ms) by default (https://cloud.redhat.com/blog/500_pods_per_node).
    autoscale_config.cluster.CRI_ENGINE_PULLING_PERIOD_MILLI = 50 # 1000 // 100
    autoscale_config.cluster.AUTOSCALING_PERIOD_MILLI = 2000 
    # * Do not invoke scheduler more often than the function scales can change.
    autoscale_config.cluster.SCHEDULING_PERIOD_MILLI = 5000 # autoscale_config.cluster.AUTOSCALING_PERIOD_MILLI  
    autoscale_config.cluster.MONITORING_PERIOD_MILLI = 10_000
    autoscale_config.cluster.MEMORY_USAGE_OFFSET = 5
    # * Update concurrency every 1 s (https://github.com/knative/serving/blob/main/pkg/autoscaler/metrics/collector.go#L37).
    autoscale_config.cluster.UPDATE_CONCURRENCY_PERIOD_MILLI = 1000
    # * Controls the slop of the burst.
    autoscale_config.cluster.NETWORK_DELAY_MILLI = 100
    # * Interval of dispatching requested in the central queue.
    autoscale_config.cluster.DISPATCH_PERIOD_MILLI = 5000
    # * The time the control plane takes to discover the status change of an instance
    # * in order to model communication delay.
    autoscale_config.cluster.DISCOVERY_DELAY_MILLI = 2000


    autoscale_config.node = config_dict.ConfigDict()
    autoscale_config.node.MAX_NUM_INSTANCES = 490
    # TODO: size of firecracker.
    autoscale_config.node.INSTANCE_SIZE_MIB = 200  
    # * Create max. two instances at a time (assuming two threads available for containerd to operate).
    autoscale_config.node.INSTANCE_CREATION_CONCURRENCY = 1
    # * Create max. ten instances can be deleted at a time.
    autoscale_config.node.INSTANCE_DELETION_CONCURRENCY = 1
    # * The number of replication controllers that are allowed to sync concurrently (https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/).
    autoscale_config.node.CONCURRENT_REPLICA_SYNCS = 5 # ! Not used. 
    # TODO: measure CRI engine delay.
    # * No container on the node 
    autoscale_config.node.COLD_INSTANCE_CREATION_DELAY_MILLI = 6 * 1000 
    # * There are requested instances on the node, but they are all busy.
    autoscale_config.node.WARM_INSTANCE_CREATION_DELAY_MILLI = 1 * 1000 
    autoscale_config.node.BINDING_REQUESTS_CONGESTION_WINDOW_MILLI = 0 # ! Not used.   
    autoscale_config.node.JOB_MEMORY_OVERHEAD_MIB = 50
    # * Default K8s grace period: 30s. 
    autoscale_config.node.INSTANCE_GRACE_PERIOD_SEC = 30 
    # * On average, 20% of the CPU time is consumed by the underling infrastructure.
    autoscale_config.node.INFRA_CPU_OVERHEAD_RATIO = 0.5

    
    autoscale_config.harvestvm = config_dict.ConfigDict()
    autoscale_config.harvestvm.USE_HARVESTVM = True
    autoscale_config.harvestvm.ENABLE_HARVEST = True
    # * Latency in spawning HarvestVMs. 
    # * Restart delay is 10s (http://web.stanford.edu/~yawenw/SmartHarvestEuroSys21.pdf)
    autoscale_config.harvestvm.HARVESTVM_SPAWN_LATENCY_MILLI = 10 * 1000
    # * Preemption notification period: 30s.
    autoscale_config.harvestvm.PREEMPTION_NOTIFICATION_SEC = 30  
    autoscale_config.harvestvm.BASE_HAZARD = 0.42
    autoscale_config.harvestvm.SURVIVAL_PREDICT_PERIOD_MILLI = 500
    # * Harvest resources every 1s (granularity of the trace).
    autoscale_config.harvestvm.HARVEST_PERIOD_MILLI = 500
    # * Number of HVMs.
    autoscale_config.harvestvm.NUM_HVMS = 0


    autoscale_config.autoscaler = config_dict.ConfigDict()
    autoscale_config.autoscaler.ALWAYS_PANIC = True
    # * Due to communication overhead, the metrics are not always reflective of the 
    # * real-time scales of the functions -> x10 for the window size.
    autoscale_config.autoscaler.PANIC_WINDOW_SEC = 6 * 10
    autoscale_config.autoscaler.STABLE_WINDOW_SEC = 60 * 10
    autoscale_config.autoscaler.MAX_SCALE_UP_RATE = 1000
    autoscale_config.autoscaler.MAX_SCALE_DOWN_RATE = 2
    autoscale_config.autoscaler.PANIC_THRESHOLD_PCT = 200
    
    autoscale_config.policy = config_dict.ConfigDict()
    autoscale_config.policy.LOAD_BALANCE = 'first_available' # 'least_loaded'
    autoscale_config.policy.DUP_EXECUTION = False
    autoscale_config.policy.DUP_EXECUTION_THRESHOLD = 0.5
    
    return autoscale_config
    