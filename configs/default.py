from ml_collections import config_dict


def get_config():
    default_config = config_dict.ConfigDict()
    

    default_config.cluster = config_dict.ConfigDict()
    # * Kubelet avg QPS=50, burst QPS=100 (every 20 or 10ms) between master API server for a 500-node cluster.
    # * Kubelet QPS=5 to 10 (every 200 or 100 ms) by default (https://cloud.redhat.com/blog/500_pods_per_node).
    default_config.cluster.CRI_ENGINE_PULLING_PERIOD_MILLI = 1 # 1000 // 100
    default_config.cluster.AUTOSCALING_PERIOD_MILLI = 2000 
    # * Do not invoke scheduler more often than the function scales can change.
    default_config.cluster.SCHEDULING_PERIOD_MILLI = 5000 # default_config.cluster.AUTOSCALING_PERIOD_MILLI  
    default_config.cluster.MONITORING_PERIOD_MILLI = 1_000
    default_config.cluster.MEMORY_USAGE_OFFSET = 5
    # * Update concurrency every 1 s (https://github.com/knative/serving/blob/main/pkg/autoscaler/metrics/collector.go#L37).
    default_config.cluster.UPDATE_CONCURRENCY_PERIOD_MILLI = 1000
    # * Controls the slop of the burst.
    default_config.cluster.NETWORK_DELAY_MILLI = 10
    # * Interval of dispatching requested in the central queue.
    default_config.cluster.DISPATCH_PERIOD_MILLI = 1 # 5000
    # * The time the control plane takes to discover the status change of an instance
    # * in order to model communication delay.
    default_config.cluster.DISCOVERY_DELAY_MILLI = 1 # 2000


    default_config.node = config_dict.ConfigDict()
    default_config.node.MAX_NUM_INSTANCES = 490
    # TODO: size of firecracker.
    default_config.node.INSTANCE_SIZE_MIB = 200  
    # * Create max. two instances at a time (assuming two threads available for containerd to operate).
    default_config.node.INSTANCE_CREATION_CONCURRENCY = 1
    # * Create max. ten instances can be deleted at a time.
    default_config.node.INSTANCE_DELETION_CONCURRENCY = 1
    # * The number of replication controllers that are allowed to sync concurrently (https://kubernetes.io/docs/reference/command-line-tools-reference/kube-controller-manager/).
    default_config.node.CONCURRENT_REPLICA_SYNCS = 5 # ! Not used. 
    # TODO: measure CRI engine delay.
    # * No container on the node 
    default_config.node.COLD_INSTANCE_CREATION_DELAY_MILLI = 3 * 1000 
    # * There are requested instances on the node, but they are all busy.
    default_config.node.WARM_INSTANCE_CREATION_DELAY_MILLI = 1 * 1000 
    default_config.node.BINDING_REQUESTS_CONGESTION_WINDOW_MILLI = 0 # ! Not used.   
    default_config.node.JOB_MEMORY_OVERHEAD_MIB = 50
    # * Default K8s grace period: 30s. 
    default_config.node.INSTANCE_GRACE_PERIOD_SEC = 30 
    # * On average, 20% of the CPU time is consumed by the underling infrastructure.
    default_config.node.INFRA_CPU_OVERHEAD_RATIO = 0.0

    
    default_config.harvestvm = config_dict.ConfigDict()
    default_config.harvestvm.USE_HARVESTVM = False
    default_config.harvestvm.ENABLE_HARVEST = True
    # * Latency in spawning HarvestVMs. 
    # * Restart delay is 10s (http://web.stanford.edu/~yawenw/SmartHarvestEuroSys21.pdf)
    default_config.harvestvm.HARVESTVM_SPAWN_LATENCY_MILLI = 10 * 1000
    # * Preemption notification period: 30s.
    default_config.harvestvm.PREEMPTION_NOTIFICATION_SEC = 30  
    default_config.harvestvm.BASE_HAZARD = 0.42
    default_config.harvestvm.SURVIVAL_PREDICT_PERIOD_MILLI = 500
    # * Harvest resources every 1s (granularity of the trace).
    default_config.harvestvm.HARVEST_PERIOD_MILLI = 500
    # * Number of HVMs.
    default_config.harvestvm.NUM_HVMS = 0


    default_config.autoscaler = config_dict.ConfigDict()
    default_config.autoscaler.ALWAYS_PANIC = True
    # * Due to communication overhead, the metrics are not always reflective of the 
    # * real-time scales of the functions -> x10 for the window size.
    default_config.autoscaler.PANIC_WINDOW_SEC = 6 * 10
    default_config.autoscaler.STABLE_WINDOW_SEC = 60 * 10
    default_config.autoscaler.MAX_SCALE_UP_RATE = 1000
    default_config.autoscaler.MAX_SCALE_DOWN_RATE = 2
    default_config.autoscaler.PANIC_THRESHOLD_PCT = 200
    
    
    default_config.policy = config_dict.ConfigDict()
    default_config.policy.LOAD_BALANCE = 'first_available' # 'least_loaded'
    default_config.policy.DUP_EXECUTION = False
    default_config.policy.DUP_EXECUTION_THRESHOLD = 0.5
    
    
    default_config.request = config_dict.ConfigDict()
    # & Max duration 15min (AWS: https://aws.amazon.com/about-aws/whats-new/2018/10/aws-lambda-supports-functions-that-can-run-up-to-15-minutes/)
    default_config.request.MAX_DURATION_SEC = 60 * 15
    
    
    return default_config
    