
# default[:chef_throttle][:server] = nil
# default[:chef_throttle][:exhibitor] = nil
default[:chef_throttle][:run_on_failure] = false
default[:chef_throttle][:limit] = 1
default[:chef_throttle][:cluster_name] = "default_cluster"
default[:chef_throttle][:cluster_path] = "/chef_throttle/clusters"
default[:chef_throttle][:lock_path] = "/queue"
