module Multicluster

using Reexport
@reexport using Distributed
using MPIClusterManagers

include("cluster.jl")
include("remotecall.jl")

export Cluster, 
       Node,
       addcluster, 
       addworkers,
       rmcluster,
       nodes,
       clusters,
       nclusters,
       contexts,

       pmap,
       @cluster_distributed,
       @distributed,
       @spawnat,
       remotecall,
       remotecall_wait,
       remotecall_fetch,
       remote_do,
       cluster_fetch,
       @node_spawnat,
       @cluster_spawnat,
       @fetchfrom_cluster,
       @fetchfrom_node,
       @cluster_everywhere

end # module Multicluster
