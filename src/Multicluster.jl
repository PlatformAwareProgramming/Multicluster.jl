module Multicluster

using Distributed
using MPIClusterManagers

include("cluster.jl")
include("remotecall.jl")

export Cluster, Node,
       addcluster, 
       addworkers,
       rmcluster,
       nodes,
       clusters,
       nclusters,
       contexts,

       pmap,
       @distributed_cluster,
       @distributed,
       @spawnat,
       remotecall,
       remotecall_wait,
       remotecall_fetch,
       remote_do,
       fetch_cluster,
       @spawnat_node,
       @spawnat_cluster,
       @fetchfrom_cluster,
       @fetchfrom_node,
       @everywhere_cluster

end # module Multicluster
