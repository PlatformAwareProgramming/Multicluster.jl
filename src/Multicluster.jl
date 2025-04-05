module Multicluster

using Distributed
using MPIClusterManagers

include("cluster.jl")
include("remotecall.jl")

export addcluster

end # module Multicluster
