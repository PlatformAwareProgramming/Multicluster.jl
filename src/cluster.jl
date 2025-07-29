# addcluster

mutable struct ClusterInfo 
    cid::Integer
    access_node_address::String
    nw::Integer
    access_node_args
    compute_node_args
    contexts::Vector{Union{Nothing,Vector{Integer}}}
end

struct Cluster
    cid::Integer
    xid::Union{Nothing,Integer}
    Cluster(cid) = (@assert haskey(cluster_table[], cid); new(cid, nothing))
    function Cluster(cid, xid) 
        @assert haskey(cluster_table[], cid)
        @assert isnothing(xid) || xid <= length(cluster_table[][cid].contexts)
        @assert isnothing(xid) || !isnothing(cluster_table[][cid].contexts[xid])
        new(cid, xid)
    end
end

struct Node
    cid::Integer
    pid::Integer
    function Node(cluster_handle::Cluster, pid)
        cid = cluster_handle.cid
        @assert haskey(cluster_table[], cid)
        @assert in(pid, reduce(vcat, filter(!isnothing, cluster_table[][cid].contexts)))
        new(cid, pid)
    end
end


cluster_table = Ref(Dict{Integer,ClusterInfo}())

function addcluster(access_node, nw; kwargs...)

    access_node_args = get(kwargs, :access_node_args, kwargs)
    compute_node_args = get(kwargs, :compute_node_args, kwargs)    

    @info access_node_args
    @info compute_node_args

    master_id = addprocs([access_node]; access_node_args...)

    @everywhere master_id @eval using MPIClusterManagers
    @everywhere master_id @eval using Multicluster

    MPI = get(kwargs, :MPI, true)

    wpids = if MPI 
               Distributed.remotecall_fetch(addprocs, master_id[1], MPIWorkerManager(nw); compute_node_args...)
            else
               # TODO
               throw("not implemented")
            end

    cid = master_id[1]
    cluster_table[][cid] = ClusterInfo(cid, access_node, nw, access_node_args, compute_node_args, [wpids])

    return Cluster(master_id[1])
end


# create another team of worker processes accross the compute nodes of the cluster, but within another MPI context.
# TODO: how to bind the contexts using intercommunicators ?
function addworkers(cluster_handle::Cluster, nw; MPI=true)

    cid = cluster_handle.cid

    wpids = if MPI 
                Distributed.remotecall_fetch(addprocs, cid, MPIWorkerManager(nw); cluster_table[][cid].compute_node_args...)
            else
                # TODO
                throw("not implemented")
            end

    push!(cluster_table[][cid].contexts, wpids)

    # return the index of the new context pids
    xid = length(cluster_table[][cid].contexts)

    return Cluster(cid, xid)
end

# nclusters

clusters() = map(Cluster, collect(keys(cluster_table[])))

nclusters() = length(cluster_table[])

nodes(cluster_handle::Cluster) = map(w->Node(cluster_handle, w), reduce(vcat, filter(!isnothing, cluster_table[][cluster_handle.cid].contexts)))

contexts(cluster_handle::Cluster) = cluster_table[][cluster_handle.cid].contexts 

function Distributed.nworkers(cluster_handle::Cluster) 
    context = !isnothing(cluster_handle.xid)
    nworkers(cluster_handle, Val(context))
end

Distributed.nworkers(cluster_handle::Cluster, _::Val{true}) = map(x -> isnothing(x) ? 0 : length(x), cluster_table[][cluster_handle.cid].contexts)

Distributed.nworkers(cluster_handle::Cluster, _::Val{false}) = Distributed.remotecall_fetch(nworkers, cluster_handle.cid)


# TODO: return handles to refer to the cluster nodes (integers ?)
#function nodes(cluster_handle::Cluster)

#end

# TODO: number of cluster nodes 
# number of nodes must be informed by the access node (how?)
#function nnodes(cluster_handle::Cluster)

#end

function Distributed.workers(cluster_handle::Cluster) 
    context = !isnothing(cluster_handle.xid)
    workers(cluster_handle, Val(context))
end

Distributed.workers(cluster_handle::Cluster, _::Val{false}) = reduce(vcat, filter(!isnothing, cluster_table[][cluster_handle.cid].contexts)) # Distributed.remotecall_fetch(workers, cluster_handle.cid; role=:master)

Distributed.workers(cluster_handle::Cluster, _::Val{true}) = cluster_table[][cluster_handle.cid].contexts[cluster_handle.xid] # Distributed.remotecall_fetch(workers, cluster_handle.cid; role=:master)


Distributed.nprocs(cluster_handle::Cluster) = Distributed.remotecall_fetch(nprocs, cluster_handle.cid; role=:master)
    
Distributed.procs(cluster_handle::Cluster) = Distributed.remotecall_fetch(procs, cluster_handle.cid; role=:master)


# coworkers(cid) ???


function rmcluster(cluster_handle::Cluster)
    cid = cluster_handle.cid
    contexts = cluster_table[][cid].contexts
    wpids = reduce(vcat, filter(!isnothing, contexts); init=[])
    Distributed.remotecall_fetch(rmprocs, cid, wpids)
    rmprocs(cid)
    delete!(cluster_table[], cid)
end

Distributed.rmprocs(cluster_handle::Cluster) = rmcluster(cluster_handle) 

function rmcluster(cluster_handle::Cluster, context_id)
    cid = cluster_handle.cid
    contexts = cluster_table[][cid].contexts
    @assert length(contexts) >= context_id
    @assert !isnothing(contexts[context_id])
    wpids = contexts[context_id]
    Distributed.remotecall_fetch(rmprocs, cid, wpids)
    contexts[context_id] = nothing
end

Distributed.rmprocs(cluster_handle::Cluster, context_id) = rmcluster(cluster_handle, context_id)
