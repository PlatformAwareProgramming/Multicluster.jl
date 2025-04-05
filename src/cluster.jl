# addcluster

mutable struct ClusterInfo 
    cid::Integer
    access_node_address::String
    nw::Integer
    access_node_args
    compute_node_args
    contexts::Vector{Union{Nothing,Integer}}
end

struct Cluster
    cid::Integer
    Cluster(cid) = (@assert haskey(cluster_table[], cid); new(cid))
end

struct Node
    cid::Integer
    pid::Integer
    function Node(cid, pid)
        @assert haskey(cluster_table[], cid)
        #@assert(@fetchfrom(cid, in(pid, workers(role=:master))))
        try
            b = remotecall_fetch(w -> in(w, workers(role=:master)), cid, pid)
            @assert b
        catch e
            @info e
        end        
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
    @everywhere master_id @eval using Distributed 

    @info "master_id = $master_id"

    MPI = get(kwargs, :MPI, true)

    @info "compute_node_args = $compute_node_args"

    wpids = if MPI 
               remotecall_fetch(addprocs, master_id[1], MPIWorkerManager(nw); compute_node_args...)
            else
               # TODO
               throw("not implemented")
            end

    @info "pids ? $wpids"

    cluster_table[][master_id[1]] = ClusterInfo(master_id[1], access_node, nw, access_node_args, compute_node_args, wpids)

    return Cluster(master_id[1])
end


# create another team of worker processes accross the compute nodes of the cluster, but within another MPI context.
# TODO: how to bind the contexts using intercommunicators ?
function addworkers(cid, nw; MPI=true)

    wpids = if MPI 
                remotecall_fetch(addprocs, master_id[1], MPIWorkerManager(nw), cluster_table[][cid].compute_node_args)
            else
                # TODO
                throw("not implemented")
            end

    push!(cluster_table[][cid].contexts, [wpids])

    # return the index of the new context pids
    length(cluster_table[][cid].contexts)

end

# nclusters

function nclusters()
    length(cluster_table[])
end


function nodes(cluster_handle::Cluster)
    map(w->Node(cluster_handle.cid, w), @fetchfrom cluster_handle.cid workers(role=:master))
end



Distributed.nworkers(cluster_handle::Cluster; context=false) = nworkers(cluster_handle.cid, Val(context))

Distributed.nworkers(cluster_handle::Cluster, _::Val{true}) = map(length, cluster_table[][cluster_handle.cid].contexts)

Distributed.nworkers(cluster_handle::Cluster, _::Val{false}) = sum(nworkers(cluster_handle.cid), Val(false))


# TODO: return handles to refer to the cluster nodes (integers ?)
#function nodes(cluster_handle::Cluster)

#end

# TODO: number of cluster nodes 
# number of nodes must be informed by the access node (how?)
#function nnodes(cluster_handle::Cluster)

#end

function Distributed.nprocs(cluster_handle::Cluster) 
    remotecall_fetch(nprocs, cluster_handle.cid; role=:master)
end
    


function clusters()
    keys(cluster_table[])
end

function Distributed.procs(cluster_handle::Cluster) 
    remotecall_fetch(procs, cluster_handle.cid; role=:master)
end


function Distributed.workers(cluster_handle::Cluster) 
    remotecall_fetch(workers, cluster_handle.cid; role=:master)
end

# coworkers(cid) ???



function rmcluster(cid)
    wpids = remotecall_fetch(workers, cid, role=:master)
    remotecall_fetch(rmprocs, cid, wpids)
    rmprocs(cid)
    delete!(cluster_table[], cid)
end

Distributed.rmprocs(cluster_handle::Cluster) = rmcluster(cluster_handle.cid) 

function rmcluster(cid, context_id)
    @assert length(cluster_table[][cid].contexts >= context_id)
    @assert cluster_table[][cid].contexts[context_id] != nothing
    wpids = cluster_table[][cid].contexts[context_id]
    remotecall_fetch(rmprocs, cid, wpids)
    rmprocs(cid)
    cluster_table[][cid].contexts[context_id] = nothing
end

Distributed.rmprocs(cluster_handle::Cluster, context_id) = rmcluster(cluster_handle.cid, context_id)
