


function Distributed.pmap(cluster_handle::Cluster, f, c...; kwargs...)
    cid = cluster_handle.cid
    remotecall_fetch(pmap, cid, f, c...; kwargs...)
end

# TODO: modify the @distributed macro in macros.jl
# Multicluster.@distributed(cid, …)

macro cluster_distributed(cluster_handle, args...)
   # esc(quote
   #        @spawnat $cluster_handle.cid @distributed $args...
   #     end)
   esc(Expr(:macrocall, Symbol("@spawnat"), :(#==#), Expr(:., cluster_handle, :(:cid)), Expr(:macrocall, Symbol("@distributed"), :(#==#), args...)))    
end

future_table = Ref(Dict{Future,Integer}())


function Distributed.remotecall(f, node_handle::Node, args...; kwargs...) 
    cid = cluster_handle.cid
    pid = node_handle.pid
    r = remotecall(() -> remotecall(f, pid, args...; kwargs...), cid)
    future_table[][r] = cid
    return r
end

function Distributed.remotecall(f, cluster_handle::Cluster, args...; kwargs...) 
    cid = cluster_handle.cid
    wids = workers(cluster_handle)
    r = remotecall(() -> asyncmap(w -> remotecall(f, w, args...; kwargs...), wids), cid)
    future_table[][r] = cid
    return r
end


function Distributed.remotecall_fetch(f, node_handle::Node, args...; kwargs...) 
    cid = cluster_handle.cid
    pid = node_handle.pid
    remotecall_fetch(() -> remotecall_fetch(f, pid, args...; kwargs...), cid)
end

function Distributed.remotecall_fetch(f, cluster_handle::Cluster, args...; kwargs...) 
    cid = cluster_handle.cid
    wids = workers(cluster_handle)
    remotecall_fetch(() -> asyncmap(pid -> remotecall_fetch(f, pid, args...; kwargs...), wids), cid)
end

function Distributed.remotecall_fetch(reducer, f, cluster_handle::Cluster, args...; kwargs...) 
    cid = cluster_handle.cid
    wids = workers(cluster_handle)
    remotecall_fetch(() -> reduce(reducer, asyncmap(w -> remotecall_fetch(f, w, args...; kwargs...), wids)), cid)
end


function Distributed.remotecall_wait(f, node_handle::Node, args...; kwargs...) 
    pid = node_handle.pid
    r = remotecall_wait(() -> remotecall_wait(f, pid, args...; kwargs...), node_handle.cid)
    future_table[][r] = node_handle.cid
    return r
end

function Distributed.remotecall_wait(f, cluster_handle::Cluster, args...; kwargs...) 
    cid = cluster_handle.cid
    wids = workers(cluster_handle)
    r = remotecall_wait(() -> asyncmap(w -> remotecall_wait(f, w, args...; kwargs...), wids), cid)
    future_table[][r] = cid
    return r
end


# TODO: implement this behaviour directly in the fetch implementation
function cluster_fetch(f)
    if haskey(future_table[], f)
        r = fetch(f)
        asyncmap(x -> @fetchfrom(future_table[][f], fetch(x)), r)
    else
        fetch(f)
    end
end

function cluster_fetch(reducer, f)
    if haskey(future_table[], f)
        r = fetch(f)
        reduce(reducer, asyncmap(x -> @fetchfrom(future_table[][f], fetch(x)), r))
    else
        fetch(f)
    end
end

function Distributed.remote_do(f, node_handle::Node, args...; kwargs...) 
    pid = node_handle.pid
    remote_do(() -> remote_do(f, pid, args...; kwargs...), node_handle.cid)
end

function Distributed.remote_do(f, cluster_handle::Cluster, args...; kwargs...) 
    cid = cluster_handle.cid
    wids = workers(cluster_handle)
    remote_do(() -> for w in wids remote_do(f, w, args...; kwargs...) end, cid)
end

# @spawn ???


# @spawnat
macro cluster_spawnat(cluster_handle, arg)
    quote
        cid = $cluster_handle.cid
        wids = workers($cluster_handle)
        f = @spawnat(cid, asyncmap(w->@spawnat(w, $arg), wids))
        $future_table[][f] = cid
        f
    end
end

macro node_spawnat(node_handle, arg)
    quote
        cid = $node_handle.cid
        pid = $node_handle.pid
        f = @spawnat(cid, @spawnat(esc($pid), $arg))
        $future_table[][f] = cid
        f
    end 
end

# @fetchfrom
macro fetchfrom_cluster(cluster_handle, arg)
    esc(quote
        cid = $cluster_handle.cid
        wids = workers($cluster_handle)
        @fetchfrom(cid, asyncmap(w->@fetchfrom(w, $arg), wids))
    end)
end

macro fetchfrom_cluster(reducer, cluster_handle, arg)
    esc(quote
        cid = $cluster_handle.cid
        wids = workers($cluster_handle)
        @fetchfrom(cid, reduce($reducer, asyncmap(w->@fetchfrom(w, $arg), wids)))
    end)
end

macro fetchfrom_node(node_handle, arg)
    esc(quote
        cid = $node_handle.cid
        pid = $node_handle.pid
        @fetchfrom(cid, @fetchfrom(pid, $arg))
    end) 
end
  
# @everywhere

macro cluster_everywhere(clusters, arg)
    esc(Expr(:call, :(Multicluster.perform_cluster_everywhere), clusters, (Expr(:quote, arg))))
end

function perform_cluster_everywhere(cluster_handle::Cluster, ex)
    wids = workers(cluster_handle)
    @everywhere [cluster_handle.cid] @everywhere($wids, $ex)
end

function perform_cluster_everywhere(clusters::Vector{Cluster}, ex)
    procs = map(c->c.cid, clusters)
    contexts = Dict(map(c -> c.cid => workers(c), clusters))
    @everywhere procs begin 
        cid = myid(role=:worker)
        wids = $contexts[cid]
        @everywhere(wids, $ex)
    end
end


