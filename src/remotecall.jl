


function Distributed.pmap(cluster_handle::Cluster, f, c...; kwargs...)
    cid = cluster_handle.cid
    remotecall_fetch(pmap, cid, f, c...; kwargs...)
end

# TODO: modify the @distributed macro in macros.jl
# Multicluster.@distributed(cid, …)

macro distributed_cluster(cluster_handle, args...)
   # esc(quote
   #        @spawnat $cluster_handle.cid @distributed $args...
   #     end)
   esc(Expr(:macrocall, Symbol("@spawnat"), :(#==#), Expr(:., cluster_handle, :(:cid)), Expr(:macrocall, Symbol("@distributed"), :(#==#), args...)))    
end

future_table = Ref(Dict{Future,Integer}())


function Distributed.remotecall(f, node_handle::Node, args...; kwargs...) 
    pid = node_handle.pid
    r = remotecall(() -> remotecall(f, pid, args...; kwargs...), node_handle.cid)
    future_table[][r] = node_handle.cid
    return r
end

function Distributed.remotecall(f, cluster_handle::Cluster, args...; kwargs...) 
    r = remotecall(() -> asyncmap(w -> remotecall(f, w, args...; kwargs...), workers(role=:master)), cluster_handle.cid)
    future_table[][r] = cluster_handle.cid
    return r
end



function Distributed.remotecall_fetch(f, node_handle::Node, args...; kwargs...) 
    pid = node_handle.pid
    remotecall_fetch(() -> remotecall_fetch(f, pid, args...; kwargs...), node_handle.cid)
end


#function gggg(pid,f,args,kwargs) remotecall_fetch(f, pid, args...; kwargs...) end

function Distributed.remotecall_fetch(f, cluster_handle::Cluster, args...; kwargs...) 
    cid = cluster_handle.cid
    @everywhere [cid] @eval using Multicluster
    remotecall_fetch(() -> asyncmap(pid -> remotecall_fetch(f, pid, args...; kwargs...), workers(role=:master)), cid)
#    pids = workers(cluster_handle)
#    @info "..... $args +++ $kwargs"
#    remotecall_fetch(asyncmap, cid, pid -> 109#=remotecall_fetch(f, pid, args...; kwargs...)=#, pids)
 #   remotecall_fetch(asyncmap, cid, abs#=remotecall_fetch(f, pid, args...; kwargs...)=#, pids)
 #   remotecall_fetch(asyncmap, cid, gggg, pids, f, args, kwargs)
end


function Distributed.remotecall_fetch(reducer, f, cluster_handle::Cluster, args...; kwargs...) 
    remotecall_fetch(() -> reduce(reducer, asyncmap(w -> remotecall_fetch(f, w, args...; kwargs...), workers(role=:master))), cluster_handle.cid)
end



function Distributed.remotecall_wait(f, node_handle::Node, args...; kwargs...) 
    pid = node_handle.pid
    r = remotecall_wait(() -> remotecall_wait(f, pid, args...; kwargs...), node_handle.cid)
    future_table[][r] = node_handle.cid
    return r
end

function Distributed.remotecall_wait(f, cluster_handle::Cluster, args...; kwargs...) 
    r = remotecall_wait(() -> asyncmap(w -> remotecall_wait(f, w, args...; kwargs...), workers(role=:master)), cluster_handle.cid)
    future_table[][r] = cluster_handle.cid
    return r
end


# TODO: implement this behaviour directly in the fetch implementation
function fetch_cluster(f)
    if haskey(future_table[], f)
        r = fetch(f)
        asyncmap(x -> @fetchfrom(future_table[][f], fetch(x)), r)
    else
        fetch(f)
    end
end

function fetch_cluster(reducer, f)
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
    remote_do(() -> for w in workers() remote_do(f, w, args...; kwargs...) end, cluster_handle.cid)
end

# @spawn ???


# @spawnat
macro spawnat_cluster(cluster_handle, arg)
  quote
    f = @spawnat($cluster_handle.cid, asyncmap(w->@spawnat(w, $arg), workers(role=:master)))
    $future_table[][f] = $cluster_handle.cid
    f
  end
end

macro spawnat_node(node_handle, arg)
  quote
    f = @spawnat($node_handle.cid, @spawnat($node_handle.pid, $arg))
    $future_table[][f] = $node_handle.cid
    f
  end 
end

# @fetchfrom
macro fetchfrom_cluster(reducer, cluster_handle, arg)
    quote
        @fetchfrom($cluster_handle.cid, reduce($reducer, asyncmap(w->@fetchfrom(w, $arg), workers(role=:master))))
    end
end

macro fetchfrom_node(node_handle, arg)
    quote
      @fetchfrom($node_handle.cid, @fetchfrom($node_handle.pid, $arg))
    end 
end
  
# @everywhere

macro everywhere_cluster(clusters, arg)
    esc(Expr(:call, :perform_everywhere_cluster, clusters, (Expr(:quote, arg))))
#=    quote
        procs = map(c->c.cid, $clusters)
        @everywhere(procs, @everywhere(workers(role=:master), $arg))
    end =#
end

function perform_everywhere_cluster(cluster::Cluster, ex)
    @everywhere [cluster.cid]  @everywhere(workers(role=:master), $ex)
end

function perform_everywhere_cluster(clusters::Vector{Cluster}, ex)
    procs = map(c->c.cid, clusters)
    @everywhere procs @everywhere(workers(role=:master), $ex)
end


