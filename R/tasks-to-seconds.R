library(igraph)

el<-read.table("../data/edges-tasks-to-seconds.txt",sep="\t",header=F)
g<-graph.data.frame(el,directed=F)

simplify(g,remove.multiple=T,remove.loops=T)

# http://igraph.org/c/doc/igraph-Layout.html


#lo=layout.kamada.kawai
#lo=layout.reingold.tilford
#lo=layout.graphopt
#lo=layout.mds
lo=layout.lgl
#lo=layout.sugiyama
#lo=layout.random
#lo=layout.star
#lo=layout.drl
#lo=layout_with_fr
#lo=layout_as_tree(g,circular=T)
#lo=layout
#lo=layout_nicely
#lo=layout.fruchterman.reingold(g,niter=1000)

vsize  <- ifelse(substr(V(g)$name,1,1)=="T",2,15)
colour <- ifelse(substr(V(g)$name,1,1)=="T","white","green")
fontsize <- ifelse(substr(V(g)$name,1,1)=="T", 0.1, 0.9)
label <- ifelse(substr(V(g)$name,1,1)=="T", "", V(g)$name)
par(mar=c(0,0,1,0))

plot(g
    ,vertex.label = label
    ,vertex.size  = vsize
    ,vertex.label.cex = fontsize
    ,vertex.label.dist=0.5
    ,edge.width=0.8
    ,vertex.color=colour
    ,main="Tasks to Seconds"
    ,layout=lo
)