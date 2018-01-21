#rm(list = ls())

require(statnet)
require(ndtv)
require(tsna)
require(networkDynamic)

# ingest CSV files created by script "build-ndtv.sh"

vdf <- read.csv("../data/vertex-spells.csv",stringsAsFactors=F)
edf <- read.csv("../data/edge-spells.csv",stringsAsFactors=F)

# create network

appNW <- networkDynamic(edge.spells = edf, vertex.spells = vdf)

# some plot settings

sliceparams<-list(start=0,end=max(vdf$terminus),interval=2, aggregate.dur=2,rule="latest")
renderparams<-list(tween.frames=5,show.time=T,verbose=T)
plotparams <- list(bg='white')
transparentwhite <- rgb(1,1,1,0)
semilightblue <- rgb(173/255,216/255,230/255,0.5)
semilightgreen <- rgb(144/255,238/255,144/255,0.5)
semired <- rgb(1,0,0,0.5)

# define different types of vertex and edge for mapping to properties

vertextypes <- c("worker","data","taskLocal","taskNode","taskRack","stage")
edgetypes <- c("taskToNode","taskToData","taskToStage")

vdf$name <- ifelse(vdf$type=="data","",vdf$name)  # remove "datasets" name as this is a hidden/transparent vertex
chartTitle <- vdf$info[1]


# convert generic vertex type "task" to one of three depending on locality

tasktypes <- c("taskLocal","taskNode","taskRack")
localitytypes <- c("PROCESS_LOCAL","NODE_LOCAL","RACK_LOCAL")
names(tasktypes) <- localitytypes
vdf$type <- ifelse(vdf$type=="task",tasktypes[vdf$info],vdf$type)

# convert stage names to include info

vdf$name <- ifelse(vdf$type=="stage", paste(vdf$name,vdf$info,sep=" "), vdf$name)

# set vertex labels to be the names

network.vertex.names(appNW) <- as.vector(vdf$name)

# dynamic attributes (not used because of rendering problems with large network)

activate.vertex.attribute(appNW,'vclass',vdf$type,onset=-Inf,terminus=Inf)
activate.edge.attribute(appNW,'eclass',edf$type,onset=-Inf,terminus=Inf)

vscales <- c(4,0.1,1,1,1,4) # scale factor for different vertex types
names(vscales) <- vertextypes
vdf$size <- vscales[as.vector(vdf$type)]
activate.vertex.attribute(appNW,'vscale',vdf$size,onset=-Inf,terminus=Inf)

vshapes <- c(32,32,3,3,3,6) # shape based on number of sides for different vertex types
names(vshapes) <- vertextypes
vdf$vshapes <- vshapes[as.vector(vdf$type)]
activate.vertex.attribute(appNW,'vshape',vdf$vshapes,onset=-Inf,terminus=Inf)

vcolours <- c("lightgrey",transparentwhite,semilightgreen,semilightblue,semired,"yellow")
#vcolours <- c("lightgrey","purple",semilightgreen,semilightblue,semired,"yellow")
names(vcolours) <- vertextypes
vdf$colour <- vcolours[as.vector(vdf$type)]
activate.vertex.attribute(appNW,'vcolour',vdf$colour,onset=-Inf,terminus=Inf)

vborders <- c("black",transparentwhite,"black","black","black","black")
names(vborders) <- vertextypes
vdf$border <- vborders[as.vector(vdf$type)]
activate.vertex.attribute(appNW,'vborder',vdf$border,onset=-Inf,terminus=Inf)

labelvscales <- c(0.8,0.9,0.1,0.1,0.1,1) # label sizes based on vertex types
names(labelvscales) <- vertextypes
vdf$labelsize <- labelvscales[as.vector(vdf$type)]
activate.vertex.attribute(appNW,'vlabelscale',vdf$labelsize,onset=-Inf,terminus=Inf)

ecolours <- c("lightgrey",transparentwhite,"yellow") # edge colours based on edge types
names(ecolours) <- edgetypes
edf$colour <- ecolours[as.vector(edf$type)]
activate.edge.attribute(appNW,'ecolour',edf$colour,onset=-Inf,terminus=Inf)

# plotting

#plot(appNW,displaylabels=T,usearrows=F)
staticCoords<-network.layout.kamadakawai(appNW,layout.par = list())
activate.vertex.attribute(appNW,'x',staticCoords[,1],onset=-Inf,terminus=Inf)
activate.vertex.attribute(appNW,'y',staticCoords[,2],onset=-Inf,terminus=Inf)

#compute.animation(appNW,slice.par=sliceparams,animation.mode='kamadakawai',verbose=F)

compute.animation(appNW,slice.par=sliceparams,animation.mode='useAttribute',verbose=F)

# this should work in render but breaks with big networks
#vertex.cex='vscale',vertex.col='vcolour',label.cex='labelscale',vertex.sides='vshape',edge.col='ecolour',

# render

saveVideo(render.animation(appNW,displaylabels=T,usearrows=F, render.par=renderparams, slice.par=sliceparams,
                           plot.par=plotparams, 
                           main=chartTitle, ani.options = list(interval=0.5),
                           vertex.rot = 45, label.col = "black", label.pos = 5, pad = 0.6,
                           vertex.cex=function(slice) {vscales[slice%v%'vclass']},
                           vertex.col=function(slice) {vcolours[slice%v%'vclass']},
                           vertex.border=function(slice) {vborders[slice%v%'vclass']},
                           label.cex=function(slice) {labelvscales[slice%v%'vclass']},
                           vertex.sides=function(slice) {vshapes[slice%v%'vclass']},
                           edge.col=function(slice) {ecolours[slice%e%'eclass']}),video.name="../data/spark.mp4",ani.width=1200,ani.height=800)

# ,other.opts="-b: 4000k" 
#render.d3movie(appNW,ani.replay())

# End
