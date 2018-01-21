require(ggplot2)

plot.nodes <- function(data, events, colname, scale){
  
  # create plot of epoch time vs metric column
  p <- ggplot(data, aes(epoch, get(colname)))
   
  # axis labels and plot title
  p <- p + labs(x = "seconds from start")
  #p <- p + labs(y = "node metrics")
  p <- p + labs(title = paste("Application Effect on",colname, sep=" "))
  
  # make it line graph
  p <- p + geom_line()
  #p <- p + geom_area(fill = "lightblue")  
  
  # split graphs by node name
  p <- p + facet_grid(node ~ .) 
  
  # settle y axis at 0 to 100 for percentage columns
  if (scale == "%") {
    p <- p + expand_limits(y = c(0,100))
  }
  p <- p + expand_limits(x = -5) # allow room for application marker
  
  # remove margins to get more real estate
  p <- p + scale_x_continuous(expand = c(0, 0))
  p <- p + scale_y_continuous(expand = c(0, 0))    

  
  # set margin between each plot panel to get more real estate
  p <- p + theme(panel.margin = unit(1, "lines"))
  
  # black on white
  p <- p + theme_bw()
  p <- p + theme(plot.background = element_blank()
                ,panel.grid.major = element_blank()
                ,panel.grid.minor = element_blank()
                ,panel.border = element_blank()
                ,axis.text.y=element_text(size=4)
                ,axis.title.y=element_blank()
                ,axis.title.x=element_text(size=6)
                ,axis.text.x=element_text(size=6))
  
  # disable legend to get more real estate
  p <- p + theme(legend.position="none")
  
  p <- p + theme(axis.ticks.y = element_blank())
  if (scale == "%") {
    p <- p + expand_limits(y = 101) 
  }
  
  # tart up the node labels
  p <- p + theme(strip.background = element_blank(), strip.text.y = element_text(colour = "blue", angle = 90, size = 10, hjust = 0.5, vjust = 0.5))
  
  # split global, stage and node events
  globalEvents <- events[events$node == "global",]
  stageEvents <- events[events$node == "stage",]
  nodeEvents <- events[events$node != "global" & events$node != "stage" ,]
  
  # height of annotation line (assuming start is 0)
  ann_top <- max(data[,colname])
  
  # add global events e.g. start of stages

  if (nrow(globalEvents) > 0) {
     for (i in 1:nrow(globalEvents)){
           # indicator lines across all plots
           #p <- p + annotate("segment",
           #                  x = globalEvents$epoch[i],
           #                  xend = globalEvents$epoch[i],
           #                  y = 0,
           #                  yend = ann_top,
           #                  colour = "red")
           # text
           p <- p + annotate("text",
                              x = globalEvents$epoch[i],
                              y = 0,
                              angle = 90,
                              size = 3,
                              hjust = 0,
                              colour = "red",
                              label = globalEvents$event[i])
     }
  }

  colours = c("pink", "orange")
  if (nrow(stageEvents) > 0) {
    for (i in 1:nrow(stageEvents)){
      # indicator lines across all plots
      p <- p + annotate("rect",
                        xmin = stageEvents$epoch[i],
                        xmax = stageEvents$epoch[min(i+1,nrow(stageEvents))],
                        ymin = -Inf,
                        ymax = Inf,
                        alpha = 0.2,
                        fill = colours[2 - i%%2]
      )
      # text
      p <- p + annotate("text",
                        x = stageEvents$epoch[i],
                        y = 0,
                        angle = 90,
                        size = 3,
                        hjust = 0,
                        colour = "darkgreen",
                        label = stageEvents$event[i])
    }
  }

  # add node events e.g. executor added

  if (nrow(nodeEvents) > 0) {
       # indicator lines
       # p <- p + geom_segment(data = nodeEvents,
       #                      mapping = aes(x = epoch,
       #                                    xend = epoch,
       #                                    y = 0,
       #                                    yend = ann_top,
       #                                  colour = "blue"))
    
       # text
       p <- p + geom_text(data = nodeEvents,
                          mapping = aes(x = epoch,
                                        y = 0,
                                        label = event,
                                        angle = 90,
                                        hjust = 0),
                          size = 3,
                          colour = "blue")
  }
  
  return(p)
}

# main

#
# read cluster metrics data and convert Epoch to offset from time 0
#
data <- read.csv("../data/cm.csv", stringsAsFactors = FALSE)
earliestEpoch <- min(data$epoch)
data$epoch <- sapply(data$epoch, function(x) x - earliestEpoch)
data$CPU <- (100-data$idl)
data$MEMUSED <- ((data$used/(data$free+data$buff+data$cach))*100)
data$DISK <- (data$writ+data$read)
data$NETWORK <- (data$recv+data$send)
#
# read events data and convert Epoch to same offset
#
events <- read.csv("../data/ev.csv", stringsAsFactors = FALSE)
events$epoch <- sapply(events$epoch, function(x) x - earliestEpoch)

#
# Save and Show plots
#
w <- 20
h <- length(unique(data$node))
p <- plot.nodes(data,events,"CPU", "%")
ggsave(filename='../data/CPU.svg', plot=p, width = w, height = h)
print("CPU plot saved to ../data/CPU.svg")
if (Sys.info()['sysname'] == "Windows") win.graph() else x11()
plot(p)

p <- plot.nodes(data,events,"MEMUSED", "%")
ggsave(filename='../data/MEMUSED.svg', plot=p, width = w, height = h)
if (Sys.info()['sysname'] == "Windows") win.graph() else x11()
print("MEMUSED plot saved to ../data/MEMUSED.svg")
plot(p)

p <- plot.nodes(data,events,"DISK", "v")
ggsave(filename='../data/DISK.svg', plot=p, width = w, height = h)
print("DISK plot saved to ../data/DISK.svg")
if (Sys.info()['sysname'] == "Windows") win.graph() else x11()
plot(p)

p <- plot.nodes(data,events,"NETWORK", "v")
ggsave(filename='../data/NETWORK.svg', plot=p, width = w, height = h)
print("NETWORK plot saved to ../data/NETWORK.svg")
if (Sys.info()['sysname'] == "Windows") win.graph() else x11()
plot(p)

# End