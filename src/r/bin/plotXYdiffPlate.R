#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# arguments: input path, output path, plate name, global min, global max
# writes a png heatmap of sample xy intensity difference for wells on a plate
# clips data range to +/- 3 standard deviations from mean, rescaling high/low values to max.

args <- commandArgs(TRUE) 
data <- as.matrix(read.table(args[1]))
plate.name <- args[2]
global.min <- as.numeric(args[3])
global.max <- as.numeric(args[4])
out.path <- args[5]

# create data structures for plot
data[data==0] <- NA # replace zeroes with NA's; must do this *after* taking mean/sd
data[data>global.max] <-  global.max
data[data<global.min] <-  global.min
# TODO: NA's print in white, could use 'breaks' argument to 'image' to make them print in black
x <- c(0:nrow(data)) # note that 'image' function transposes x and y wrt original matrix
y <- c(0:ncol(data))

# open output file and write main plot
png(out.path, height=800, width=800, pointsize=18)
layout(matrix(c(1,2), 2, 1),  heights=c(2,1))
image(x, y, data, col=rainbow(50, end=0.8), zlim=c(global.min, global.max),  mar=c(1,1,1,1), main=paste("Sample mean (y-x) intensity:  Plate", plate.name))
# create colour scale key
image(seq(from=global.min, to=global.max, length.out=50), c(1), as.matrix(c(1:50)), col=rainbow(50, end=0.8), ylim=c(0,1), xlab="Sample mean (y-x) intensity", yaxt='n', ylab='')
axis(3, at=c(global.min, global.max), labels=c(paste("< ", signif(global.min,3)), paste("> ", signif(global.max,3))) ) # round labels to 3 s.f.
dev.off()
q(status=0)
