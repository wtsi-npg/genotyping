#!/usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# arguments: input path, plate name, output path
# the create_plate_heatmap_plots script also supplies global min/max arguments, not currently used
# writes a png heatmap of sample autosome het rate for wells on a plate

args <- commandArgs(TRUE)
data <- as.matrix(read.table(args[1]))
title <- args[2]
output <- args[3]
x = c(0:nrow(data)) # note that 'image' function transposes x and y wrt original matrix
y = c(0:ncol(data))
png(output, height=800, width=800, pointsize=18)
layout(matrix(c(1,2), 2, 1),  heights=c(2,1))
image(x, y, data, col=c("#000000", rainbow(50, end=0.8)), zlim=c(0,1),  mar=c(1,1,1,1), main=paste("Sample Autosome Het Rate:  Plate", title))
image(c(0:50)*0.02, c(1), as.matrix(c(1:50)), col=c("#000000", rainbow(50, end=0.8)), ylim=c(0,1), xlab="Sample het rate", yaxt='n', ylab='')
axis(3, at=c(0), labels=c('No data'))
dev.off()
q(status=0)
