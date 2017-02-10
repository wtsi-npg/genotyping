#!/usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

# based on plotCrPlate.R
# do plate heatmap plot for magnitude of intensity

args <- commandArgs(TRUE)
data <- as.matrix(read.table(args[1]))
title <- args[2]
output <- args[3]
data[data>0 & data<0.8] <- 0.8 # truncate intensity below 0.8
data[data==0] <- 0.78 # missing data
data[data>1.2] <- 1.2 # truncate intensity above 1.2
x = c(0:nrow(data)) # NB 'image' function transposes x and y wrt original matrix
y = c(0:ncol(data))
png(output, height=800, width=800, pointsize=18)
layout(matrix(c(1,2), 2, 1),  heights=c(2,1))
image(x, y, data, col=c("#000000", rainbow(42, end=0.8)), zlim=c(0.78,1.2),  mar=c(1,1,1,1), main=paste("Sample Magnitude:  Plate", title))
image(0.78+(c(0:42)*0.01), c(1), as.matrix(c(1:42)), col=c("#000000", rainbow(41, end=0.8)), ylim=c(0,1), xlab='Normalised magnitude of intensity', yaxt='n', ylab='', mar=c(1,1,1,1))
axis(2, at=0.5, labels=c("No data"), las=1, cex.axis=0.8)
axis(3, at=c(0.8,1.2), labels=c('< 0.8', '> 1.2'))
dev.off()
q(status=0)


# Author: Iain Bancarz <ib5@sanger.ac.uk>

# Copyright (c) 2012, 2016 Genome Research Limited. All Rights Reserved.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
