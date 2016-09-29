#!/usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# arguments: input path, plate name, output path
# the create_plate_heatmap_plots script also supplies global min/max arguments, not currently used
# writes a png heatmap of CR for wells on a plate

args <- commandArgs(TRUE)
data <- as.matrix(read.table(args[1]))
title <- args[2]
output <- args[3]
data <- -10*log10(1-data) # convert to Phred quality values
data[data>40] <- 40 # in case call rate > Q40 (!)
x = c(0:nrow(data)) # note that 'image' function transposes x and y wrt original matrix
y = c(0:ncol(data))
png(output, height=800, width=800, pointsize=18)
layout(matrix(c(1,2), 2, 1),  heights=c(2,1))
image(x, y, data, col=c("#000000", rainbow(40, end=0.8)), zlim=c(0,40),  mar=c(1,1,1,1), main=paste("Sample Call Rate:  Plate", title))
image(c(0:40), c(1), as.matrix(c(1:40)), col=c("#000000", rainbow(40, end=0.8)), ylim=c(0,1), xlab='Call rate and Phred score', yaxt='n', ylab='', mar=c(1,1,1,1))
axis(3, at=c(0,10,20,30,40), labels=c('No data', '90%', '99%', '99.9%', '99.99%'))
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
