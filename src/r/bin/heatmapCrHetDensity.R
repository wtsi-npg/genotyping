#!/usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create heatmap of populations in the (cr, het) plane
# requires input from cr_het_heatmap script

args <- commandArgs(TRUE)
data <- as.matrix(read.table(args[1]))
title <- args[2]
hetMin <- as.numeric(args[3])
hetMax <- as.numeric(args[4])
pdfPath <- args[5]
pngPath <- args[6]

make.plot <- function(data, title, hetMin, hetMax, type, outPath) {
    if (type=='pdf') { pdf(outPath, paper="a4") }
    else if (type=='png') { png(outPath, width=800,height=800,pointsize=18) }
    layout(matrix(c(1,2), 1, 2),  widths=c(4,1))
    x = c(0:nrow(data)) # note that 'image' function transposes x and y wrt original matrix
    y = c(0:ncol(data))
    image(y, x, log2(1+t(data)), col=c("#000000", topo.colors(100) ), xlab="Autosome heterozygosity rate", ylab="Call rate", main=paste(title, ": Sample density map"), xaxt='n', yaxt='n')
# custom y axes; y axis is already on Phred scale
    axis(2, c(0,10,20,30,40), c("No data", "90%", "99%", "99.9%", "99.99%"), las=1, cex.axis=0.8) # custom y axes
    axis(4, c(0:8)*5, las=1, cex.axis=0.8)
# custom x axis for het rate; original scale is bin count, from 0 to (hetMax-hetMin)
    hetStepTotal <- 10
    hetStepWidth <- (hetMax - hetMin) / hetStepTotal # space between labelled points on het scale
    hetMarks <- c(0:hetStepTotal)*(max(y)/hetStepTotal) # positions of axis marks, wrt original scale
    hetLabels <- c(0:hetStepTotal)*hetStepWidth + hetMin # axis labels
    hetLabels <- round(hetLabels, 3)
    axis(1, hetMarks, hetLabels, cex.axis=0.8) # het rate scale

    # create heatmap colour scale; original scale is integers (0:100)
    image(c(1), c(0:100), t(as.matrix(c(1:100))), col=c("#000000", topo.colors(100) ), xaxt='n' , yaxt='n', ylab='Sample count', xlab='')
    countStepTotal <- 10
    countStepWidth <- log2(max(data)+1) / countStepTotal # find width wrt log2(1+z) rescaling
    countMarks <- c(0:countStepTotal)*10
    countLabels <- 2**(c(0:countStepTotal)*countStepWidth) -1 # apply inverse of rescaling to get  labels
    countLabels <- round(countLabels, 0)
    axis(2, countMarks, countLabels, las=1)
    dev.off()
}


make.plot(data, title, hetMin, hetMax, 'pdf', pdfPath)
make.plot(data, title, hetMin, hetMax, 'png', pngPath)

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
