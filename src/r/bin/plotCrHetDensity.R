#!/usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create scatterplots and histograms for CR and het rate

args <- commandArgs(TRUE)
data <- read.table(args[1])
title <- args[2]
scatterPdf <- args[3]
scatterPng <- args[4]
crPng <- args[5]
hetPng <- args[6]

cr <- data$V1
het <- data$V2
#options(device="png") # sets default graphics output; prevents generation of empty PDF files # not needed?

# round CR Phred score to nearest integer in CR histograms (ensures sensible bin boundaries)

make.plot <- function(cr, het, title, type, outPath) {
  # scatterplot and accompanying histograms, on same plot
  if (type=='pdf') { pdf(outPath, paper="a4") }
  else if (type=='png') { png(outPath, width=800,height=800,pointsize=18) }
  layout(matrix(c(1,2,3,4), 2, 2), widths=c(2,1), heights=c(1,2))
  hist(het, col=2, breaks=40, xlab="Autosome heterozygosity rate", main=title, cex.main=1.5) # het rate histogram
  plot(het, cr, col=2, xlab="Autosome heterozygosity rate", ylab="Call Rate (Phred scale)") # main scatterplot
  axis(4, c(10,20,30), c('90%', '99%', '99.9%'), las=1)
  frame() # null plot in upper right corner
  cr.hist <- hist(round(cr), breaks=40, plot=FALSE) # CR histogram, rotated
  barplot(cr.hist$counts, horiz=TRUE, col=2, space=0, names.arg=cr.hist$breaks[0:length(cr.hist$counts)], las=1, xlab="Frequency", cex.names=0.7, ylab="", main="CR (Phred)", cex.main=0.9)
  dev.off()
}

make.plot(cr, het, title, 'pdf', scatterPdf)
make.plot(cr, het, title, 'png', scatterPng)

layout(1)

# cr histogram alone
png(crPng, height=800, width=800, pointsize=18)
hist(round(cr), breaks=40, col=2, xlab="CR (Phred scale)", main=paste(title, "Sample Call Rate"))
graphics.off()

# het histogram alone
png(hetPng, height=800, width=800, pointsize=18)
hist(het, col=2, breaks=40, xlab="Autosome heterozygosity rate", main=paste(title, "Sample Het Rate"))
dev.off()


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

