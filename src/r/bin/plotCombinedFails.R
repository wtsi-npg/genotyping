#!/usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

args <- commandArgs(TRUE)
data <- read.table(args[1])
experiment <- args[2]
pdfPath <- args[3]
pngPath <- args[4]
combined.causes <- data$V1
combined.counts <- data$V2
total <- sum(combined.counts)

make.plot <- function(experiment, combined.counts, combined.causes, total,
                      type, outPath) {
  if (type=='pdf') { pdf(outPath, paper="a4") }
  else if (type=='png') { png(outPath, width=800,height=800,pointsize=18) }
  cause.key <- c("C = Call_rate", "D = Duplicate", "G = Gender", "H = Heterozygosity", "I = Probability_of_identity", "M = Magnitude_of_intensity")
  layout(matrix(c(1,2), 2, 1),  heights=c(2,1))
  par(mar=c(5.1, 4.1, 7.1, 2.1)) # increase top margin
  barplot(rev(combined.counts), names.arg=rev(combined.causes), col=2, las=1, horiz=TRUE, xlab="Total failed samples", main=paste(experiment, "\nCombined causes of sample failure\n", sep=""), cex.names=0.8)
  axis(3, c(0:50)*0.02*total, c(0:50)*2)
  mtext("% of failed samples", 3, line=2)
  par(mar=c(1.1, 1.1, 1.1, 1.1)) # decrease margins
  plot(1, type="n", axes=FALSE, xlab="", ylab="",) # empty to contain legend
  par(family="mono", xpd=TRUE); legend("top", cause.key, title="Failure codes", ); par(family="", xpd=FALSE)
  dev.off()
}

make.plot(experiment, combined.counts, combined.causes, total, 'pdf', pdfPath)
make.plot(experiment, combined.counts, combined.causes, total, 'png', pngPath)


# Author: Iain Bancarz <ib5@sanger.ac.uk>

# Copyright (c) 2012, 2016, 2017 Genome Research Limited. All Rights Reserved.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
