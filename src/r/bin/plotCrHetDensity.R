#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create scatterplots and histograms for CR and het rate

args <- commandArgs(TRUE)
data <- read.table(args[1])
title <- args[2]
scatterPng <- args[3]
crPng <- args[4]
hetPng <- args[5]

cr <- data$V1
het <- data$V2
options(device="png") # sets default graphics output; prevents generation of empty PDF files

# scatterplot and accompanying histograms, on same plot
png(scatterPng, height=800, width=800, pointsize=18)
layout(matrix(c(1,2,3,4), 2, 2), widths=c(2,1), heights=c(1,2))
hist(het, col=2, breaks=40, xlab="Autosome heterozygosity rate", main=title, cex.main=1.5) # het rate histogram
plot(het, cr, col=2, pch=3, xlab="Autosome heterozygosity rate", ylab="Call Rate (Phred scale)") # main scatterplot
axis(4, c(10,20,30), c('90%', '99%', '99.9%'), las=1)
frame() # null plot in upper right corner
cr.hist <- hist(cr, breaks=40, plot=FALSE) # CR histogram, rotated
barplot(cr.hist$counts, horiz=TRUE, col=2, space=0, names.arg=cr.hist$breaks[0:length(cr.hist$counts)], las=1, xlab="Frequency", cex.names=0.7, ylab="", main="CR (Phred)", cex.main=0.9)
graphics.off()
layout(1)

# cr histogram alone
png(crPng, height=800, width=800, pointsize=18)
hist(cr, breaks=40, col=2, xlab="CR (Phred scale)", main=paste(title, "Sample Call Rate"))
graphics.off()

# het histogram alone
png(hetPng, height=800, width=800, pointsize=18)
hist(het, col=2, breaks=40, xlab="Autosome heterozygosity rate", main=paste(title, "Sample Het Rate"))
dev.off()
