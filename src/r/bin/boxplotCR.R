#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create boxplot of call rate for all plates
# arguments: input path, output path, title (eg. experiment/analysis)

args <- commandArgs(TRUE)
data <- read.table(args[1])
title <-  args[2]
outpath <- args[3]
popSizePath <- args[4] # optional path for barplot of plate population sizes
plate <- data$V1
qmax = 40 # max CR quality for plot
q <- -10*log10(1 - data$V2) # call rate, converted to phred scale
q[q>qmax] <- qmax # truncate call rate if > Q40 (!)
png(outpath, height=800, width=800, pointsize=18)
boxplot(q~plate, horizontal=TRUE, las=1, ylim=c(0,40), xlab="Call rate (Phred scale)", main=paste("Sample CR by plate: ", title, "\n\n" )) # extra "\n\n" is a hack to correct spacing; TODO fix this properly
axis(3, at=c(0,10,20,30,40), labels=c('No data', '90%', '99%', '99.9%', '99.99%')) # alternate scale on top
abline(v=median(q), col=2)
dev.off()
# now plot population size for each plate (if output path supplied)
if (!is.na(popSizePath)) {
  png(popSizePath, height=800, width=800, pointsize=18)
  boxplot.cr <- boxplot(q~plate, plot=FALSE)
  barplot(boxplot.cr$n, names.arg=boxplot.cr$names, horiz=TRUE, las=1, xlab="Sample count", main=paste("Total samples per plate: ", title))
  dev.off()
}
