#! /usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create boxplot of xy intensity difference for all plates
# arguments: input path, title (eg. experiment/analysis), output path, 

args <- commandArgs(TRUE)
data <- read.table(args[1])
title <-  args[2]
outpath <- args[3]
plate <- data$V1
xy.diff <-  data$V2
png(outpath, height=800, width=800, pointsize=18)
library(beanplot)
beanplot(xy.diff~plate, horizontal=TRUE, las=1, col=c(3,1), xlab="Mean (y-x) intensity", main=paste("Sample XYdiff by plate: ", title ), log="")
dev.off()
