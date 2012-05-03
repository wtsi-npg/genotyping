#! /usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create beanplot of het rate for all plates
# arguments: input path, title (eg. experiment/analysis), output path
# TODO replace hard-coded library path with argument

args <- commandArgs(TRUE)
data <- read.table(args[1])
title <-  args[2]
outpath <- args[3]
plate <- data$V1
het <-  data$V2
png(outpath, height=800, width=800, pointsize=18)
library(beanplot)
beanplot(het~plate, horizontal=TRUE, las=1,  col=c(3,1), xlab="Autosome heterozygosity rate", main=paste("Sample het rate by plate: ", title ))
dev.off()
