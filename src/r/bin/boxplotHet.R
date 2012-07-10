#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create boxplot of het rate for all plates
# arguments: input path, output path, title (eg. experiment/analysis)

args <- commandArgs(TRUE)
data <- read.table(args[1])
title <-  args[2]
outpath <- args[3]
plate <- data$V1
het <-  data$V2
png(outpath, height=800, width=800, pointsize=18)
boxplot(het~plate, horizontal=TRUE, las=1, xlab="Autosome heterozygosity rate", main=paste("Sample het rate by plate: ", title ))
abline(v=median(het), col=2)
dev.off()
