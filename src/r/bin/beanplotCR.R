#! /usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# create beanplot of call rate for all plates
# arguments: input path, output path,  title (eg. experiment/analysis),
# requires beanplot package from CRAN to have been installed

args <- commandArgs(TRUE)
data <- read.table(args[1])
title <-  args[2]
outpath <- args[3]
plate <- data$V1
qmax = 40 # max CR quality for plot
q <- -10*log10(1 - data$V2) # call rate, converted to phred scale
q[q>qmax] <- qmax # truncate call rate if > Q40 (!)
png(outpath, height=800, width=800, pointsize=18)
library(beanplot)
beanplot(q~plate, horizontal=TRUE, las=1, col=c(3,1), ylim=c(0,40), xlab="Call rate (Phred scale)", main=paste("Sample CR by plate: ", title, "\n\n" )) # extra "\n\n" is a hack to correct spacing; TODO fix this properly
axis(3, at=c(0,10,20,30,40), labels=c('No data', '90%', '99%', '99.9%', '99.99%')) # alternate scale on top
dev.off()
