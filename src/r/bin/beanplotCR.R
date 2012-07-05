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
qmax <- 40 # max CR quality for plot
crMax <- 1 - 10**(-qmax/10) # truncate call rate if > Q40 (could have 100% CR on few SNPs)
cr <- data$V2
cr[cr>crMax] <- crMax
q <- -10*log10(1 - cr) # call rate, converted to phred scale
png(outpath, height=800, width=800, pointsize=18)
library(beanplot)
beanplot(q~plate, horizontal=TRUE, las=1, col=c(3,1), ylim=c(0,40), xlab="Call rate (Phred scale)", main=paste("Sample CR by plate: ", title, "\n\n" ), log="")
# extra "\n\n" is a hack to correct spacing; log="" prevents inappropriate attempts to take logs
axis(3, at=c(0,10,20,30,40), labels=c('No data', '90%', '99%', '99.9%', '99.99%')) # alternate scale on top
dev.off()
