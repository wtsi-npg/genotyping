#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# June 2012

# create histogram of xy intensity difference metric


args <- commandArgs(TRUE)
data <- read.table(args[1])
title <- args[2]
pngOut <- args[3]

xydiff <- data$V2
options(device="png") # sets default graphics output; prevents generation of empty PDF files

png(pngOut, height=800, width=800, pointsize=18)
hist(xydiff, breaks=40, col=2, xlab="xydiff", main=paste(title, ": XY intensity difference"))
dev.off()
