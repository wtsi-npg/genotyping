#! /usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

args <- commandArgs(TRUE)
data <- read.table(args[1])
total <- as.numeric(args[2]) # total number of failures
experiment <- args[3]
outPath <- args[4] # TODO fix argument order
single.causes <- data$V1
single.counts <- data$V2
#causes = c("Call rate", "Duplicate", "Gender", "Heterozygosity", "Identity (Sequenom)") # long cause names
png(outPath, width=800,height=800,pointsize=18)
par(mar=c(5.1, 10.1, 8.1, 2.1)) # increase left & top margins
barplot(rev(single.counts), names.arg=rev(single.causes), col=2, las=1, xlab="Total failures of QC criterion", main=paste(experiment, "\nIndividual causes of sample failure\n", sep=""), horiz=TRUE)
axis(3, c(0:10)*0.1*total, c(0:10)*10)
mtext("% of failed samples", 3, line=2)

