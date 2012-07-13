#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

args <- commandArgs(TRUE)
data <- read.table(args[1])
experiment <- args[2]
outPath <- args[3]
combined.causes <- data$V1
combined.counts <- data$V2
total <- sum(combined.counts)
cause.key <- c("C = Call_rate", "D = Duplicate", "G = Gender", "H = Heterozygosity", "I = Identity_with_Sequenom",
               "X = XY_intensity_difference")
png(outPath, width=800,height=800,pointsize=18)
layout(matrix(c(1,2), 2, 1),  heights=c(3,1))
par(mar=c(5.1, 4.1, 7.1, 2.1)) # increase top margin
barplot(rev(combined.counts), names.arg=rev(combined.causes), col=2, las=1, horiz=TRUE, xlab="Total failed samples", main=paste(experiment, "\nCombined causes of sample failure\n", sep=""), cex.names=0.8)
axis(3, c(0:50)*0.02*total, c(0:50)*2)
mtext("% of failed samples", 3, line=2)
par(mar=c(1.1, 1.1, 1.1, 1.1)) # decrease margins
plot(1, type="n", axes=FALSE, xlab="", ylab="",) # empty plot to contain legend
par(family="mono", xpd=TRUE); legend("top", cause.key, title="Failure codes", ); par(family="", xpd=FALSE)
dev.off()
