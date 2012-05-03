#! /usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

args <- commandArgs(TRUE)
data <- read.table(args[1])
hetMean <- as.numeric(args[2]) # mean het rate across all samples (not just failures)
hetMaxDist <- as.numeric(args[3]) # max distance from het mean for samples to pass QC
minCR <- as.numeric(args[4])
experiment <- args[5] # title for plots
outputFull <- args[6]
outputDetail <- args[7]

names <- data$V1
cr <- data$V2
het <- data$V3
d.fail <- data$V4
g.fail <- data$V5
i.fail <- data$V6

qmax <- 40
q = -10 * log10(1-cr) # convert to phred scale
q[q>qmax] <- qmax # truncate CR better than Q40 (!)
qmin <-  -10 * log10(1-minCR) # minimum CR for QC pass
categories = c("Duplicate", "Gender", "Identity (Sequenom)", "Gender & Identity", "Other") # legend categories

# remove duplicate samples from x/y coordinates
q0 <- subset(q, d.fail==0)
het0 <- subset(het, d.fail==0)

### First plot:  Show entire CR/Het range
png(outputFull, width=800, height=800, pointsize=18)
layout(matrix(c(1,2), 2, 1),  heights=c(3,1))
par(mar=(c(5, 4, 4, 5) + 0.1), xpd=TRUE)
ymax = qmax
plot(subset(het, d.fail==1), subset(q, d.fail==1), ylim=c(0,ymax), xlim=c(min(het), max(het)), col=2, pch=2, ylab="Call rate (Phred scale)", xlab="Autosome heterozygosity rate", main=paste(experiment, ": Failed samples", sep=" ")) # duplicates
points(subset(het0, g.fail==1 & i.fail==0), subset(q0, g.fail==1 & i.fail==0), col=3, pch=3) # gender only
points(subset(het0, g.fail==0 & i.fail==1), subset(q0, g.fail==0 & i.fail==1), col=4, pch=4) # identity only
points(subset(het0, g.fail==1 & i.fail==1), subset(q0, g.fail==1 & i.fail==1), col=6, pch=8) # identity & gender
points(subset(het0, g.fail==0 & i.fail==0), subset(q0, g.fail==0 & i.fail==0), col=1, pch=1) # other
# draw boundaries of cr/het "pass region" & add labels
abline(h=qmin, col=2, lty=2, xpd=FALSE)
abline(v=hetMean+hetMaxDist, col=2, lty=2, xpd=FALSE)
abline(v=hetMean-hetMaxDist, col=2, lty=2, xpd=FALSE)
text(hetMean+hetMaxDist, ymax-2, paste("MAX_HET\n=", round(hetMean+hetMaxDist, 3)), col=2, cex=0.7, pos=4)
text(hetMean-hetMaxDist, ymax-2, paste("MIN_HET\n=", round(hetMean-hetMaxDist, 3)), col=2, cex=0.7, pos=2)
text(max(het)+0.05*(max(het)-min(het)), qmin, paste("MIN_CR = ", round(100*minCR,2), "%", sep=""), col=2, cex=0.7, pos=4, xpd=TRUE)
axis(4, at=c(0,10,20,30,40), labels=c("0", "90%", "99%", "99.9%", "99.99%"), las=1)
mtext("Call rate", 4, line=4)
par(mar=(c(1,1,1,1)*0.1))
plot(1, type="n", axes=FALSE, xlab="", ylab="",) # empty plot to contain legend
legend("top", categories, col=c(2,3,4,6,1), pch=c(2,3,4,8,1), title="Failure causes", cex=0.8)
dev.off()

### Second plot: Detail of CR/Het "pass region"
png(outputDetail, width=800, height=800, pointsize=18)
layout(matrix(c(1,2), 2, 1),  heights=c(3,1))
par(mar=(c(5, 4, 4, 5) + 0.1))
plot(subset(het, d.fail==1), subset(q, d.fail==1), ylim=c(qmin*0.8,ymax), xlim=c(hetMean-(hetMaxDist*1.2), hetMean+(hetMaxDist*1.2)), col=2, pch=2, ylab="Call rate (Phred scale)", xlab="Autosome heterozygosity rate", main=paste(experiment, "\nFailed samples passing CR/Het filters", sep=" "), xpd=FALSE) # duplicates
points(subset(het0, g.fail==1 & i.fail==0), subset(q0, g.fail==1 & i.fail==0), col=3, pch=3) # gender only
points(subset(het0, g.fail==0 & i.fail==1), subset(q0, g.fail==0 & i.fail==1), col=4, pch=4) # identity only
points(subset(het0, g.fail==1 & i.fail==1), subset(q0, g.fail==1 & i.fail==1), col=6, pch=8) # identity & gender
points(subset(het0, g.fail==0 & i.fail==0), subset(q0, g.fail==0 & i.fail==0), col=1, pch=1) # other
# draw boundaries of cr/het "pass region" & add labels
abline(h=qmin, col=2, lty=2, xpd=FALSE) 
abline(v=hetMean+hetMaxDist, col=2, lty=2, xpd=FALSE)
abline(v=hetMean-hetMaxDist, col=2, lty=2, xpd=FALSE)
text(hetMean+hetMaxDist, ymax-2, paste("MAX_HET=", round(hetMean+hetMaxDist, 3)), col=2, cex=0.7, pos=3, xpd=TRUE)
text(hetMean-hetMaxDist, ymax-2, paste("MIN_HET=", round(hetMean-hetMaxDist, 3)), col=2, cex=0.7, pos=3, xpd=TRUE)
text(hetMean+(hetMaxDist*1.3), qmin, paste("MIN_CR = ", round(100*minCR,2), "%", sep=""), col=2, cex=0.7, pos=4, xpd=TRUE)
axis(4, at=c(0,10,20,30,40), labels=c("0", "90%", "99%", "99.9%", "99.99%"), las=1)
mtext("Call rate", 4, line=3)
par(mar=(c(1,1,1,1)*0.1))
plot(1, type="n", axes=FALSE, xlab="", ylab="",) # empty plot to contain legend
legend("top", categories, col=c(2,3,4,6,1), pch=c(2,3,4,8,1), title="Failure causes", cex=0.8)
dev.off()

