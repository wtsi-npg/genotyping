#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

args <- commandArgs(TRUE)
data <- read.table(args[1], header=TRUE)
hetMean <- as.numeric(args[2]) # mean het rate across all samples (not just failures)
hetMaxDist <- as.numeric(args[3]) # max distance from het mean for samples to pass QC
minCR <- as.numeric(args[4])
experiment <- args[5] # title for plots
pdfFull <- args[6]
pdfDetail <- args[7]
pngFull <- args[8]
pngDetail <- args[9]

names <- data$sample
cr <- data$cr
het <- data$het

d.fail <- abs(data$duplicate-1) # convert 'pass' flag to 'fail'
g.fail <- abs(data$gender-1)
i.fail <- abs(data$identity-1)
m.fail <- abs(data$magnitude-1)

fail.sum <- d.fail + g.fail + i.fail + m.fail

qmax <- 40
q = -10 * log10(1-cr) # convert to phred scale
q[q>qmax] <- qmax # truncate CR better than Q40 (!)
qmin <-  -10 * log10(1-minCR) # minimum CR for QC pass
categories = c("Duplicate", "Gender", "Identity (Sequenom)", "Magnitude", "Multiple/Other") # legend categories

make.plot.full <- function(hetMean, hetMaxDist, minCR, experiment, categories, names, cr, het, d.fail, g.fail, i.fail, m.fail, fail.sum, q, qmin, qmax, type, outPath) {
  if (type=='pdf') { pdf(outPath, paper="a4") }
  else if (type=='png') { png(outPath, width=800,height=800,pointsize=18) }
### First plot:  Show entire CR/Het range
  layout(matrix(c(1,2), 2, 1),  heights=c(3,1))
  par(mar=(c(5, 4, 4, 5) + 0.1), xpd=TRUE)
  ymax = qmax
  plot(subset(het, d.fail==1 & fail.sum==1), subset(q, d.fail==1 & fail.sum==1), ylim=c(0,ymax), xlim=c(min(het), max(het)), col=2, pch=2, ylab="Call rate (Phred scale)", xlab="Autosome heterozygosity rate", main=paste(experiment, ": Failed samples", sep=" ")) # duplicates
  cat("gender fails", length(subset(het, g.fail==1 & fail.sum==1)), "\n")
  points(subset(het, g.fail==1 & fail.sum==1), subset(q, g.fail==1 & fail.sum==1), col=3, pch=3) # gender
  points(subset(het, i.fail==1 & fail.sum==1), subset(q, i.fail==1 & fail.sum==1), col=4, pch=4) # identity
  points(subset(het, m.fail==1 & fail.sum==1), subset(q, m.fail==1 & fail.sum==1), col=6, pch=8) # magnitude
  points(subset(het, fail.sum==0 | fail.sum>1), subset(q, fail.sum==0 | fail.sum>1), col=1, pch=1) # other

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
}

make.plot.detail <- function(hetMean, hetMaxDist, minCR, experiment, categories, names, cr, het, d.fail, g.fail, i.fail, m.fail, fail.sum, q, qmin, qmax, type, outPath) {
### Second plot: Detail of CR/Het "pass region"
  if (type=='pdf') { pdf(outPath, paper="a4") }
  else if (type=='png') { png(outPath, width=800,height=800,pointsize=18) }
  ymax = qmax
  layout(matrix(c(1,2), 2, 1),  heights=c(3,1))
  par(mar=(c(5, 4, 4, 5) + 0.1))
  plot(subset(het, d.fail==1 & fail.sum==1), subset(q, d.fail==1 & fail.sum==1), ylim=c(qmin*0.8,ymax), xlim=c(hetMean-(hetMaxDist*1.2), hetMean+(hetMaxDist*1.2)), col=2, pch=2, ylab="Call rate (Phred scale)", xlab="Autosome heterozygosity rate", main=paste(experiment, "\nFailed samples passing CR/Het filters", sep=" "), xpd=FALSE) # duplicates
  points(subset(het, g.fail==1 & fail.sum==1), subset(q, g.fail==1 & fail.sum==1), col=3, pch=3) # gender 
  points(subset(het, i.fail==1 & fail.sum==1), subset(q, i.fail==1 & fail.sum==1), col=4, pch=4) # identity
  points(subset(het, m.fail==1 & fail.sum==1), subset(q, m.fail==1 & fail.sum==1), col=6, pch=8) # magnitude
  points(subset(het, fail.sum==0 | fail.sum>1), subset(q, fail.sum==0 | fail.sum>1), col=1, pch=1) # other
       
  # draw boundaries of cr/het "pass region" & add labels
  abline(h=qmin, col=2, lty=2, xpd=FALSE) 
  abline(v=hetMean+hetMaxDist, col=2, lty=2, xpd=FALSE)
  abline(v=hetMean-hetMaxDist, col=2, lty=2, xpd=FALSE)
  text(hetMean+hetMaxDist, ymax-2, paste("MAX_HET=\n", round(hetMean+hetMaxDist, 3)), col=2, cex=0.7, pos=2, xpd=TRUE)
  text(hetMean-hetMaxDist, ymax-2, paste("MIN_HET=\n", round(hetMean-hetMaxDist, 3)), col=2, cex=0.7, pos=4, xpd=TRUE)
  text(hetMean+(hetMaxDist*1.3), qmin, paste("MIN_CR = ", round(100*minCR,2), "%", sep=""), col=2, cex=0.7, pos=4, xpd=TRUE)
  axis(4, at=c(0,10,20,30,40), labels=c("0", "90%", "99%", "99.9%", "99.99%"), las=1)
  mtext("Call rate", 4, line=3)
  par(mar=(c(1,1,1,1)*0.1))
  plot(1, type="n", axes=FALSE, xlab="", ylab="",) # empty plot to contain legend
  legend("top", categories, col=c(2,3,4,6,1), pch=c(2,3,4,8,1), title="Failure causes", cex=0.8)
  dev.off()
}

make.plot.full(hetMean, hetMaxDist, minCR, experiment, categories, names, cr, het, d.fail, g.fail, i.fail, m.fail, fail.sum, q, qmin, qmax, 'pdf', pdfFull)

make.plot.full(hetMean, hetMaxDist, minCR, experiment, categories, names, cr, het, d.fail, g.fail, i.fail, m.fail, fail.sum, q, qmin, qmax, 'png', pngFull)

make.plot.detail(hetMean, hetMaxDist, minCR, experiment, categories, names, cr, het, d.fail, g.fail, i.fail, m.fail, fail.sum, q, qmin, qmax, 'pdf', pdfDetail)

make.plot.detail(hetMean, hetMaxDist, minCR, experiment, categories, names, cr, het, d.fail, g.fail, i.fail, m.fail, fail.sum, q, qmin, qmax, 'png', pngDetail)

