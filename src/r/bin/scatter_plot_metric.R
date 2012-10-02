#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# October 2012

args <- commandArgs(TRUE)
data <- read.table(args[1]) # metric values
pn <- read.table(args[2]) # plate names
pb <- read.table(args[3], header=TRUE) # plate boundaries
metricName <- args[4]
metricMean <- as.numeric(args[5])
metricSd <- as.numeric(args[6])
metricThresh <- as.numeric(args[7])
sdThresh <- as.logical(args[8]) # boolean; is threshold in standard deviations?
plotNum <- args[9]
plotTotal <- args[10]
outPath <- args[11]
# metricMean, metricSd refer to whole dataset, not just current plates
# can be NA if sdThresh is FALSE

index <- data$V1
metric <- data$V2
pass <- data$V3

sdLimit <- 10
if (sdThresh) { # heterozygosity
  ymin <- metricMean - sdLimit*metricSd
  ymax <- (metricMean + sdLimit*metricSd)*1.1 # allow space for legend
} else { # magnitude, identity, call_rate
  ymin <- 0.8
  ymax <- 1.04
}
metric[metric<ymin] <- ymin
metric[metric>ymax] <- ymax
xmin <- 0
if (sdThresh) {
  xmax = 1.2*max(index)
} else {
  xmax = 1.15*max(index)
}

write.plot <- function(index, metric, pass, pn, pb, metricName, metricMean,
                       metricSd, metricThresh, sdThresh,
                       plotNum, plotTotal, xmin, xmax, ymin, ymax, outPath) {
  #png(outPath, width=myWidth, height=myHeight, pointsize=myPointsize)
  pdf(outPath)
  bottomMargin = 9
  par('mar'=c(bottomMargin,4,4,2)+0.1)
  myTitle = paste("Sample",metricName,"by plate: Plot",plotNum,"of",plotTotal)
  plot(index, metric, type="n",  xlim=c(0,xmax), ylim=c(ymin,ymax),
       xaxt="n", xlab="", ylab=metricName, main=myTitle) # blank plotting area
  axis(1, pn$V2, pn$V1, las=3, cex.axis=0.7) # plate names
  mtext("Plate", side=1, line=bottomMargin - 2)
  shade <- rgb(190, 190, 190, alpha=80, maxColorValue=255)
  shadeTotal = length(pb$Start)
  if (shadeTotal != 0) {
    for (i in 1:shadeTotal) { # shade even-numbered plate areas
      rect(pb$Start[i], ymin, pb$End[i], ymax, density=100, col=shade)
    }
  }
  points(index[pass==1], metric[pass==1],col="blue") # points on top of shading
  points(index[pass==0], metric[pass==0], col="darkred", pch=16)
  if (sdThresh) {
    metricMax <- metricMean+metricThresh*metricSd
    metricMin <- metricMean-metricThresh*metricSd
    abline(h=metricMean, lty=2)
    if (floor(metricThresh) < metricThresh) { foo = floor(metricThresh) }
    else { foo = metricThresh - 1 }
    for (i in 1:foo) {
      abline(h=metricMean+i*metricSd, col="black", lty=3)
      abline(h=metricMean-i*metricSd, col="black", lty=3)
    }
    abline(h=metricMax, col="red", lty=2)
    abline(h=metricMin, col="red", lty=2)
    mt <- metricThresh
    text(max(index), metricMax, paste("mean +", mt, "sd\n"), pos=4, cex=0.6)
    text(max(index), metricMin, paste("mean -", mt, "sd\n"), pos=4, cex=0.6)
    text(max(index), metricMean, "mean\n", pos=4, cex=0.6)    
  } else {
    abline(h=metricThresh, col="red", lty=2)
    if (metricName=='call_rate' || metricName=='magnitude') {
      label="minimum\n"
    } else if (metricName=='identity') {
      label="maximum\n"
    }
    text(max(index), metricThresh, label, pos=4, cex=0.6)
  }
  legend("topright",
         c("Pass/fail threshold for this metric", "Passed all other metrics",
           "Failed at least one other metric"), bg="white",
         pch=c(NA,1,16), col=c("red","blue","darkred"),lty=c(2,NA,NA),cex=0.7)
  dev.off()
}

write.plot(index, metric, pass, pn, pb, metricName, metricMean,
           metricSd, metricThresh, sdThresh,
           plotNum, plotTotal, xmin, xmax, ymin, ymax, outPath)
