#!/software/R-2.11.1/bin/Rscript --vanilla

library("methods")
library("getopt")
library("splots")
suppressPackageStartupMessages(library("gapiUtilities"))

options("bigmemory.allow.dimnames" = TRUE)

## The input file contains columns "SNP" (SNP identifier), "Coor" (SNP
## coordinate, 1-based) and "Alleles" (SNP alleles), followed by a
## number of paired intensity columns. Each of these contains an
## intensity for each channel, for each SNP. The intensity values are
## arranged in paired columns, one for channel A, followed by one for
## channel B. The column names of each pair share a common prefix
## string that can be used to identify which columns belong together.

## Returns a matrix of the intensity data, one column per channel, one
## row per SNP.
readChannelIntensities <- function(file, chunkSize = 5000) {
  readMatrix(file, chunkSize, sep = "\t",
             startRow = 2, startCol = 4, colNameRow = 1, rowNameCol = 1)
}

## Returns the indices of channel A columns.
channelACols <- function(channelMatrix) {
  seq(from = 1, to = ncol(channelMatrix), by = 2)
}

## Returns the indices of channel B columns.
channelBCols <- function(channelMatrix) {
  channelACols(channelMatrix) + 1
}

## Returns the derived names of the samples.
sampleNames <- function(channelMatrix) {
  channelNames <- colnames(channelMatrix)
  mapply(function(i, j) commonPrefix(channelNames[i], channelNames[j]),
         channelACols(channelMatrix), channelBCols(channelMatrix))
}

## Returns plate names parsed from sample names.
plateNames <- function(sNames) {
  sort(unique(sapply(sNames,
                     function(x) unlist(strsplit(x, "_"))[1])))
}

## Returns a matrix of the results of subtracting channel A from
## channel B for each sample. Each column contains the differences for
## one sample.
channelDiffs <- function(channelMatrix) {
  diffs <- mapply(function(i, j) channelMatrix[, j] - channelMatrix[, i],
                  channelACols(channelMatrix), channelBCols(channelMatrix))
  ## If there is only one SNP, mapply coerces to a vector
  if (! is.matrix(diffs)) {
    diffs <- matrix(diffs, nrow = nrow(channelMatrix))
  }
  colnames(diffs) <- sampleNames(channelMatrix)
  diffs
}

plateAnnotation <- function(sampleNames) {
  name <- plateNames(sampleNames)
  len <- length(name)
  pSize <- plateSize(96)
  
  start <- 1:len * pSize - (pSize -1)
  end <- 1:len * pSize
  mid <- start + (end - start) / 2
  colour <- rep(c("grey", "lightgrey"), len, length.out = len)
  data.frame(name = name, start = start, end = end, mid = mid,
             colour = I(colour))
}

cliError <- 11

VERSION <- "0.1.0"

name <- "sample_delta_xy"

desc <- c("Reads an Illuminus intensity input file and plots the mean",
          "difference in intensity between channels A and B across all SNPs",
          "in each sample.")

usage <- paste(name,
               "[-[-help|h]]",
               "[-[-input-file|i] <intensity file>]",
               "[-[-output-file|o] <text file>]",
               "[-[-plot-file|p] <PDF file>]",
               "[-[-version|v]]")

opts = getopt(matrix(c("help",        "h", 0, "logical",
                       "input-file",  "i", 1, "character",
                       "output-file", "o", 1, "character",
                       "plot-file",   "p", 1, "character",
                       "version",     "v", 0, "logical"),
              ncol = 4, byrow = TRUE));

if (! is.null(opts[["help"]])) {
  cat(paste(name, VERSION, "\n\n"))
  cat(desc, "\n", fill = 80)
  cat(paste("Usage:", usage, "\n"))
  q(status = 0)
}

if (! is.null(opts[["version"]])) {
  cat(paste(VERSION, "\n"))
  q(status = 0)
}

if (is.null(opts[["input-file"]])) {
  cat(paste("Usage:", usage, "\n"), file = stderr())
  cat("An --input-file argument is required\n", file = stderr())
  q(status = cliError)
}

if (is.null(opts[["output-file"]]) && is.null(opts[["plot-file"]])) {
  cat(paste("Usage:", usage, "\n"), file = stderr())
  cat(paste("An --output-file and/or",
            "a --plot-file argument is required\n"), file = stderr())
  q(status = cliError)
}


intensityFile <- opts[["input-file"]]
cIntensities <- readChannelIntensities(intensityFile, chunkSize = 10000)
sNames <- sampleNames(cIntensities)
plates <-plateAnnotation(sNames)
cDiffs <- channelDiffs(cIntensities)

mDiffs <- colMeans(cDiffs)
mDiffs <- mDiffs[order(names(mDiffs))]
mean <- mean(mDiffs)
sd <- sd(mDiffs)
sdBounds <- c(-2, -1, 0, 1, 2)
sdValues <- mean + sdBounds * sd

lowOutliers <- mDiffs[mDiffs < sdValues[1]]
highOutliers <- mDiffs[mDiffs > sdValues[5]]

## Write outlier list to a file
if (! is.null(opts[["output-file"]])) {
  outputFile <- opts[["output-file"]]
  names <- sort(c(names(lowOutliers), names(highOutliers)))
  names <- cbind(names, "intensity_outlier")

  write.table(names, outputFile, quote = FALSE, sep = "\t",
              row.names = FALSE, col.names = FALSE)
}

## Plot to a file
if (! is.null(opts[["plot-file"]])) {
  plotFile <- opts[["plot-file"]]
  lineTypes <- c("dotted", "dashed", "longdash", "dashed", "dotted")

  colours <- rep("black", length(mDiffs))
  colours[mDiffs < sdValues[1]] <- "blue"
  colours[mDiffs > sdValues[5]] <- "red"

  sink("/dev/null")
  pdf(file = plotFile, onefile = T)
  plot(1:length(mDiffs), mDiffs, pch = "+",
       main = "Mean differences in intensity between A and B channels",
       sub = "Samples ordered by plate",
       xlab = "Sample",
       ylab = "Mean A-B",
       xlim = c(1, nrow(plates) * plateSize(96)),
       col = colours)

  mapply(function(xmin, xmax, colour) {
    rect(xmin, min(mDiffs), xmax, max(mDiffs), col = colour,
         border = NA)
  }, plates$start, plates$end, plates$colour)

  text(plates$mid, rep(min(mDiffs), nrow(plates)), labels = plates$name,
       cex = 0.8, srt = 90)
  points(1:length(mDiffs), mDiffs, pch = "+", col = colours)

  mapply(function(y, lty) abline(h = y, lty = lty), sdValues, lineTypes)

  legend("topright",
         paste(c("Mean:", "Mean -/+ 1 SD:", "Mean -/+ 2 SD:"),
               c(sprintf("%.3f", mean),
                 sprintf("%.3f, %.3f", sdValues[2],  sdValues[4]),
                 sprintf("%.3f, %.3f", sdValues[1],  sdValues[5]))),
         lty = c("longdash", "dashed", "dotted"), inset = 0.05)

  dev.off()
}

q(status = 0)

