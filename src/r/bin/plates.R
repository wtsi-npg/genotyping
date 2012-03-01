#!/software/R-2.11.1/bin/Rscript --vanilla

library("getopt")
library("splots")

suppressPackageStartupMessages(library("gapiUtilities"))

parsePlate <- function(str) {
  unlist(strsplit(str, "_", fixed = TRUE))
}

xyDiffColNames <- function() {
  c("plateName", "plateWell", "studyName", "sampleName", "diff")
}

fullPlate <- function(plates, plateName) {
  emptyWells <- as.matrix(plateWells())
  colnames(emptyWells) <- "plateWell"
  merge(plates[plates$plateName == plateName, ], emptyWells, all = TRUE)
}

cliError <- 11

VERSION <- "0.1.0"

name <- "plate_xy_diff"
desc <- c("Reads a Varinf XYdiff text file and plots a false-colour",
          "representation of the XYdiff values by their plate location,",
          "with black for missing wells. By default, the colour-scale is",
          "mapped to an interval of +/- 6 SD around the mean of all values.",
          "The number of SD may be changed using the -[-range|r] option",
          "with an integer value. The XYdiff values may be scaled before",
          "plotting by using the -[-scale|s] option.")

usage <- paste(name,
               "[-[-help|h]]",
               "[-[-input-file|i] <xy file>]",
               "[-[-output-file|o] <PDF file>]",
               "[-[-range|r]] <integer>]",
               "[-[-scale|s]]")

range <- 6
scale <- FALSE
legend <- "XY diff"

opts = getopt(matrix(c("input-file",  "i", 1, "character",
                       "output-file", "o", 1, "character",
                       "help",        "h", 0, "logical",
                       "range",       "r", 1, "integer",
                       "scale",       "s", 0, "logical"),
  ncol = 4, byrow = TRUE));

if (! is.null(opts[["help"]])) {
  cat(paste(name, VERSION, "\n\n"))
  cat(desc, "\n", fill = 80)
  cat(paste("Usage:", usage, "\n"))
  q(status = 0)
}

if (is.null(opts[["input-file"]])) {
  cat(paste("Usage:", usage, "\n"), file = stderr())
  cat("An --input-file argument is required\n", file = stderr())
  q(status = cliError)
}

if (is.null(opts[["output-file"]])) {
  cat(paste("Usage:", usage, "\n"), file = stderr())
  cat("An --output-file argument is required\n", file = stderr())
  q(status = cliError)
}

if (! is.null(opts[["range"]])) {
  range <- opts[["range"]]
}

if (! is.null(opts[["scale"]])) {
  scale <- TRUE
}

xyfile <- opts[["input-file"]]
pdf <- opts[["output-file"]]

xydiff <- read.table(xyfile, sep = "\t", strip.white = TRUE,
                     colClasses = c("character", "numeric"))

x <- data.frame(t(sapply(xydiff[, 1], parsePlate)), xydiff[, 2])
colnames(x) <- xyDiffColNames()

diffs <- sapply(levels(x$plateName), function(name) fullPlate(x, name)$diff)
rownames(diffs) <- plateWells()

if (scale) {
  diffs <- scale(diffs, scale = TRUE)
  legend <- paste(legend, "Standard Deviations")
}

zrange <- range *  sd(as.vector(diffs), na.rm = TRUE)
plates <- as.data.frame(diffs)
names(plates) <- levels(x$plateName)

## Suppress printing from dev.off
sink("/dev/null")

pdf(file = pdf, onefile = T)
plotScreen(as.list(plates),
           nx = length(plateCols()),
           ny = length(plateRows()),
           zrange = c(- zrange, zrange),
           # fill = c("white", "darkblue"),
           na.fill = "black",
           ncol = 6,
           main = paste("XY diff from ", basename(xyfile)),
           legend.label = legend)
dev.off()

q(status = 0)
