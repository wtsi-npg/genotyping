#! /software/R-2.14.1/bin/Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2012

#
# Copyright (c) 2012 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

#########################################################
# script to infer sample gender from xhet (x chromosome heterozygosity)
# for mixed male/female samples, expect two distinct populations
# fit a two-component Gaussian mixture model to the data; deduce xhet regions for male/female/ambiguous

#########################################################
# PREREQUISITES
# mixtools package from CRAN must be installed, and specified in R_LIBS_USER environment variable

# USAGE:  check_xhet_gender.R $INPUT_PATH $TEXT_OUTPUT $PNG_OUTPUT $TITLE $SANITY_CHECK $CLIP $TRIALS
# arguments:
#$INPUT_PATH input file in correct format (see below)
#$TEXT_OUTPUT, $PNG_OUTPUT  Paths for text and graphics output, respectively
#$TITLE  Title of analysis for plots (spaces must be escaped/quoted correctly)
#$SANITY_CHECK  Must be TRUE or FALSE, recommend TRUE
#$CLIP  Any number between 0 and 1, recommend 0.05
#$TRIALS  Any positive integer, recommend 10
# example:
#check_xhet_gender.R ./sample_xhet_input.txt gender_check.txt gender_check.png my_project TRUE 0.05 10 

# input format: tab-delimited text with first line containing headers (as usual for an R script)
# columns headed 'sample' and 'xhet' contain sample names and X heterozygosity, respectively
# other columns are ignored

# outputs: plot of mixture model, and revised text output
# gender codes for input/output: 0=ambiguous, 1=male, 2=female

########################################################
# functions for evaluation of normal mixture models

mix.sample <- function(normalmix, n) {
  # sample n points from given normal mixture object
  # record component from which each point originated -- use this as 'true' classification
  # return: vectors of components, and x values
  c.total <- length(normalmix$lambda) # total components in mixture
  c.sample <- sample(c.total, n, replace=TRUE, prob=normalmix$lambda) # sample of components
  x.sample <- rep(NA, times=n) # 'blank' sample of values
  for (i in seq(1, c.total)) { # for each mixture component
    x.i <- rnorm(length(c.sample[c.sample==i]), normalmix$mu[i], normalmix$sigma[i])
    x.sample[c.sample==i] <- x.i
  }
  samples <- data.frame('component'=c.sample, 'value'=x.sample)
  return(samples)
}

mix.error <- function (normalmix, samples) {
  # take a samples frame from mix.sample & classify each point with most likely component
  # compare to 'true' components in samples frame and compute error rate
  class <- mix.class(normalmix, samples$value)
  error <- length(class[class!=samples$component])/length(class)
  return(error)
}

mix.class <- function(normalmix, x) {
  # find classification using a mixture model
  # class = component which maximizes (local) probability
  # (local) probability = r * d(x|component)*Pr(component) where d=prob density, r=constant 'small' distance
  # TODO nested for loops are somewhat inefficient, could implement with R's matrix syntax?
  c.total <- length(normalmix$lambda) # total components in mixture
  class <- rep(0, times=length(x))
  for (i in seq(1, length(x))) {
    probs <- rep(0, times=c.total)
    for (j in seq(1, c.total)) {
      probs[j] <- dnorm(x[i], normalmix$mu[j], normalmix$sigma[j])*normalmix$lambda[j]
    }
    if (max(probs)==0) { class[i] <- 0 }
    else { class[i] <- which.max(probs) }
  }
  return(class)
}

mix.plot <- function(m, plotPath, title, m.max, f.min, i.m, i.f) {
  # plot mixture model & legend; m = normalmix object
  png(plotPath, width=800, height=800, pointsize=18)
  layout(matrix(c(1,2), 2, 1),  heights=c(2,1))
  plot.mixEM(m, 2, main2=paste(title, ": Gaussian Mixture for Gender"), xlab2="X heterozygosity", col2=c(2,4))
  abline(v=m.max, lty=2)
  abline(v=f.min, lty=2)
  plot(1, type="n", axes=FALSE, xlab="", ylab="",) # empty plot to contain legend
  par(xpd=TRUE) # allow plotting outside 'figure' area
  names <- c(NA, NA,  "Ambiguous region")
  names[i.m] <- "Male"
  names[i.f] <- "Female"
  legend("top", names, lty=c(1,1,2), col=c(2,4,1) )
  graphics.off()
}

consensus.model <- function(xhet.train, trials) {
  # repeatedly train model and get consensus result
  # deals with unusual cases where algorithm gets "stuck" in a non-optimal likelihood region
  # assume models can be distinguished by final log-likelihood (loglik); look for most common loglik
  # makes an arbitrary choice if two or more logliks are tied
  cat("Finding consensus model from", trials, "independent training runs.\n")
  models <- c()
  loglikes <- c()
  for (i in seq(1, trials)) {
    cat(paste("\t", i, "\n"))
    m <- normalmixEM(xhet.train, k=2)
    models[[i]] <- m
    loglikes[[i]] <- m$loglik
  }
  loglikes <- signif(loglikes, 10) # avoid rounding errors
  cat("Final log-likelihood of trained models:", loglikes, "\n")
  freqs <- table(loglikes) # construct frequency table
  loglik.consensus <- names(freqs)[which.max(freqs)]
  m <- NA
  for (i in seq(1, trials)) {
    if (loglikes[i] == loglik.consensus) {
      m <- models[[i]]
      break()
    }
  }
  return(m)
}

sanity.check <- function(m, n, min.weight, max.err) {
  ### sanity checks on mixture model ###
  # require clear separation between components (low simulated error)
  # also require minimum weight for each component
  # possible cause of failure is a set of samples from only one gender
  # if conditions not met, throw an error (caught by find.thresholds)
  cat("Performing sanity checks on model.\n") 
  err <- mix.error(m, mix.sample(m, n))
  cat(paste("Simulated model error:", err, "\n"))
  if (err > max.err) {
    stop("Simulated model error rate is too high!")
  } else if (min(m$lambda) < min.weight) {
    stop("Model component weights less than minimum value!")
  } else {
    cat("Sanity checks passed.\n")
  }
}

mixmodel.thresholds <- function(xhet.train, boundary.sd, plotPath, title,
                                sanityCheck, clip, trials) {
  # construct mixture model and find m.max, f.min thresholds
  total <- length(xhet.train)
  if (clip>0 & total >= 100) {
    # clip away high values; female has low xhet for some chips (eg. exome)
    xhet.train <- sort(xhet.train)
    remove <- round(total*clip)
    xhet.train <- xhet.train[0:(total-remove)]
    # do not update total; xhet.train used for model training only
  }
  # import "mixtools" package and train mixture model
  library(mixtools)
  cat("Constructing 2-component mixture model.\n")
  m <- consensus.model(xhet.train, trials)
  cat(summary(m))
  cat(paste("loglik_final", signif(m$loglik, 8), "\n")) # parse loglikelihood
  # infer male/female boundaries from model
  i.m <- which.min(m$mu) # indices for male, female components
  i.f <- which.max(m$mu)
  m.max <- m$mu[i.m] + boundary.sd*m$sigma[i.m]
  f.min <- m$mu[i.f] - boundary.sd*m$sigma[i.f]
  if (f.min < m.max) { # SD regions overlap; use midpoint +/- 10%
    dist <- m$mu[i.f]-m$mu[i.m]
    midpoint <- m$mu[i.m]+dist/2 # midpoint between population means
    f.min <- midpoint+0.1*dist
    m.max <- midpoint-0.1*dist
  }
  mix.plot(m, plotPath, title, m.max, f.min, i.m, i.f) # write PNG plot
  if (sanityCheck) {
    n <- 10000
    max.err <- 0.025
    min.weight <- 0.15
    sanity.check(m, n, min.weight, max.err)
  }
  return(c(m.max, f.min))
}

default.thresholds <- function(xhet, default.params)  {
  # find thresholds where high population has negligible xhet
  m.max <- default.params[1]
  f.min.default <- default.params[2]
  boundary.sd <- default.params[3]
  non.male <- xhet[xhet>=m.max]
  if (length(non.male) > 100) { # try to find "ambiguity zone"
    f.min <- mean(non.male) - boundary.sd*sd(non.male)
    if (f.min < m.max) { f.min <- f.min.default }
  } else {
    f.min <- f.min.default
  }
  return(c(m.max, f.min))
}

find.thresholds <- function(xhet, boundary.sd, plotPath, title,
                            sanityCheck, clip, trials, default.params) {
  # find appropriate m.max, f.min thresholds for xhet
  # run mixture model -- in case of error, use default values
  thresholds <- tryCatch({
    thresholds <- mixmodel.thresholds(xhet, boundary.sd, plotPath, title,
                                      sanityCheck, clip, trials)
  }, error = function(err) {
    cat(paste("WARNING:  ",err))
    thresholds <- c(0,0)
  }
                         )
  if (all(thresholds==0)) {
    cat("Applying default thresholds.\n")
    xhet <- args[0]
    thresholds <- default.thresholds(xhet, default.params)
  }
  return(thresholds)
}

write.thresholds <- function(m.max, f.min, thresh.path) {
  sink(thresh.path, append=FALSE, split=FALSE)
  cat("M_max\t", m.max, "\n", sep="")
  cat("F_min\t", f.min, "\n", sep="")
  sink()
}

########################################################

args <- commandArgs(TRUE)
data <- read.table(args[1], header=TRUE) # sample_xhet_gender.txt
textPath <- args[2]
plotPath <- args[3]
threshPath <- args[4]
title <- args[5]
sanityCheck <- as.logical(args[6]) # convert string 'TRUE' or 'FALSE' to boolean
clip <- as.numeric(args[7]) # high values to clip; can be zero; recommend 0.5%
trials <- as.numeric(args[8])  # number of trials for consensus; recommend 10
boundary.sd <- 3 # standard deviations for max male / min female boundaries

xhet <- data$xhet
default.params <- c(0.02, 0.03, 5) # defaults for: mMax, fMin, boundary.sd

thresholds <- find.thresholds(xhet, boundary.sd, plotPath, title,
                              sanityCheck, clip, trials, default.params)
m.max <- thresholds[1]
f.min <- thresholds[2]
cat(paste('Max_xhet_M', signif(m.max,4), "\n"))
cat(paste('Min_xhet_F', signif(f.min,4), "\n"))
write.thresholds(m.max, f.min, threshPath);

### compute gender assignments for all input xhet values ###
cat("### Gender model results ###\n")
total <- length(data$xhet) # total number of samples
gender <- rep(0, times=total) # 'blank' vector of genders
gender[data$xhet<=m.max] <- 1
gender[data$xhet>=f.min] <- 2
data.new <- data.frame('sample'=data$sample,'xhet'=round(data$xhet,8),'inferred'=gender)

# output summary to stdout and new table to file
ambig <- length(subset(gender, gender==0))
cat(paste('Total_samples', total, "\n"))
cat(paste('Ambiguities', ambig, "\n"))
cat(paste('Ambiguity_rate ', signif(ambig/total,4), "\n", sep=''))
# finding gender conflicts requires supplied gender, which may not be present
# instead record inferred/supplied genders in write_gender_files.pl
write.table(data.new, textPath, sep="\t", quote=FALSE, row.names=FALSE)

