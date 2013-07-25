#!/usr/bin/env Rscript

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
# fit a two-component Gaussian mixture model to the data
# deduce xhet regions for male/female/ambiguous gender

#########################################################
# Requires mixtools package from CRAN
# Mixtools must be available in R_LIBS_USER environment variable

# input format: tab-delimited text with first line containing headers
# columns headed 'sample' and 'xhet' contain sample names and X heterozygosity, respectively; other columns are ignored

# outputs: plot of mixture model, and revised text output
# gender codes for input/output: 0=ambiguous, 1=male, 2=female

########################################################
# functions for evaluation of normal mixture models

mix.sample <- function(normalmix, n) {
  # sample n points from given normal mixture object
  # record component from which each point originated -- use this as 'true' classification
  # get samples of components, then values
  c.total <- length(normalmix$lambda) # total components in mixture
  c.sample <- sample(c.total, n, replace=TRUE, prob=normalmix$lambda) 
  x.sample <- rep(NA, times=n) # 'blank' sample of values
  for (i in seq(1, c.total)) { # for each mixture component
    x.i <- rnorm(length(c.sample[c.sample==i]), normalmix$mu[i],
                 normalmix$sigma[i])
    x.sample[c.sample==i] <- x.i
  }
  samples <- data.frame('component'=c.sample, 'value'=x.sample)
  return(samples)
}

mix.error <- function (normalmix, samples) {
  # take a samples frame from mix.sample
  # classify each point with most likely component
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
  plot.mixEM(m, 2, main2=paste(title, ": Gaussian Mixture for Gender"),
             xlab2="X heterozygosity", col2=c(2,4))
  abline(v=m.max, lty=2)
  abline(v=f.min, lty=2)
  plot(1, type="n", axes=FALSE, xlab="", ylab="",) # empty plot for legend
  par(xpd=TRUE) # allow plotting outside 'figure' area
  names <- c(NA, NA,  "Ambiguous region")
  names[i.m] <- "Male"
  names[i.f] <- "Female"
  legend("top", names, lty=c(1,1,2), col=c(2,4,1) )
  graphics.off()
}

consensus.model <- function(xhet.train, trials) {
  # repeatedly train model and get consensus result
  # algorithm sometimes gets "stuck" in a non-optimal likelihood region
  # assume models can be distinguished by final log-likelihood (loglik)
  # look for most common loglik
  # makes an arbitrary choice if two or more logliks are tied
  cat("Finding consensus model from", trials, "independent training runs.\n")
  models <- c()
  loglikes <- c()
  for (i in seq(1, trials)) {
    cat(paste("\tTrial", i, "of", trials, "\n"))
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

mixmodel.thresholds <- function(xhet.train, m.max.default, m.max.minimum,
                                boundary.sd, plotPath, title,
                                sanityCheck, trials) {
  # construct mixture model and find m.max, f.min thresholds
  min.inputs <- 100
  outlier.sd <- 5
  trials <- 20
  cat(length(xhet.train), "training points input.\n")
  if (length(xhet.train) >= min.inputs+1) {
    # clip away high xhet outliers, if any
    non.male <- xhet.train[xhet.train>=m.max.default]
    xhet.max <- mean(non.male) + outlier.sd*sd(non.male)
    xhet.smoothed <- subset(xhet.train, xhet.train < xhet.max)
    if (length(xhet.smoothed) >= min.inputs) {
      xhet.train <- xhet.smoothed
    }
  }
  cat("Using", length(xhet.train), "training points after smoothing.\n")
  library(mixtools)
  cat("Constructing 2-component mixture model.\n")
  m <- consensus.model(xhet.train, trials)
  cat(summary(m))
  cat(paste("loglik_final", signif(m$loglik, 8), "\n")) # parse loglikelihood
  i.m <- which.min(m$mu) # indices for male, female components
  i.f <- which.max(m$mu)
  # Infer male/female boundaries from model. Requirements for M_max, F_min:
  # - must both be >= m.max.default
  # - must have M_max <= F_min
  f.min <- max(m$mu[i.f] - boundary.sd*m$sigma[i.f], m.max.minimum)
  m.max <- max(m$mu[i.m] + boundary.sd*m$sigma[i.m], m.max.minimum)
  if (f.min < m.max) { 
    cat("Standard deviations overlap; setting boundary to midpoint.\n")
    midpoint <- m$mu[i.m]+(m$mu[i.f]-m$mu[i.m])/2
    f.min <- midpoint
    m.max <- midpoint
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

default.thresholds <- function(xhet, m.max.default, boundary.sd )  {
  # find thresholds where high population has negligible xhet
  non.male <- xhet[xhet>=m.max.default]
  if (length(non.male) > 100) { # try to find "ambiguity zone"
    f.min <- mean(non.male) - boundary.sd*sd(non.male)
    if (f.min < m.max.default) { f.min <- m.max.default }
  } else {
    f.min <- m.max.default
  }
  return(c(m.max.default, f.min))
}

find.thresholds <- function(xhet, m.max.default, m.max.minimum, boundary.sd,
                            plotPath, title, sanityCheck, trials) {
  # find appropriate m.max, f.min thresholds for xhet
  # run mixture model -- in case of error, use default values
  thresholds <- tryCatch({
    thresholds <- mixmodel.thresholds(xhet, m.max.default, m.max.minimum,
                                      boundary.sd, plotPath, title,
                                      sanityCheck, trials)
  }, error = function(err) {
    cat(paste("WARNING:  ",err))
    thresholds <- c(0,0)
  }
                         )
  if (all(thresholds==0)) {
    cat("Applying default thresholds.\n")
    xhet <- args[0]
    thresholds <- default.thresholds(xhet, m.max.default, boundary.sd)
  }
  return(thresholds)
}

write.thresholds <- function(m.max, f.min, log, thresh.path) {
  thresh <- file(thresh.path, open="wt")
  sink(thresh.path, append=FALSE, split=FALSE)
  cat("M_max\t", m.max, "\n", sep="")
  cat("F_min\t", f.min, "\n", sep="")
  close(thresh)
  sink(log) # redirect stdout/stderr back to log connection
  sink(log, type="message")
}

########################################################

args <- commandArgs(TRUE)
data <- read.table(args[1], header=TRUE) # input sample_xhet_gender.txt
textPath <- args[2] # text output
plotPath <- args[3] # PNG plot
threshPath <- args[4] # thresholds text output
logPath <- args[5]
title <- args[6] # Plot title
m.max.default <- as.numeric(args[7]) # default M_max threshold
m.max.minimum <- as.numeric(args[8]) # minimum M_max threshold
boundary.sd <- as.numeric(args[9]) # standard deviations for adaptive threshold

log <- file(logPath, open="wt")
sink(log) # stdout
sink(log, type="message") # stderr

sanityCheck <- TRUE
trials <- 20 # number of independent trials for consensus model

thresholds <- find.thresholds(data$xhet, m.max.default, m.max.minimum,
                              boundary.sd, plotPath,
                              title, sanityCheck, trials)
m.max <- thresholds[1]
f.min <- thresholds[2]
cat(paste('Max_xhet_M', signif(m.max,4), "\n"))
cat(paste('Min_xhet_F', signif(f.min,4), "\n"))
write.thresholds(m.max, f.min, log, threshPath);

### compute gender assignments for all input xhet values ###
cat("### Gender model results ###\n")
total <- length(data$xhet) # total number of samples
gender <- rep(0, times=total) # 'blank' vector of genders
gender[data$xhet<=m.max] <- 1
gender[data$xhet>=f.min] <- 2
data.new <- data.frame('sample'=data$sample, 'xhet'=round(data$xhet,8),
                       'inferred'=gender)

# output summary to stdout and new table to file
ambig <- length(subset(gender, gender==0))
cat(paste('Total_samples', total, "\n"))
cat(paste('Ambiguities', ambig, "\n"))
cat(paste('Ambiguity_rate ', signif(ambig/total,4), "\n", sep=''))
# finding gender conflicts requires supplied gender, which may not be present
# instead record inferred/supplied genders in write_gender_files.pl

write.table(data.new, textPath, sep="\t", quote=FALSE, row.names=FALSE)

# note that R will automatically close logfile
