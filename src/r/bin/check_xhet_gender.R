#! /usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2012

# want to infer sample gender from xhet (x chromosome heterozygosity)
# for mixed male/female samples, expect two distinct populations
# fit a two-component Gaussian mixture model to the data; deduce xhet regions for male/female/ambiguous

# requires mixtools package from CRAN to have been installed
# TODO replace hard-coded library path with argument

# outputs: plot of mixture model, and revised gender_fails.txt file
# summaries written to stdout/stderr, can be redirected to file

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
  # require low chance of sample 'misclassification'; implies clear separation between components
  # also require minimum weight for each component
  # possible cause of failure is a set of samples from only one gender
  # if conditions not met, then exit
  cat("Performing sanity checks on model.\n") 
  n = 100000 # number of points to sample for error assessment
  err <- mix.error(m, mix.sample(m, n))
  cat(paste("Simulated model error:", err, "\n"))
  x <- seq(0,1,length.out=100000)
  if (err > max.err) {
    stop("Simulated model error rate is too high!")
  } else if (min(m$lambda) < min.weight) {
    stop("Model component weights less than minimum value!")
  } else {
    cat("Sanity checks passed.\n")
  }
}

mixmodel.thresholds <- function(xhet.train, boundary.sd, plotPath, title, sanityCheck, clip, trials) {
  # construct mixture model and find m.max, f.min thresholds
  total <- length(xhet.train)
  if (clip>0 & total >= 100) { # clip away high values; female has mostly low xhet for some chips (eg. exome)
    xhet.train <- sort(xhet.train)
    remove <- round(total*clip)
    xhet.train <- xhet.train[0:(total-remove)] # do not update total; xhet.train used for model training only
  }
  # import "mixtools" package and train mixture model
  library(mixtools)
  cat("Constructing 2-component mixture model.\n")
  m <- consensus.model(xhet.train, trials)
  cat(summary(m))
  cat(paste("loglik_final", signif(m$loglik, 8), "\n")) # use to parse loglikelihood from output
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
  mix.plot(m, plotPath, title, m.max, f.min, i.m, i.f) # write plot of mixture model
  if (sanityCheck) {
    n <- 10000
    max.err <- 0.025
    min.weight <- 0.15
    sanity.check(m, n, min.weight, max.err)
  }
  return(c(m.max, f.min))
}

zero.thresholds <- function(xhet, boundary.sd) {
  # find thresholds for high population with xhet exactly zero
  m.max <- 0.05 # corresponds to a 5% calling error in male samples which "should" have zero xhet
  non.male <- xhet[xhet>=m.max]
  f.min <- mean(non.male) - boundary.sd*sd(non.male)
  if (f.min < m.max) { f.min <- m.max }
  return(c(m.max, f.min))
}

find.thresholds <- function(xhet.train, boundary.sd, plotPath, title, sanityCheck, clip, trials) {
  # find appropriate m.max, f.min thresholds for xhet
  zero.boundary.sd <- 5
  zeroCount <- length(xhet.train[xhet.train<=0.001]) # 'zeroes' could have a very small number of xhet hits
  smallCount <- length(xhet.train[xhet.train<=0.05])
  total <- length(xhet.train)
  if (zeroCount/total >= 0.1 & (smallCount-zeroCount)/total <= 0.1) {
    # large population with xhet (almost) zero and few close outliers; mixture model won't work
    cat(paste(signif(zeroCount/total, 4), "of samples have negligible xhet; omitting mixture model.\n"))
    thresholds <- zero.thresholds(xhet.train, zero.boundary.sd)
  } else { # train and apply mixture model
    thresholds <- mixmodel.thresholds(xhet.train, boundary.sd, plotPath, title, sanityCheck, clip, trials)
  }
  return(thresholds)
}

########################################################

args <- commandArgs(TRUE)
data <- read.table(args[1], header=TRUE) # read sample_xhet_gender.txt with standard column headers
textPath <- args[2]
plotPath <- args[3]
title <- args[4]
sanityCheck <- as.logical(args[5]) # convert string 'TRUE' or 'FALSE' to boolean value
clip <- as.numeric(args[6]) # proportion of high values to clip; can be zero; recommend 0.5%
trials <- as.numeric(args[7])  #10 # number of trials for consensus; TODO read from command line args
boundary.sd <- 3 # number of standard deviations for max male / min female boundaries

thresholds <- find.thresholds(data$xhet, boundary.sd, plotPath, title, sanityCheck, clip, trials)
m.max <- thresholds[1]
f.min <- thresholds[2]
cat(paste('Max_xhet_M', signif(m.max,4), "\n"))
cat(paste('Min_xhet_F', signif(f.min,4), "\n"))

### compute gender assignments for all input xhet values ###
cat("### Gender model results ###\n")
total <- length(data$xhet) # total number of samples
gender <- rep(0, times=total) # 'blank' vector of genders
gender[data$xhet<=m.max] <- 1
gender[data$xhet>=f.min] <- 2
# old sample_xhet_gender.txt format has a 'supplied' column after 'inferred'; can add elsewhere
data.new <- data.frame('sample'=data$sample,'xhet'=round(data$xhet,8),'inferred'=gender)

# output summary to stdout and new table to file
ambig <- length(subset(gender, gender==0))
cat(paste('Total_samples', total, "\n"))
cat(paste('Ambiguities', ambig, "\n"))
cat(paste('Ambiguity_rate ', signif(ambig/total,4), "\n", sep=''))
conflict <- length(subset(data.new$inferred, data.new$inferred!=data.new$supplied))
cat(paste('Conflicts', conflict, "\n"))
cat(paste('Conflict_rate ', signif(conflict/total,4), "\n", sep=''))
write.table(data.new, textPath, sep="\t", quote=FALSE, row.names=FALSE)

