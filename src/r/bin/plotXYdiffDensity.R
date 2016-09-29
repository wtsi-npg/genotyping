#!/usr/bin/env Rscript

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# June 2012

# create histogram of xy intensity difference metric


args <- commandArgs(TRUE)
data <- read.table(args[1])
title <- args[2]
pngOut <- args[3]

xydiff <- data$V2
options(device="png") # sets default graphics output; prevents generation of empty PDF files

png(pngOut, height=800, width=800, pointsize=18)
hist(xydiff, breaks=40, col=2, xlab="xydiff", main=paste(title, ": XY intensity difference"))
dev.off()


# Author: Iain Bancarz <ib5@sanger.ac.uk>

# Copyright (c) 2012, 2016 Genome Research Limited. All Rights Reserved.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
