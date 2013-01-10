#! /software/bin/perl

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

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

# Invoke R script to do improved gender check.  
# Input may be in text, json or plink binary format.
# Plink/json input will be converted to text format for R script.
# For plink input, extract x chromosome data to temporary files.

# Output: 
## Gender information in text or JSON format
## Log file
## png plot of mixture model (if any)
## (Can also update internal pipeline database)

use strict;
use warnings;
use Carp;
use File::Temp qw/tempdir/;
use Getopt::Long;
use WTSI::Genotyping::QC::GenderCheck;
use WTSI::Genotyping::QC::GenderCheckDatabase;
use WTSI::Genotyping::QC::PlinkIO qw(checkPlinkBinaryInputs);

my %opts = ();

GetOptions(\%opts, "help", "input=s", "input-format=s", "output-dir=s", 
           "dbfile=s", "json", "title=s", "include-par", "run=s", "default=f", 
           "minimum=f", "boundary=f");

my $dbopts = 1;
%opts = processOptions(\%opts, $dbopts);

my ($namesRef, $inferredRef) = run(%opts);

my $dbFile = $opts{'dbfile'};
my $runName = $opts{'run'};
if ($dbFile) { updateDatabase($namesRef, $inferredRef, $dbFile, $runName); }

