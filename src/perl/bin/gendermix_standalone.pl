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
# January 2013

# Script to run gender check independently of WTSI genotyping pipeline
# Excludes all references to internal pipeline database

use strict;
use warnings;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::GenderCheck;

my %opts = ();
GetOptions(\%opts, "help", "input=s", "input-format=s", "output-dir=s", 
           "json", "title=s", "include-par", "default=f", 
           "minimum=f", "boundary=f");
%opts = processOptions(\%opts);
run(%opts);
