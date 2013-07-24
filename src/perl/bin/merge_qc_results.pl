#! /software/bin/perl

#
# Copyright (c) 2013 Genome Research Ltd. All rights reserved.
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
# July 2013

# Script to merge .json files containing QC metrics
# Use to combine outputs from general pipeline QC and special MAF/het check

use warnings;
use strict;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/mergeJsonResults/;

my $input1;
my $input2;
my $outPath;
my $help;

GetOptions("input1=s"  => \$input1,
           "input2=s"  => \$input2,
           "output=s"  => \$outPath,
           "help"      => \$help);

my $outDefault = "qc_merged.json";
if ($help) {
    my $helpText = "Usage: $0 [ options ] 

--help     Print this help text and exit
--input1   Input file, in the form of qc_results.json as produced by 
           genotyping pipeline QC. Required.
--input2   Input file, in same format as input1 above. Input files must 
           have congruent sample identifiers and disjoint metrics. Required.
--output   Path for .json output. Optional, defaults to $outDefault
";
    print STDERR $helpText;
    exit 0;
}

my @inPaths = ($input1, $input2);
foreach my $inPath (@inPaths) {
    if (!(-e $inPath)) {
        print STDERR "Input file \"$inPath\" does not exist!\n";
        exit 1;
    }
}
$outPath ||= $outDefault;
mergeJsonResults(\@inPaths, $outPath);

