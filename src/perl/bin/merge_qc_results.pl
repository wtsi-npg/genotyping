# /usr/bin/env perl

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
# July 2013

# Script to merge .json files containing QC metrics
# Use to combine outputs from general pipeline QC and special MAF/het check

use warnings;
use strict;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/mergeJsonResults/;

my @inPaths;
my $outPath;
my $help;

GetOptions("in=s"  => \@inPaths,
           "out=s" => \$outPath,
           "help"  => \$help);
if ($help) {
    my $helpText = "Usage: $0 [ options ] 

--help     Print this help text and exit
--in       Input files, in the form of qc_results.json as produced by 
           genotyping pipeline QC. Should have congruent sample identifiers and 
           disjoint metrics. Specify exactly twice.
--out      Path for .json output

Example usage: $0 --in qc_foo.json --in qc_bar.json --out qc_merged.json
";
    print STDERR $helpText;
    exit 0;
} elsif (@inPaths!=2) {
    print STDERR "Must specify exactly two input paths!\n";
    exit 1;
} elsif (!(-e $inPaths[0] && -e $inPaths[1])) {
    print STDERR "Input file does not exist!\n";
    exit 1;
} else {
    mergeJsonResults(\@inPaths, $outPath);
}
