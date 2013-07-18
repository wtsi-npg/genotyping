#! /usr/bin/env perl

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

# exclude samples from pipeline database if they fail given QC thresholds
# use to pre-filter input for zcall and illuminus

use warnings;
use strict;
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::MetricExclusion qw/runFilter/;

my ($resultsPath, $configPath, $dbPath, $logPath, $help);

GetOptions("results=s"  => \$resultsPath,
           "config=s"   => \$configPath,
           "db=s"       => \$dbPath,
           "log=s"      => \$logPath,
           "help"       => \$help);

my $logDefault = "exclude_samples.log";

if ($help) {
    my $helpText = "Usage: $0 [ options ] 

--help     Print this help text and exit
--config   JSON file containing metric names, thresholds, and threshold types 
--results  JSON file containing sample identifiers and metric values
--db       Path to genotyping pipeline SQLite database
--log      Path to log file; defaults to $logDefault

Updates a genotyping database to exclude samples which fail given QC criteria.
";
    print STDERR $helpText;
    exit 0;
}

foreach my $inPath ($resultsPath, $configPath, $dbPath) {
    if (!(-e $inPath)) { die "Input path $inPath does not exist"; }
}
$logPath ||= $logDefault;
runFilter($configPath, $resultsPath, $dbPath, $logPath);
