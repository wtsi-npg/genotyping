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
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/defaultConfigDir/;

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

my ($inPath, $thresholdsPath, $dbPath, $outPath, $logPath, $iniPath,
    $illuminus, $zcall, $help);

GetOptions("in=s"           => \$inPath,
           "thresholds=s"   => \$thresholdsPath,
           "db=s"           => \$dbPath,
           "out=s"          => \$outPath,
           "log=s"          => \$logPath,
           "ini=s"          => \$iniPath,
           "illuminus"      => \$illuminus,
           "zcall"          => \$zcall,
           "help"           => \$help);

my $logDefault = "exclude_samples.log";
my $outDefault = "exclude_samples.json";

if ($help) {
    my $helpText = "Usage: $0 [ options ] 

--help                Print this help text and exit
--thresholds  PATH    JSON file containing metric names and thresholds
                      Optional, default from ini and illuminus/zcall status.
--in          PATH    JSON file containing sample identifiers and metric 
                      values
--out         PATH    Path for JSON output with filter pass/fail results. 
                      Defaults to $outDefault
--db          PATH    Path to genotyping pipeline SQLite database
--log         PATH    Path to log file; defaults to $logDefault
--ini         PATH    Path to .ini file for default threshold location. 
                      Optional, defaults to $DEFAULT_INI
--illuminus           Apply default Illuminus thresholds
--zcall               Apply default zCall thresholds

Updates a genotyping database to exclude samples which fail given QC criteria.
Must specify exactly one of: thresholds, illuminus, zcall
";
    print STDERR $helpText;
    exit 0;
}

my $modes = 0;
if ($thresholdsPath) { $modes++; }
if ($illuminus) { $modes++; }
if ($zcall) { $modes++; }
unless ($modes==1) {
    die "ERROR: Must specify exactly one of: thresholds, illuminus, zcall";
}

unless ($thresholdsPath) {
    $iniPath ||= $DEFAULT_INI;
    my $configDir = defaultConfigDir($iniPath);
    if ($illuminus) {
        $thresholdsPath = $configDir."/illuminus_prefilter.json";
    } elsif ($zcall) {
        $thresholdsPath = $configDir."/zcall_prefilter.json";
    }
}

my @types = qw/results thresholds database/;
my @inPaths = ($inPath, $thresholdsPath, $dbPath);
foreach (my $i=0; $i<@inPaths; $i++) {
    if (!defined($inPaths[$i])) {
        die "No value specified for $types[$i] input path";
    } elsif (!(-e $inPaths[$i])) { 
        die "Input path \"$inPaths[$i]\" for $types[$i] does not exist";
    }
}
$logPath ||= $logDefault;
runFilter($thresholdsPath, $inPath, $dbPath, $outPath, $logPath);
