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

# Invoke R script to do improved gender check.  Input may be in text or plink binary format.
# For plink input, extract x chromosome data to temporary files and write text input for R script.
# Output: Gender information in text or JSON format; log file; png plot of mixture model (if any)

use strict;
use warnings;
use Carp;
use File::Temp qw/tempdir/;
use Getopt::Long;
use WTSI::Genotyping::QC::GenderCheck;
use WTSI::Genotyping::QC::PlinkIO qw(checkPlinkBinaryInputs);

my ($help, $input, $inputFormat, $outputDir, $dbFile, $json, $title, 
    $includePar, $runName, $m_max_default, $m_max_minimum, $boundary_sd);

GetOptions("h|help"              => \$help,
           "input=s"             => \$input,
           "input-format=s"      => \$inputFormat,
           "output-dir=s"        => \$outputDir,
           "dbfile=s"            => \$dbFile,
           "json"                => \$json,
           "title=s"             => \$title,
           "include-par"         => \$includePar,
           "run=s"               => \$runName,
           "default=f"           => \$m_max_default,
           "minimum=f"           => \$m_max_minimum,
           "boundary=f"          => \$boundary_sd,
    );


if ($help) {
    print STDERR "Usage: $0 [ options ] 
Input/output options:
--input=PATH           Path to text/json input file, OR prefix for binary plink files (without .bed, .bim, .fam extension).  Required.
--input-format=FORMAT  One of: $textFormat, $jsonFormat, $plinkFormat.  Optional; if not supplied, will be deduced from input filename.
--output-dir=PATH      Path to output directory.  Defaults to current working directory.
--include-par          Read SNPs from pseudoautosomal regions.  Plink input only; may increase apparent x heterozygosity of male samples.
--json                 Output in .json format
--dbfile=PATH          Push results to given pipeline database file (in addition to writing text/json output)
--run=NAME             Name of pipeline run to update in pipeline database

Gender model options:
--title=STRING         Title for plots and other output
--default=FLOAT        Default value for M_max, the maximum male heterozygosity
--minimum=FLOAT        Minimum value for M_max
--boundary=FLOAT       Number of standard deviations from population means, for boundary of ambiguous region

Other:
--help                 Print this help text and exit
";
    exit(0);
}

if ($inputFormat) {
    if ($inputFormat ne $textFormat && $inputFormat ne $jsonFormat && $inputFormat ne $plinkFormat) {
	croak "ERROR: Input format must be one of: $textFormat, $jsonFormat, $plinkFormat";
    }
} elsif ($input =~ /\.txt$/) { 
    $inputFormat = $textFormat; 
} elsif ($input =~ /\.json$/) {
    $inputFormat = $jsonFormat;
} else {
    $inputFormat = $plinkFormat; 
}
if ($inputFormat eq $plinkFormat) { 
    if (not checkPlinkBinaryInputs($input)) { 
        croak "ERROR: Plink binary input files not available"; 
    }
} elsif (not -r $input) {
    croak "ERROR: Cannot read input path $input";
}

$outputDir ||= '.';
$json ||= 0; 
$dbFile ||= 0;
$title ||= "Untitled";
$includePar ||= 0;
$m_max_default ||= 0.02;
$m_max_minimum ||= 0.005;
$boundary_sd ||= 3;

my $outputFormat;
if ($json) { $outputFormat = $jsonFormat; }
else { $outputFormat = $textFormat; }

my @modelParams = 
    ($outputDir, $title, $m_max_default, $m_max_minimum, $boundary_sd);

my ($namesRef, $xhetsRef, $suppliedRef) = readSampleXhet($input, $inputFormat, 
                                                         $includePar);
my @inferred = runGenderModel($namesRef, $xhetsRef, \@modelParams);
writeOutput($namesRef, $xhetsRef, \@inferred, $suppliedRef, 
            $outputFormat, $outputDir); 
if ($dbFile) { updateDatabase($namesRef, \@inferred, $dbFile, $runName); }

