
# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

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

# Module to do gender inference from x chromosome heterozygosity
# Does input/output processing and acts as front-end for R script implementing mixture model
# See GenderCheckDatabase.pm for internal pipeline database functions

use warnings;
use strict;
use Carp;
use File::Temp qw/tempfile tempdir/;
use FindBin qw /$Bin/;
use JSON;
use plink_binary; # from gftools package
use Exporter;
use WTSI::Genotyping qw/read_sample_json/;
use WTSI::Genotyping::QC::PlinkIO;

our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/$ini_path $helpText $helpTextDB processOptions run
  diffGenders readBenchmark readGenderOutput/;

use vars qw/$textFormat $jsonFormat $plinkFormat $nameKey $xhetKey $inferKey 
  $supplyKey $ini_path $helpText $helpTextDB/;
($textFormat, $jsonFormat, $plinkFormat) = qw(text json plink);
($nameKey, $xhetKey, $inferKey, $supplyKey) = qw(sample xhet inferred supplied);
$ini_path = "$Bin/../etc/";

my %defaultParams = ('m-max-default' => 0.02,
                     'm-max-minimum' => 0.005,
                     'boundary' => 3,
                     'output-dir' => '.',
                     'json' => 0,
                     'dbfile' => 0,
                     'title' => 'Untitled',
                     'include-par' => 0,
    );

$helpText =  "Usage: $0 [ options ] 

--help                 Print this help text and exit

Input/output options:
--input=PATH           Path to text/json input file, OR prefix for binary Plink 
                       files (without .bed, .bim, .fam extension).  Required.
--input-format=FORMAT  One of: $textFormat, $jsonFormat, $plinkFormat.  
                       Optional; if absent, will be deduced from input filename.
--output-dir=PATH      Path to output directory.  Defaults to current directory.
--include-par          Include SNPs from pseudoautosomal regions.  Plink input 
                       only; may increase apparent x heterozygosity of male 
                       samples.
--json                 Output in .json format, instead of tab-delimited text

Gender model options:
--title=STRING         Title for plots and other output
--m-max-default=FLOAT  Default value for M_max, the maximum male 
                       heterozygosity, in the event of model error.  
                       Default: ".$defaultParams{'m-max-default'}."
--m-max-minimum=FLOAT  Minimum permitted value for M_max.  
                       Default: ".$defaultParams{'m-max-minimum'}."
--boundary=FLOAT       Number of standard deviations from population means, 
                       for boundary of ambiguous region.  
                       Default: ".$defaultParams{'boundary'}."
";

$helpTextDB = $helpText."
Options for WTSI genotyping pipeline internal database:
--dbfile=PATH          Push results to given pipeline database file (in 
                       addition to writing text/json output). Optional.
--run=NAME             Name of pipeline run to update in pipeline database.

";

sub getSuppliedGenderOutput {
    my $suppliedRef = shift;
    my $i = shift;
    my $out = 'NA';
    if ($suppliedRef) { $out = @$suppliedRef[$i]; }
    return $out;
}

sub processOptions {
    # validate command-line options for either database or non-database script
    my ($optRef, $dbopts) = @_;
    my %opts = %$optRef;
    my $input = $opts{'input'};
    if ($opts{'help'}) {
        if ($dbopts) { print STDERR $helpTextDB; }
        else { print STDERR $helpText; }
        exit(0);
    } elsif (!$input) {
        print STDERR "Input data must be specified!\n".
            "Run with --help for additional usage information.\n";
        exit(1);
    } elsif ($opts{'dbfile'} && !(-r $opts{'dbfile'})) {
        croak "ERROR: Cannot read database path: ".$opts{'dbfile'};
    }
    my $inputFormat;
    if ($opts{'input-format'}) {
        $inputFormat = $opts{'input-format'};
        if ($inputFormat ne $textFormat && 
            $inputFormat ne $jsonFormat && 
            $inputFormat ne $plinkFormat) {
            croak "ERROR: Input format must be one of: ".
                "$textFormat, $jsonFormat, $plinkFormat";
        }
    } elsif ($input =~ /\.txt$/) { 
        $inputFormat = $textFormat; 
    } elsif ($input =~ /\.json$/) {
        $inputFormat = $jsonFormat;
    } else {
        $inputFormat = $plinkFormat; 
    }
    $opts{'input-format'} = $inputFormat;
    if ($inputFormat eq $plinkFormat) { 
        if (not checkPlinkBinaryInputs($input)) { 
            croak "ERROR: Plink binary input files not available"; 
        }
    } elsif (not -r $input) {
        croak "ERROR: Cannot read input path $input";
    }
    if ($opts{'json'}) { $opts{'output-format'} = $jsonFormat; }
    else { $opts{'output-format'} = $textFormat; }
    foreach my $key (keys(%defaultParams)) {
        if (!defined($opts{$key})) {
            $opts{$key} = $defaultParams{$key};
        }
    }
    return %opts;
}

sub readModelGenders {
    # read inferred gender from model output
    my $inPath = shift;
    my @inferred = ();
    open my $in, "<", $inPath || croak "Cannot read input path $inPath";
    my $first = 1;
    while (<$in>) {
	if ($first) { $first=0; next; } # skip header line
	my @words = split;
	push(@inferred, $words[2]);
    }
    close $in;
    return @inferred;
}

sub readNamesXhetJson {
    # read sample names and xhet from .json file
    # typically use module $nameKey, $xhetKey variables
    my ($inPath) = @_;
    $nameKey ||= "sample";
    $xhetKey ||= "xhet";
    my @records = read_sample_json($inPath);
    my @names = ();
    my @xhets = ();
    foreach my $recordRef (@records) {
	my %record = %$recordRef;
	push(@names, $record{$nameKey});
	push(@xhets, $record{$xhetKey});
    }
    return (\@names, \@xhets);
}

sub readPlink {
    # extract X chromosome data from plink files, write to temporary directory
    # then read sample name/xhet data
    my ($plinkPrefix, $includePar) = @_;
    my $tempDir = tempdir(CLEANUP => 1);
    # extract X data and get plink_binary reading object
    my $pb = extractChromData($plinkPrefix, $tempDir); 
    my ($nameRef, $suppliedRef) = getSampleNamesGenders($pb);
    my $snpLogPath ||=  $tempDir."/snps_gender_check.txt";
    open(my $log, ">", $snpLogPath) || 
        croak "Cannot open log path $snpLogPath: $!";
    my %hetRates = findHetRates($pb, $nameRef, $log, $includePar);
    close $log;
    my @xhets;
    foreach my $name (@$nameRef) {
	push(@xhets, $hetRates{$name});
    }
    return ($nameRef, \@xhets, $suppliedRef);
}

sub readNamesXhetText {
    # read sample_xhet_gender.txt file; tab-delimited, first two columns are name and xhet, first line is header
    my $inPath = shift;
    open my $in, "<", $inPath || croak "Cannot open input file $inPath: $!";
    my $line = 0;
    my @names = ();
    my @xhets = ();
    while (<$in>) {
	if ($line==0) { $line++; next; } # first line is header
	chomp;
	my @words = split;
	push(@names, $words[0]);
	push(@xhets, $words[1]);
    }
    close $in || croak "Cannot close input file $inPath: $!";
    return (\@names, \@xhets);
}

sub readSampleXhet {
    # read parallel list of names and xhet values (preserves name order)
    # also read supplied gender, for PLINK input only
    my ($input, $inputFormat, $includePar) = @_;
    my ($nameRef, $xhetRef, $suppliedRef);
    if ($inputFormat eq $textFormat) {
        ($nameRef, $xhetRef) = readNamesXhetText($input);
    } elsif ($inputFormat eq $jsonFormat) {
        ($nameRef, $xhetRef) = readNamesXhetJson($input);
    } elsif ($inputFormat eq $plinkFormat) {
        ($nameRef, $xhetRef, $suppliedRef) = readPlink($input, $includePar); 
    } else {
        croak "Illegal input format $inputFormat";
    }
    return ($nameRef, $xhetRef, $suppliedRef);
}

sub run {
    # 'main' method to run gender check
    # return sample names and inferred genders for possible database update
    my %opts = @_;
    my ($namesRef, $xhetsRef, $suppliedRef) 
        = readSampleXhet($opts{'input'}, 
                         $opts{'input-format'},
                         $opts{'include-par'});
    my @modelParams = ();
    my @keys = qw/output-dir title m-max-default m-max-minimum boundary/;
    foreach my $key (@keys) { push(@modelParams, $opts{$key}); }
    my @inferred = runGenderCheckR($namesRef, $xhetsRef, \@modelParams);
    writeOutput($namesRef, $xhetsRef, \@inferred, $suppliedRef, 
            $opts{'output-format'}, $opts{'output-dir'});
    return ($namesRef, \@inferred);
}

sub runGenderCheckR {
    # run R script to infer gender
    # return list of inferred genders, in same order as input names
    my ($namesRef, $xhetRef, $paramsRef) = @_;
    my ($modelOutputDir, $title, $m_max_default, $m_max_minimum, $boundary_sd) 
        = @$paramsRef;
    my $tempFile = writeSampleXhetTemp($namesRef, $xhetRef);
    my $scratchDir = tempdir(CLEANUP => 1);
    my $textPath = $scratchDir."/sample_xhet_gender_model_output.txt";
    my $pngPath = $modelOutputDir."/sample_xhet_gender.png";
    my $threshPath = $modelOutputDir."/sample_xhet_gender_thresholds.txt";
    my $logPath = $modelOutputDir."/sample_xhet_gender.log";
    my $cmd = join(' ', ("check_xhet_gender.R", $tempFile, $textPath, $pngPath, 
                         $threshPath, $logPath, $title, $m_max_default, 
                         $m_max_minimum, $boundary_sd) ); 
    system($cmd) == 0 or confess("system '$cmd' failed: $?");

    my @inferred = readModelGenders($textPath);
    return @inferred;
}

sub writeOutput {
    # write output in .txt or .json format
    my ($namesRef, $xhetRef, $inferredRef, $suppliedRef, $format, $outputDir, $outputName) = @_;
    my @names = @$namesRef;
    my @xhet = @$xhetRef;
    my @inferred = @$inferredRef;
    if ($format eq $textFormat) { 
        $outputName ||= "sample_xhet_gender.txt"; 
    } elsif ($format eq $jsonFormat) {  
        $outputName ||= "sample_xhet_gender.json"; 
    } else { 
        croak "Illegal format argument: $format"; 
    }
    my $outPath = $outputDir."/".$outputName;
    open (my $out, ">", $outPath) || croak "Cannot open output file $outPath!";
    if ($format eq $textFormat) {
        print $out join("\t", qw(sample xhet inferred supplied))."\n";
        for (my $i=0;$i<@names;$i++) {
            my $supplied = getSuppliedGenderOutput($suppliedRef, $i);
            printf $out "%s\t%6f\t%d\t%s\n", ($names[$i], $xhet[$i], $inferred[$i], $supplied);
        }
    } elsif ($format eq $jsonFormat) {
        my @records = ();
        for (my $i=0;$i<@names;$i++) {
            my %record;
            $record{$nameKey} = $names[$i];
            $record{$xhetKey} = $names[$i];
            $record{$inferKey} = $inferred[$i];
            $record{$supplyKey} = getSuppliedGenderOutput($suppliedRef, $i);
            push(@records, \%record);
        }
        print $out encode_json(\@records);
    }
    close $out || croak "Cannot close output file $outPath!";
    return 1;
}

sub writeSampleXhetTemp {
    # write given sample names and xhets to temporary file
    my ($namesRef, $xhetRef) = @_;
    my ($fh, $filename) = tempfile(UNLINK => 1); # remove tempfile on exit
    my @names = @$namesRef;
    my @xhets = @$xhetRef;
    my $total = @names;
    my @header = qw/sample xhet/;
    print $fh join("\t", @header)."\n";
    if ($total != @xhets) { 
        die "Name and xhet list arguments of different length: $!";  
    }
    for (my $i=0;$i<$total;$i++) {
        print $fh "$names[$i]\t$xhets[$i]\n";
    }
    close $fh;
    return $filename;
}

#################################################################
# methods for testing

sub diffGenders {
    # compare gender results to benchmark
    my ($benchmarkRef, $inPath) = @_;
    my %benchmark = %$benchmarkRef;
    my $diff = 0;
    my %genders = readGenderOutput($inPath);
    # check gender codes
    foreach my $sample (keys(%genders)) {
        if ($benchmark{$sample} != $genders{$sample}) {
            $diff = 1; 
            print STDERR "Genders differ for sample $sample: benchmark ".
                $benchmark{$sample}.", model ". $genders{$sample}."\n";
            last; 
        }
    }
    # if codes OK, check that sample sets match
    unless ($diff) {
        foreach my $sample (keys(%benchmark)) {
            if (!defined($genders{$sample})) { 
                $diff = 1; 
                print STDERR "Gender not defined for sample $sample\n";
                last; 
            }
        }
    }
    if ($diff==1) {
        print STDERR "Reference and model genders differ; check ".
            "t/gender/sample_xhet_gender.log for possible error messages.\n";
    }
    return $diff;
}

sub readBenchmark {
    my $inPath = shift;
    open my $in, "< $inPath";
    my @lines = ();
    while (<$in>) {
        chomp;
        push(@lines, $_);
    }
    close $in;
    my $ref = decode_json(join('', @lines));
    my %refGenders = %$ref;
    return %refGenders;
}

sub readGenderOutput {
    # read gender codes from .txt or .json output of check_xhet_gender.pl
    my $inPath = shift;
    open my $in, "<", $inPath;
    my %genders;
    if ($inPath =~ /\.txt$/) {
	my $first = 1;
	while (<$in>) {
	    if ($first) { $first = 0; next; } # skip headers
	    my @words = split;
	    my ($sample, $gender) = ($words[0], $words[2]); # fields are: name, xhet, inferred, supplied
	    $genders{$sample} = $gender;
	}
    } elsif ($inPath =~ /\.json$/) {
	my @lines = ();
	while (<$in>) {
	    chomp;
	    push(@lines, $_);
	}
	my $ref = decode_json(join('', @lines));
	my @records = @$ref;
	foreach my $recRef (@records) {
	    my %record = %$recRef;
	    $genders{$record{'sample'}} = $record{'inferred'};
	}
    } else {
	croak "Illegal filename extension: $inPath";
    }
    close $in;
    return %genders;
}

