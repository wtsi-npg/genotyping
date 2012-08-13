#! /usr/bin/env perl

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

use warnings;
use strict;
use Carp;
use File::Temp qw/tempfile tempdir/;
use FindBin qw /$Bin/;
use JSON;
#use Log::Log4perl;
use plink_binary; # from gftools package
use Exporter;
use WTSI::Genotyping qw/read_sample_json/;
use WTSI::Genotyping::Database::Pipeline;
use WTSI::Genotyping::QC::PlinkIO;
use WTSI::Genotyping::QC::QCPlotShared qw/getDatabaseObject/;
use WTSI::Genotyping::QC::QCPlotTests;


our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/$textFormat $jsonFormat $plinkFormat $ini_path readSampleXhet runGenderModel writeOutput 
readDatabaseGenders updateDatabase/;

use vars qw/$textFormat $jsonFormat $plinkFormat $nameKey $xhetKey $inferKey $supplyKey $ini_path/;
($textFormat, $jsonFormat, $plinkFormat) = qw(text json plink);
($nameKey, $xhetKey, $inferKey, $supplyKey) = qw(sample xhet inferred supplied);
$ini_path = "$Bin/../etc/";

sub getSuppliedGenderOutput {
    my $suppliedRef = shift;
    my $i = shift;
    my $out = 'NA';
    if ($suppliedRef) { $out = @$suppliedRef[$i]; }
    return $out;
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

sub readDatabaseGenders {
    # read inferred genders from database -- use for testing database update
    # return hash of genders indexed by sample URI (not sample name)
    my $dbfile = shift;
    my $method = shift;
    $method ||= 'Inferred';
    my $db = getDatabaseObject($dbfile);
    my @samples = $db->sample->all;
    my %genders;
    $db->in_transaction(sub {
	foreach my $sample (@samples) {
	    my $sample_uri = $sample->uri;
	    my $gender = $db->gender->find
		({'sample.id_sample' => $sample->id_sample,
		  'method.name' => $method},
		 {join => {'sample_genders' => ['method', 'sample']}},
		 {prefetch =>  {'sample_genders' => ['method', 'sample']} });
	    $genders{$sample_uri} = $gender->code;
	}
			});
    $db->disconnect();
    return %genders;
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
    # extract X chromosome data from plink files and write to temporary directory
    # then read sample name/xhet data
    my ($plinkPrefix, $includePar) = @_;
    my $tempDir = tempdir(CLEANUP => 1);
    my $pb = extractChromData($plinkPrefix, $tempDir); # extract X data and get plink_binary reading object
    my ($nameRef, $suppliedRef) = getSampleNamesGenders($pb);
    my $snpLogPath ||=  $tempDir."/snps_gender_check.txt";
    open(my $log, ">", $snpLogPath) || croak "Cannot open log path $snpLogPath: $!";
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
    # read parallel list of names and xhet values (instead of a hash) to preserve name order
    # also read supplied gender, for PLINK input only (otherwise return an undefined value)
    my ($input, $inputFormat) = @_;
    my ($nameRef, $xhetRef, $suppliedRef);
    if ($inputFormat eq $textFormat) {
	($nameRef, $xhetRef) = readNamesXhetText($input);
    } elsif ($inputFormat eq $jsonFormat) {
	($nameRef, $xhetRef) = readNamesXhetJson($input);
    } elsif ($inputFormat eq $plinkFormat) {
	($nameRef, $xhetRef, $suppliedRef) = readPlink($input); 
    } else {
	croak "Illegal input format $inputFormat";
    }
    return ($nameRef, $xhetRef, $suppliedRef);
}

sub runGenderModel {
    # run R script to infer gender
    # return list of inferred genders, in same order as input names
    my ($namesRef, $xhetRef, $paramsRef) = @_;
    my ($sanityCancel, $clip, $trials, $title, $modelOutputDir) = @$paramsRef;
    my $tempFile = writeSampleXhetTemp($namesRef, $xhetRef);
    my $scratchDir = tempdir(CLEANUP => 1);
    my $textPath = $scratchDir."/sample_xhet_gender_model_output.txt";
    my $pngPath = $modelOutputDir."/sample_xhet_gender.png";
    my $logPath = $modelOutputDir."/sample_xhet_gender.log";
    my $sanityOpt;
    if ($sanityCancel) { $sanityOpt='FALSE'; }
    else { $sanityOpt='TRUE'; }
    my $cmd = join(' ', ("check_xhet_gender.R", $tempFile, $textPath, $pngPath, $title, $sanityOpt, $clip, $trials, ">& ".$logPath) ); # $cmd uses csh redirect
    #my ($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommand($cmd, \*STDOUT);
    system($cmd);
    my @inferred = readModelGenders($textPath);
    return @inferred;
}

sub updateDatabase {
    # update pipeline database with inferred genders
    # assume that sample names are given in URI format
    my ($uriRef, $gendersRef, $dbfile, $runName) = @_;
    my @uris = @$uriRef;
    my @genders = @$gendersRef;
    my %genders;
    for (my $i=0;$i<@uris;$i++) {
	$genders{$uris[$i]} = $genders[$i];
    }
    my $db = getDatabaseObject($dbfile);
    my $inferred = $db->method->find({name => 'Inferred'});
    my $run = $db->piperun->find({name => $runName});
    unless ($runName) {
	croak "Run '$runName' does not exist. Valid runs are: [" .
	    join(", ", map { $_->name } $db->piperun->all) . "]\n";
    }
    # transaction to update sample genders
    my @datasets = $run->datasets->all;
    foreach my $ds (@datasets) {
	my @samples = $ds->samples->all;
	$db->in_transaction(sub {
	    foreach my $sample (@samples) {
		my $sample_uri = $sample->uri;
		my $genderCode = $genders{$sample_uri};
		unless (defined($genderCode)) { 
		    croak "Error: Cannot find gender for sample \"$sample_uri\""; 
		    #$genderCode = -1;
		}
		my $gender;
		if ($genderCode==1) { $gender = $db->gender->find({name => 'Male'}); }
		elsif ($genderCode==2) { $gender = $db->gender->find({name => 'Female'}); }
		elsif ($genderCode==0) { $gender = $db->gender->find({name => 'Unknown'}); }
		else { $gender = $db->gender->find({name => 'Not Available'}); }
		$sample->add_to_genders($gender, {method => $inferred});
	    }
			    });
    }
    $db->disconnect();
    return 1;
}

sub writeOutput {
    # write output in .txt or .json format
    my ($namesRef, $xhetRef, $inferredRef, $suppliedRef, $format, $outputDir, $outputName) = @_;
    my @names = @$namesRef;
    my @xhet = @$xhetRef;
    my @inferred = @$inferredRef;
    if ($format eq $textFormat) { $outputName ||= "sample_xhet_gender.txt"; }
    elsif ($format eq $jsonFormat) {  $outputName ||= "sample_xhet_gender.json"; }
    else { croak "Illegal format argument: $format"; }
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
    if ($total != @xhets) { die "Name and xhet list arguments of different length: $!";  }
    for (my $i=0;$i<$total;$i++) {
	print $fh "$names[$i]\t$xhets[$i]\n";
    }
    close $fh;
    return $filename;
}
