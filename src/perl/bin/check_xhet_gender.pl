#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2012

# invoke R script to do improved gender check
# script writes revised sample_xhet_gender.txt, and png plot of mixture model, to given output directory

use strict;
use warnings;
use File::Temp qw(tempfile);
use Getopt::Long;
use FindBin qw($Bin);
use WTSI::Genotyping qw(read_sample_json);
use WTSI::Genotyping::QC::QCPlotShared; # must have path to WTSI directory in PERL5LIB
use WTSI::Genotyping::QC::QCPlotTests;

sub readNamesXhetText {
    # read sample_xhet_gender.txt file; tab-delimited, first two columns are name and xhet, first line is header
    my $inPath = shift;
    open IN, "< $inPath" || die "Cannot open input file $inPath: $!";
    my $line = 0;
    my @names = ();
    my @xhets = ();
    while (<IN>) {
	if ($line==0) { $line++; next; } # first line is header
	chomp;
	my @words = split;
	push(@names, $words[0]);
	push(@xhets, $words[1]);
    }
    close IN;
    return (\@names, \@xhets);
}

sub readNamesXhetJson {
    # read sample names and xhet from .json file
    # TODO may need to change $nameKey, $xhetKey defaults
    my ($inPath, $nameKey, $xhetKey) = @_;
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

sub writeSampleXhet {
    # write given sample names and xhets to temporary file
    my ($namesRef, $xhetRef) = @_;
    my ($fh, $filename) = tempfile();
    my @names = @$namesRef;
    my @xhets = @$xhetRef;
    my $total = @names;
    if ($total != @xhets) { die "Name and xhet list arguments of different length: $!";  }
    my $header = "sample\txhet\n";
    print $fh $header;
    for (my $i=0;$i<$total;$i++) {
	print $fh "$names[$i]\t$xhets[$i]\n";
    }
    close $fh;
    return $filename;
}

my ($inPath, $outDir, $title, $help, $prefix, $clip, $trials, $sanityCancel, $sanityOpt, $namesRef, $xhetRef);

GetOptions("input=s"                =>  \$inPath,
	   "output_dir=s"           =>  \$outDir,
	   "title=s"                =>  \$title,
	   "file_prefix=s"          =>  \$prefix,
	   "cancel_sanity_check"    =>  \$sanityCancel,
	   "clip=f"                 =>  \$clip,
	   "trials=i"               =>  \$trials,
	   "h|help"                 =>  \$help);

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Script to do improved gender check by fitting a mixture model to xhet data.

Options:
--input=PATH          Input file in .json or sample_xhet_gender.txt format
--output_dir=PATH     Output directory 
--title=TITLE         Title for model summary plot
--file_prefix=NAME    Prefix for output file names
--cancel_sanity_check Cancel sanity-checking on model
--trials=INTEGER      Number of trials used to obtain consensus mdoel
--help                Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

$inPath ||= './sample_xhet_gender.txt';
$outDir ||= '.';
$title  ||= "Untitled";
$prefix ||= "sample_xhet_gender_model";
$clip   ||= 0.01; # proportion of high xhet values to clip; default to 1%
$trials ||= 20;
$sanityCancel ||= 0;

# read sample names and xhet values from given input
if ($inPath =~ /\.txt$/) {
    ($namesRef, $xhetRef) = readNamesXhetText($inPath);
} elsif ($inPath =~ /\.json$/) {
    ($namesRef, $xhetRef) = readNamesXhetJson($inPath);
} else {
    die "ERROR: Illegal filename extension on $inPath: $!";
}
my $tempName = writeSampleXhet($namesRef, $xhetRef);  # write input for R script

if ($outDir !~ /\/$/) { $outDir .= '/'; }
my $textPath = $outDir.$prefix.'.txt';
my $plotPath = $outDir.$prefix.'.png';
if ($sanityCancel) { $sanityOpt='FALSE'; }
else { $sanityOpt='TRUE'; }
my $summaryPath = $outDir.$prefix.'_summary.txt';
my $cmd = join(' ', ($WTSI::Genotyping::QC::QCPlotShared::RScriptExec, 
		     $Bin.'/'.$WTSI::Genotyping::QC::QCPlotShared::RScriptsRelative.'/check_xhet_gender.R',
		     $tempName, $textPath, $plotPath, $title, $sanityOpt, $clip, $trials, ">& ".$summaryPath) ); 
# $cmd uses csh redirect
my ($tests, $failures) = (0,0);
($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommand($cmd, \*STDOUT, $tests, $failures);
if ($failures == 0) { system("rm $tempName"); exit(0); }
else { exit(1); } # error; keep tempfile for debugging
