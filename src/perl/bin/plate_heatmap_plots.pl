#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# script to generate heatmap plots of sample CR and het rate, and intensity xydiff, for wells in each input plate
# assume sample names are in the form PLATE_POSITION_SAMPLE-ID
# POSITION is in the form H10 for x=8, y=10
# writes small .txt files containing input values for each plot (in case needed for later reference)

use strict;
use warnings;
use Carp;
use FindBin qw($Bin);
use Getopt::Long;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(parseLabel getPlateLocationsFromPath);
use WTSI::NPG::Genotyping::QC::QCPlotTests;

our $VERSION = '';

my ($dbPath, $iniPath, $mode, $outDir, $help);

GetOptions("dbpath=s"  => \$dbPath,
	   "inipath=s" => \$iniPath,
	   "mode=s"    => \$mode,
	   "out_dir=s" => \$outDir,
	   "h|help"    => \$help);

if ($help) {
    print STDERR "Usage: $0 [ options ]
Script to generate heatmap plots for each sample on a plate surface.
Plots include call rate, autosome heterozygosity, and xy intensity difference.
Appropriate input data must be supplied to STDIN: either sample_cr_het.txt or the *XYdiff.txt file.

Options:
--mode=KEY          Keyword to determine plot type. Must be one of: cr, het, xydiff, magnitude
--dbpath=PATH       Path to SQLite pipeline database, to find plate addresses
--inipath=PATH      Path to .ini file for SQLite pipeline database
--out_dir=PATH      Output directory for plots
--help              Print this help text and exit
Unspecified options will receive default values, with output written to: ./platePlots
";
    exit(0);
}


# mode is a string; one of cr, het, xydiff.
# mode determines some custom options (eg. xydiff scale), also used to construct filenames
# default options
$mode ||= "cr";
$outDir ||= 'platePlots';

if ((!$dbPath) && (!$iniPath)) {
    croak("Must supply at least one of pipeline database path and .ini path!");
}
if ($dbPath && !(-r $dbPath)) {
    croak("Cannot read pipeline database path '", $dbPath, "'");
}
if ($iniPath && !(-r $iniPath)) {
    croak("Cannot read .ini path '", $iniPath, "'");
}

sub getXYdiffMinMax {
    # get min/max for plot range
    # range = median +/- (maximum distance from median, *excluding* most extreme fraction of data)
    my ($exclude, $excl1, $excl2);
    my @sortedResults = @{shift()};
    my $frac = shift();
    $frac ||= 0.01; # default to 1%
    $exclude = int(@sortedResults * $frac); # number of results to exclude from range
    $excl1 = int($exclude/2); # total to remove from low end
    $excl2 = $exclude - $excl1; # remove from high end
    @sortedResults = @sortedResults[$excl1 .. ($#sortedResults - $excl2)];
    return ($sortedResults[0], $sortedResults[-1]);
}

sub makePlots {
    # assume file names are of the form PREFIX_PLATE.txt
    # execute given script with input table, output path, and plate name as arguments
    # supply global min/max as arguments (not used except by XYdiff)
    my ($inputDir, $plotScript, $expr, $prefix, $minMaxArgs) = @_;
    my @paths = glob($inputDir.'/'.$expr);
    my $allPlotsOK = 1;
    foreach my $path (@paths) {
	my %comments = readComments($path);
	my $plate = $comments{'PLATE_NAME'};
	# TODO fail silently for reserved filenames, eg. xydiff_boxplot.txt
	if (not(defined($plate))) {
	    carp("Cannot read plate name from '", $path, "'. Skipping.");
	    next;
	}
	$plate =~ s/\s+/_/msx; # get rid of spaces in plate name (if any)
	my $outPath = $inputDir.'/'.$prefix.$plate.'.png';
	my @args = ($plotScript, $path, $plate);
	if ($minMaxArgs) { push(@args, ($comments{'PLOT_MIN'}, $comments{'PLOT_MAX'})); }
	my @outputs = ($outPath, );
	my $plotsOK = WTSI::NPG::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
	if ($plotsOK==0) { $allPlotsOK = 0; }
    } 
    return $allPlotsOK;
}


sub parseSampleName {
    # parse sample name in PLATE_WELL_ID format
    # WELL is in the form H10 for x=8, y=10
    # silently return undefined values if name not in correct format
    # OBSOLETE as of 2012-07-24; get plate/well info from pipeline DB instead
    my $name = shift;
    my ($plate, $well, $id, $x, $y);
    if ($name =~ m{^[^_]+_[[:upper:]]\d+_\w+}msx) { # check name format, eg some-plate_H10_sample-id
	($plate, $well, $id) = split /_/msx, $name;
	($x, $y) = parseLabel($well);
    }
    return ($plate, $x, $y);
}

sub readData {
    # read from a filehandle; get data values by plate
    # silently omit samples with no known plate location (eg. excluded sample)
    my ($inputRef, $index, $mode, $dbPath, $iniPath) = @_;
    my (%results, @allResults, $plotMin, $plotMax, %names, %plateNames, $name);
    my ($xMax, $yMax, $duplicates) = (0,0,0);
    my $dataOK = 1;
    my %plateLocs = getPlateLocationsFromPath($dbPath, $iniPath);
    while (<$inputRef>) {
        if (m{^\#}msx) { next; } # ignore comments
        chomp;
        my @words = split;
        if (!defined($plateLocs{$words[0]})) { next; }
        my ($plate, $addressLabel) = @{$plateLocs{$words[0]}};
        my ($x, $y) = parseLabel($addressLabel);
        if (!(defined($x) && defined($y))) { next; }
        # clean up plate name by removing illegal characters
        if (not $plateNames{$plate}) {
            $name = $plate;
            $name =~ s/[-\W]/_/msxg;
            if ($names{$name}) {  # plate name not unique after cleanup
                # fix is *not* guaranteed unique, but should be ok 
                $duplicates++;
                $name .= '_'.$duplicates; 
            }
            $names{$name} = 1;
            $plateNames{$plate} = $name;
        } else {
            $name = $plateNames{$plate};
        }
        if ($x > $xMax) { $xMax = $x; }
        if ($y > $yMax) { $yMax = $y; }
        my $result = $words[$index];
        push(@allResults, $result);
        $results{$name}{$x}{$y} = $result;
    }
    @allResults = sort {$a<=>$b} @allResults; # sort numerically
    if ($mode eq 'xydiff') { # special plot range for xydiff
        ($plotMin, $plotMax) = getXYdiffMinMax(\@allResults);
    } else { # default to plot range = data range
        $plotMin = $allResults[0];
        $plotMax = $allResults[-1];
    }
    if (keys(%results)==0) { $dataOK = 0; }
    return ($dataOK, \%results, $xMax, $yMax, $plotMin, $plotMax);
}

sub readComments {
    # read comments from table file header into a hash
    # header lines of the form '# KEY VALUE' ; VALUE may contain spaces!
    my $inPath = shift;
    my %comments = ();
    open my $in, "<", $inPath || croak("Cannot open input path '",
                                       $inPath, "': $!");
    while (<$in>) {
	chomp;
	unless (m{^\#}msx) { next; }
	my @words = split /\s/msx;
	$comments{$words[1]} = join(' ', @words[2..$#words]); # value may contain spaces!
    }
    close $in || croak("Cannot close input path '", $inPath, "'");
    return %comments;
}

sub writeGrid {
    # write table of results to file; could be CR or het rate
    my ($resultsRef, $outDir, $outPrefix, $xMax, $yMax, $commentRef) = @_;
    my %results = %$resultsRef;
    my %comments = %$commentRef; # comments to put in header
    my $plate = $comments{'PLATE_NAME'};
    $plate =~ s/\s+/_/msx; # get rid of spaces in plate name (if any)
    my $outPath = $outDir."/".$outPrefix.$plate.".txt";
    my @keyList = keys(%comments);
    @keyList = sort(@keyList);
    open my $out, ">", $outPath || croak("Cannot open output path '",
                                         $outPath, "': $!");
    foreach my $key (@keyList) { print $out "# $key $comments{$key}\n"; }
    for (my $y=1; $y<=$yMax; $y++) { # x, y counts start at 1
        my @row = ();
        for (my $x=1; $x<=$xMax; $x++) {
            my $result = $results{$x}{$y};
            unless (defined($result)) { $result = 0; }
            push (@row, $result);
        }
        print $out join("\t", @row)."\n";
    }
    close $out || croak("Cannot close output path '", $outPath, "'");
    return 1;
}

sub writePlateData {
    # for each plate, generate (x,y) grids of data (cr, het rate, xydiff) by sample; write to small files
    # grids form input to r script that does plotting
    # also supply plate name and min/max range for plot across all plates as comments
    my ($dataRef, $prefix, $xMax, $yMax, $outDir, $min, $max) = @_; # will append plate name to prefix
    my %data = %$dataRef;
    if (not -e $outDir) {
      mkdir($outDir) || croak("Failed to create output directory '",
                              $outDir, "'");
    }
    elsif (not -d $outDir) { croak("'", $outDir, "' is not a directory"); }
    elsif (not -w $outDir) { croak("Directory '", $outDir,
                                   "' is not writable"); }
    foreach my $plate (keys(%data)) {
	my %comments = (
	    PLATE_NAME => $plate,
	    PLOT_MIN => $min,
	    PLOT_MAX => $max,
	);
	writeGrid($data{$plate}, $outDir, $prefix, $xMax, $yMax, \%comments);
    }
    return 1;
}

sub run {
    # mode = cr, het or xydiff
    my ($mode, $outDir, $dbPath, $iniPath) = @_;
    my $test = 1; # keep tests on by default, since they are very quick to run
    my %plotScripts = ( # R plotting scripts for each mode
                        cr        => 'plotCrPlate.R',
                        het       => 'plotHetPlate.R',
                        xydiff    => 'plotXYdiffPlate.R',
                        magnitude => 'plotMagnitudePlate.R',
        );
    my %index = ( # index in whitespace-separated input data for each mode
                  cr        => 1,
                  het       => 2,
                  xydiff    => 1,
                  magnitude => 1,
        );
    my %minMaxArgs = ( # supply min/max arguments to R script?
                       cr        => 0,
                       het       => 0,
                       xydiff    => 1,
                       magnitude => 0,);
    my $inputFH = \*STDIN;
    # read data from STDIN; output data values by plate & useful stats
    my ($dataOK, $dataRef, $xMax, $yMax, $plotMin, $plotMax) = 
        readData($inputFH, $index{$mode}, $mode, $dbPath, $iniPath);
    my $ok = 1;
    if ($dataOK) {
        writePlateData($dataRef, $mode.'_', $xMax, $yMax, $outDir,
                       $plotMin, $plotMax);
        $ok = makePlots($outDir, $plotScripts{$mode}, $mode."_*",
                        "plot_${mode}_", $minMaxArgs{$mode}, $test);
    } else {
        carp("Cannot parse plate/well locations; omitting ",
             "plate heatmap plots.");
    }
    return $ok;
}

my $ok = run($mode, $outDir, $dbPath, $iniPath);
if ($ok) { exit(0); }
else { exit(1); }


__END__

=head1 NAME

plate_heatmap_plots

=head1 DESCRIPTION

Generate heatmap plots of QC metrics by plate

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2013, 2015, 2016 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
