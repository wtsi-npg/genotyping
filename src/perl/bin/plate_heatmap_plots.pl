#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# script to generate heatmap plots of sample CR and het rate, and intensity xydiff, for wells in each input plate
# assume sample names are in the form PLATE_POSITION_SAMPLE-ID
# POSITION is in the form H10 for x=8, y=10
# writes small .txt files containing input values for each plot (in case needed for later reference)

use strict;
use warnings;
use FindBin qw($Bin);
use Getopt::Long;
use WTSI::Genotyping::QC::QCPlotTests;

my ($mode, $RScriptPath, $outDir, $help);

GetOptions("mode=s"    => \$mode,
	   "out_dir=s" => \$outDir,
	   "h|help"    => \$help);

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Script to generate heatmap plots for each sample on a plate surface.
Plots include call rate, autosome heterozygosity, and xy intensity difference.
Appropriate input data must be supplied to STDIN: either sample_cr_het.txt or the *XYdiff.txt file.

Options:
--mode=KEY          Keyword to determine plot type. Must be one of: cr, het, xydiff
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
    my ($inputDir, $plotScript, $expr, $prefix, $minMaxArgs, $test) = @_;
    my @paths = glob($inputDir.'/'.$expr);
    my $allPlotsOK = 1;
    foreach my $path (@paths) {
	my %comments = readComments($path);
	my $plate = $comments{'PLATE_NAME'};
	# TODO fail silently for reserved filenames, eg. xydiff_boxplot.txt
	if (not(defined($plate))) {
	    print STDERR "WARNING: Cannot read plate name from $path. Skipping.\n";
	    next;
	}
	$plate =~ s/\s+/_/; # get rid of spaces in plate name (if any)
	my $outPath = $inputDir.'/'.$prefix.$plate.'.png';
	my @args = ($plotScript, $path, $plate);
	if ($minMaxArgs) { push(@args, ($comments{'PLOT_MIN'}, $comments{'PLOT_MAX'})); }
	my @outputs = ($outPath, );
	my $plotsOK = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs, $test);
	if ($plotsOK==0) { $allPlotsOK = 0; }
    } 
    return $allPlotsOK;
}

sub parseSampleName {
    # parse sample name in PLATE_WELL_ID format
    # WELL is in the form H10 for x=8, y=10
    # silently return undefined values if name not in correct format
    my $name = shift;
    my ($plate, $well, $id, $x, $y);
    if ($name =~ m/^[^_]+_[A-Z][0-9]+_\w+/) { # check name format, eg some-plate_H10_sample-id
	($plate, $well, $id) = split /_/, $name;
	my @chars = split //, $well;
	$x = ord(uc(shift(@chars))) - 64; # convert letter to position in alphabet 
	$y = join('', @chars);
	$y =~ s/^0+//; # remove leading zeroes from $y
    }
    return ($plate, $x, $y);
}

sub readData {
    # read from a filehandle
    # get data values by plate
    my ($inputRef, $index, $mode) = @_;
    my (%results, @allResults, $plotMin, $plotMax, %names, %plateNames, $name);
    my ($xMax, $yMax, $duplicates) = (0,0,0);
    my $dataOK = 1;
    while (<$inputRef>) {
	if (/^#/) { next; } # ignore comments
	chomp;
	my @words = split;
	my ($plate, $x, $y) = parseSampleName($words[0]);
	unless ($plate) { $dataOK = 0; last; }
	# clean up plate name by removing illegal characters; plate name used as filename component
	if (not $plateNames{$plate}) {
	    # first time plate is encountered
	    $name = $plate;
	    $name =~ s/[-\W]/_/g;
	    if ($names{$name}) {  # plate name not unique after cleanup
		$duplicates++;
		$name .= '_'.$duplicates; # *not* guaranteed unique, but should be ok except in pathological cases
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
    return ($dataOK, \%results, $xMax, $yMax, $plotMin, $plotMax);
}

sub readComments {
    # read comments from table file header into a hash
    # header lines of the form '# KEY VALUE' ; VALUE may contain spaces!
    my $inPath = shift;
    my %comments = ();
    open IN, "< $inPath" || die "Cannot open input path $inPath: $!";
    while (<IN>) {
	chomp;
	unless (/^#/) { next; }
	my @words = split(/ /);
	$comments{$words[1]} = join(' ', @words[2..$#words]); # value may contain spaces!
    }
    close IN;
    return %comments;
}

sub writeGrid {
    # write table of results to file; could be CR or het rate
    my ($resultsRef, $outDir, $outPrefix, $xMax, $yMax, $commentRef) = @_;
    my %results = %$resultsRef;
    my %comments = %$commentRef; # supply a list of key/value comments; eg. PLATE_NAME my-name
    my $plate = $comments{'PLATE_NAME'};
    $plate =~ s/\s+/_/; # get rid of spaces in plate name (if any)
    my $outPath = $outDir."/".$outPrefix.$plate.".txt";
    open OUT, "> $outPath" || die "Cannot open output path $outPath: $!";
    my @keyList = keys(%comments);
    @keyList = sort(@keyList);
    foreach my $key (@keyList) { print OUT "# $key $comments{$key}\n"; }
    for (my $y=1; $y<=$yMax; $y++) { # x, y counts start at 1
	my @row = ();
	for (my $x=1; $x<=$xMax; $x++) {
	    my $result = $results{$x}{$y};
	    unless (defined($result)) { $result = 0; }
	    push (@row, $result);
	}
	print OUT join("\t", @row)."\n";
    }
    close OUT;
}

sub writePlateData {
    # for each plate, generate (x,y) grids of data (cr, het rate, xydiff) by sample; write to small files
    # grids form input to r script that does plotting
    # also supply plate name and min/max range for plot across all plates as comments
    my ($dataRef, $prefix, $xMax, $yMax, $outDir, $min, $max) = @_; # will append plate name to prefix
    my %data = %$dataRef;
    if (not -e $outDir) { mkdir($outDir) || die "Failed to create output directory $outDir : $!"; }
    elsif (not -d $outDir) { die "$outDir is not a directory: $!"; }
    elsif (not -w $outDir) { die "Directory $outDir is not writable: $!"; }
    foreach my $plate (keys(%data)) { 
	my %comments = (
	    PLATE_NAME => $plate,
	    PLOT_MIN => $min,
	    PLOT_MAX => $max,
	);
	writeGrid($data{$plate}, $outDir, $prefix, $xMax, $yMax, \%comments); 
    }
}

sub run {
    # mode = cr, het or xydiff
    my ($mode, $outDir) = @_;
    my $test = 1; # keep tests on by default, since they are very quick to run
    my %plotScripts = ( # R plotting scripts for each mode
	cr     => 'plotCrPlate.R',
	het    => 'plotHetPlate.R', 
	xydiff => 'plotXYdiffPlate.R', );
    my %index = ( # index in whitespace-separated input data for each mode
	cr     => 1,
	het    => 2, 
	xydiff => 1, );
    my %minMaxArgs = ( # supply min/max arguments to R script?
	cr     => 0,
	het    => 0, 
	xydiff => 1, );
    my $inputFH = \*STDIN;  
    # read data from STDIN; output data values by plate & useful stats
    my ($dataOK, $dataRef, $xMax, $yMax, $plotMin, $plotMax) = readData($inputFH, $index{$mode}, $mode);
    my $ok = 1;
    if ($dataOK) {
	writePlateData($dataRef, $mode.'_', $xMax, $yMax, $outDir, $plotMin, $plotMax); 
	$ok = makePlots($outDir, $plotScripts{$mode}, 
			$mode."_*", "plot_${mode}_", $minMaxArgs{$mode}, $test); 
    } else {
	print STDERR "Cannot parse plate/well locations; omitting plate heatmap plots.\n";
    }
    return $ok;
}

my $ok = run($mode, $outDir);
if ($ok) { exit(0); }
else { exit(1); }
