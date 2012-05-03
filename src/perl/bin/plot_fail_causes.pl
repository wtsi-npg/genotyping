#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# Plot frequencies of causes for samples failing QC
#Possible failure causes:
#* Gender check
#* Identity check (vs. Sequenom)
#* Duplicate check (vs. panel of 400 SNPs generated on-the-fly)
#* Call rate (< 95%? )
#* Heterozygosity rate of autosomes (+/- 2 sd from mean)
#* (Any combination of the above!  This means 32 distinct combinations, ie. 2**[failures] )

# write .txt input for R plotting scripts, then execute scripts (using test wrapper)

use strict;
use warnings;
use Getopt::Long;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotShared;
use WTSI::Genotyping::QC::QCPlotTests;

my ($inputDir, $outputDir, $gender, $identity, $duplicate, $crHet, $help, $failText, $comboText, $causeText, 
    $crHetFail, $comboPng, $causePng, $scatterPng, $detailPng, $minCR, $maxHetSd, $noSequenom, $sequenom, $title);

GetOptions("cr_het=s"      => \$crHet,
	   "duplicate=s",  => \$duplicate,
	   "gender=s"      => \$gender,
	   "help"          => \$help,
	   "input_dir=s"   => \$inputDir,
	   "output_dir=s"  => \$outputDir,
	   "min_cr=f",     => \$minCR,
	   "max_het_sd=f", => \$maxHetSd,
	   "title=s",      => \$title,
	   "no_sequenom"   => \$noSequenom,  # exclude sequenom identity check results
    );

$inputDir   ||= '.';
$crHet      ||= 'sample_cr_het.txt';
$duplicate  ||= 'duplicate_summary.txt';
$gender     ||= 'gender_fails.txt';
$identity   ||= 'identity_fail.txt'; # Will be replaced by identity_check_fail.txt when using refactored script
$minCR      ||= 0.95; # minimum sample call rate
$maxHetSd   ||= 2; # maximum standard deviations from het mean
$failText   ||= 'failTotals.txt';
$comboText  ||= 'failCombos.txt';
$causeText  ||= 'failCauses.txt';
$crHetFail  ||= 'failedSampleCrHet.txt';
$comboPng   ||= 'failsCombined.png';
$causePng   ||= 'failsIndividual.png';
$scatterPng ||= 'failScatterPlot.png';
$detailPng  ||= 'failScatterDetail.png';
$outputDir  ||= '.';
$title      ||= 'Unknown_experiment';
if ($noSequenom) { $sequenom = 0; }
else { $sequenom = 1; } # use sequenom by default

if ($help) {
    print STDERR "Usage: $0 [ options ] 
Options:
--help              Print this help text and exit
Unspecified options will receive default values.
";
    exit(0);
}

my (@inputPaths, @outputPaths);
my @inputNames = ($crHet, $duplicate, $identity, $gender, );
if ($inputDir !~ /\/$/) { $inputDir .= '/'; }
foreach my $name (@inputNames) { push(@inputPaths, $inputDir.$name); }
my @outNames = ($failText, $comboText, $causeText, $comboPng, $causePng, $crHetFail, $scatterPng, $detailPng);
if ($outputDir !~ /\/$/) { $outputDir .= '/'; }
foreach my $name (@outNames) { push(@outputPaths, $outputDir.$name); }

my $scriptDir = $Bin."/".$WTSI::Genotyping::QC::QCPlotShared::RScriptsRelative; 

sub findFailedCrHet {
    # find cr/het metrics for failed samples
    my ($allFailsRef, $namesRef, $crRef, $hetRef) = @_;
    my %failed;
    foreach my $name (@$allFailsRef) { $failed{$name} = 1; }
    my @names = @$namesRef;
    my @cr = @$crRef;
    my @het = @$hetRef;
    my %failedCrHet;
    for (my $i=0;$i<@names;$i++) {
	if ($failed{$names[$i]}) { 
	    $failedCrHet{$names[$i]} = [ ($cr[$i], $het[$i]) ];
	}
    }
    return %failedCrHet;
}

sub findFailedSamples {
    # get samples failing under each possible criterion
    # also want master list of all failed samples
    my ($crHetPath, $identityPath, $genderPath, $duplicatePath, $minCR, $maxHetSd, $sequenom) = @_;
    my %dFails = getFailsColumn($duplicatePath, 1);
    my %gFails = getFailsColumn($genderPath);
    my %iFails;
    if ($sequenom) { %iFails = getFailsColumn($identityPath); }
    else { %iFails = (); }
    my ($namesRef, $crRef, $hetRef) = readSampleCrHet($crHetPath);
    my @crStats = meanSd(@$crRef);
    my %cFails = getCrFails($namesRef, $crRef, $minCR);
    my ($hFailsRef, $mean, $sd, $maxDist) = getHetFails($namesRef, $hetRef, $maxHetSd);
    my @hetStats = ($mean, $sd, $maxDist);
    my @allFails = mergeKeys(\%dFails, \%gFails, \%iFails, \%cFails, $hFailsRef);
    my %failCauses = (C => \%cFails, 
		      D => \%dFails,
		      G => \%gFails,
		      H => $hFailsRef, 
		      I => \%iFails,);
    my $totalSamples = @$namesRef;
    my $totalFails = @allFails;
    # find CR/Het metrics for failed samples
    my %failedCrHet = findFailedCrHet(\@allFails, $namesRef, $crRef, $hetRef);
    return (\@allFails, \%failCauses, $totalSamples, $totalFails, \%failedCrHet, \@crStats, \@hetStats);
}

sub getCrFails {
    # get names of samples below CR threshold
    my @names = @{shift()};
    my @cr = @{shift()};
    my $minCR = shift;
    my %fails;
    for (my $i=0;$i<@names;$i++) {
	if ($cr[$i] < $minCR) { $fails{$names[$i]}=1; }
    }
    return %fails;
}

sub getFailsColumn {
    # read sample names from identity_fails.txt *or* gender_fails.txt; return dictionary
    # silently return an empty dictionary if file is not readable
    my $inPath = shift;
    my $index = shift;
    $index ||= 0;
    my %fails = ();
    if (-r $inPath) {
	open IN, "< $inPath" || die "Cannot open input path $inPath: $!";
	while (<IN>) {
	    if (/^#/) { next; } # ignore comments
	    $fails{(split)[$index]}=1;
	}
	close IN;
    }
    return %fails;
}

sub getHetFails {
    # get names of samples too far from het mean
    my @names = @{shift()};
    my @het = @{shift()};
    my $maxSD = shift;
    my ($mean, $sd) = meanSd(@het);
    my $maxDist = $maxSD * $sd; # maximum distance from mean
    my %fails;
    for (my $i=0;$i<@names;$i++) {
	if (abs($het[$i] - $mean) > $maxDist) { $fails{$names[$i]}=1; }
    }
    return (\%fails, $mean, $sd, $maxDist);
}

sub meanSd {
    # find mean and standard deviation of input list
    # first pass -- mean
    my $total = 0;
    foreach my $x (@_) { $total+= $x; }
    my $mean = $total / @_;
    # second pass -- sd
    $total = 0;
    foreach my $x (@_) { $total += abs($x - $mean); }
    my $sd = $total / @_;
    return ($mean, $sd);
}

sub mergeKeys {
    # find set of all keys (without repetition) for given list of hash references
    my @hashRefs = @_; 
    my %allKeys;
    foreach my $hashRef (@hashRefs) {
	foreach my $key (keys(%$hashRef)) { $allKeys{$key} = 1; }
    }
    return sort(keys(%allKeys));
}

sub readSampleCrHet {
    # read lists of sample name, CR and het rate from sample_cr_het.txt
    my $inPath = shift;
    my (@names, @cr, @het);
    open IN, "< $inPath" || die "Cannot open input path $inPath: $!";
    while (<IN>) {
	if (/^#/) { next; } # ignore comments
	chomp;
	my ($name, $cr, $het) = split;
	push(@names, $name);
	push(@cr, $cr);
	push(@het, $het);
    }
    close IN;
    return (\@names, \@cr, \@het);
}

sub sortFailCodes {
    # want to sort failure codes: all one-letter codes, then all two-letter, then all three-letter, etc.
    # groups "numbers of causes" together in plot
    my %codesByLen; # hash of arrays for each length
    my $max = 0;
    foreach my $code (@_) { 
	my $len = length($code);
	$codesByLen{$code} = $len;
	if ($len>$max) { $max = $len; }
    }
    my @sorted;
    for (my $i=1;$i<=$max;$i++) {	
	my @codes = ();
	foreach my $code (@_) { # repeated loop is inefficient, but doesn't matter with <= 32 codes!
	    if ($codesByLen{$code}==$i) { push(@codes, $code); }
	}
	push(@sorted, (sort(@codes)));
    }
    return @sorted;
}

sub writeFailedCrHet {
    # write CR/het scores and failure cause breakdowns, for failed samples
    # TODO Option to exclude Sequenom identity check from breakdown
    my ($crHetRef, $causeRef, $hetStatsRef, $outPath) = @_;
    my %failedCrHet = %$crHetRef;
    my %causes = %$causeRef;
    my @names = (sort(keys(%failedCrHet))); # names of failed samples
    my @hetStats = @$hetStatsRef;
    my @hetStatsTitles = qw/HET_MEAN HET_SD HET_FAIL_DISTANCE/;
    my @headers = ('# failed_sample', 'cr', 'het', 'duplicate_fail', 'gender_fail', 'identity_fail');
    open OUT, "> $outPath" || die "Cannot open output path $outPath: $!";
    for (my $i=0;$i<@hetStats;$i++) {
	print OUT join(" ", ('#', $hetStatsTitles[$i], $hetStats[$i]))."\n"; # record in case needed later
    }
    print OUT join("\t", @headers)."\n";
    my @causeCodes = qw/D G I/;
    my @items;
    foreach my $name (@names) {
	my ($cr, $het) = @{$failedCrHet{$name}};
	@items = ($name, $cr, $het, );
	foreach my $code (@causeCodes) { # append 1 if failure cause applies, 0 otherwise
	    if ($causes{$code}{$name}) { push(@items, 1); }
	    else { push(@items, 0); }
	}
	print OUT join("\t", @items)."\n";
    }
    close OUT || die "Cannot close output path $outPath: $!";
}

sub writeFailCounts {
    # write counts of individual/combined failures
    my ($namesRef, $causesRef, $comboPath, $causePath) = @_;
    my @names = @$namesRef; # names of failed samples
    my %causes = %$causesRef; # hashes of failed sample names, by single-letter cause code
    my @causeNames = keys(%causes);
    @causeNames = sort(@causeNames);
    my (%comboCounts, %causeCounts);
    # count individual and combined failure causes
    foreach my $name (@names) {
	my @failCodes;
	foreach my $cause (@causeNames) {
	    if ($causes{$cause}{$name}) { 
		push (@failCodes, $cause); 
		$causeCounts{$cause}++;
	    }
	}
	my $failCombo = join('', @failCodes);
	$comboCounts{$failCombo}++;
    }	
    # write combined fails
    my @sortedCodes = sortFailCodes(keys(%comboCounts));
    writeHash(\%comboCounts, $comboPath, \@sortedCodes);
    # write individual fails
    my %longCauseCounts;
    my %longNames = (C => "Call_rate", 
		     D => "Duplicate",
		     G => "Gender",
		     H => "Heterozygosity", 
		     I => "Identity_with_Sequenom",);
    foreach my $key (keys(%causeCounts)) { $longCauseCounts{$longNames{$key}} = $causeCounts{$key} }
    writeHash(\%longCauseCounts, $causePath);
}

sub writeFailTotals {
    # write summary stats to text file; later use to write table in html output
    my ($totalSamples, $totalFails, $crStatsRef, $hetStatsRef, $title, $sequenom, $outPath) = @_;
    my ($crMean, $crSd) = @$crStatsRef;
    my ($hetMean, $hetSd, $hetMaxDist) = @$hetStatsRef;
    my %stuff = (
	TITLE => $title,
	TOTAL_SAMPLES => $totalSamples,
	TOTAL_FAILURES => $totalFails,
	CR_MEAN => $crMean,
	CR_STANDARD_DEVIATION => $crSd,
	HET_MEAN => $hetMean,
	HET_STANDARD_DEVIATION => $hetSd,
	HET_MAX_DIVERGENCE => $hetMaxDist,
	USE_SEQUENOM => $sequenom,
    );
    writeHash(\%stuff, $outPath);
}

sub writeHash {
    # write hash of key/value pairs to a tab-delimited file
    # optionally, can specify order for keys
    my ($hashRef, $outPath, $keysRef) = @_;
    my %hash = %$hashRef;
    my @keys;
    if ($keysRef) { @keys = @$keysRef; }
    else { @keys = sort(keys(%hash)); }
    open OUT, "> $outPath" || die "Cannot open output path $outPath: $!";
    foreach my $key (@keys) { print OUT "$key\t$hash{$key}\n"; }
    close OUT;
}

sub run {
    # find failure causes and write input for R scripts
    my ($inputsRef, $outputsRef, $minCR, $maxHetSd, $title, $sequenom, $scriptDir) = @_;
    my ($crHetPath, $duplicatePath, $identityPath, $genderPath) = @$inputsRef;
    my ($totalText, $comboText, $causeText, $comboPng, $causePng, $failCrHetPath, $scatterPng, 
	$detailPng) = @$outputsRef;
    my @failInputs = ($crHetPath, $identityPath, $genderPath, $duplicatePath, $minCR, $maxHetSd, $sequenom);
    my ($namesRef, $causesRef, $totalSamples, $totalFails, $crHetRef, $crStatsRef, $hetStatsRef) 
	= findFailedSamples(@failInputs);
    writeFailTotals($totalSamples, $totalFails, $crStatsRef, $hetStatsRef, $title, $sequenom, $totalText);
    # do barplots of individual and combined failure causes
    writeFailCounts($namesRef, $causesRef, $comboText, $causeText);
    my $allPlotsOK = 1;
    my @args = ($WTSI::Genotyping::QC::QCPlotShared::RScriptExec, $scriptDir.'/plotIndividualFails.R', 
		$causeText, $totalFails, $title);
    my @outputs = ($causePng,);
    my $ok = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    unless ($ok) { $allPlotsOK = 0; }
    @args = ($WTSI::Genotyping::QC::QCPlotShared::RScriptExec,, $scriptDir.'/plotCombinedFails.R', 
	     $comboText, $title);
    @outputs = ($comboPng,);
    $ok = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    unless ($ok) { $allPlotsOK = 0; }
    # now do scatterplot of failed samples in cr/het plane
    writeFailedCrHet($crHetRef, $causesRef, $hetStatsRef, $failCrHetPath);
    my ($mean, $sd, $maxDist) = @$hetStatsRef;
    @args = ($WTSI::Genotyping::QC::QCPlotShared::RScriptExec,, $scriptDir.'/scatterPlotFails.R',
	     $failCrHetPath, $mean, $maxDist, $minCR, $title);
    @outputs = ($scatterPng, $detailPng);
    $ok = WTSI::Genotyping::QC::QCPlotTests::wrapPlotCommand(\@args, \@outputs);
    unless ($ok) { $allPlotsOK = 0; }
    return $allPlotsOK;
}

my $allPlotsOK = run(\@inputPaths, \@outputPaths, $minCR, $maxHetSd, $title, $sequenom, $scriptDir);
if ($allPlotsOK) { exit(0); }
else { exit(1); }
