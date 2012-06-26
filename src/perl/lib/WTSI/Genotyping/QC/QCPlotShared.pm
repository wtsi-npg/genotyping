#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# define shared constants and subroutines for QC plot scripts

package WTSI::Genotyping::QC::QCPlotShared;

use JSON;

$RScriptExec = "/software/R-2.11.1/bin/Rscript";
$RScriptsRelative = "../../r/bin/";  # relative path from perl bin dir to R scripts

# file and directory names
$sampleCrHet = 'sample_cr_het.txt'; # main source of input
$xyDiffExpr = "/*XYdiff.txt"; # use to glob for xydiff input (old pipeline output only; now read from .sim file)
$xydiff = "xydiff.txt"; # xydiff output file in new qc
$mainIndex = 'index.html';
$plateHeatmapDir = 'plate_heatmaps';
$plateHeatmapIndex = 'index.html'; # written to $plateHeatmapDir, not main output directory
$duplicates = 'duplicate_summary.txt';
$idents = 'identity_check_results.txt'; 
$genders = 'sample_xhet_gender.txt';
$qcResults = 'qc_results.json';

# set of allowed QC metric names (long and short versions)
@qcMetricNames = qw(call_rate heterozygosity duplicate identity gender xydiff);
%qcMetricNames;
foreach my $name (@qcMetricNames) { $qcMetricNames{$name}=1; } # convenient for checking name legality
%qcMetricNamesShort = ($qcMetricNames[0] => 'C',
		       $qcMetricNames[1] => 'H',
		       $qcMetricNames[2] => 'D',
		       $qcMetricNames[3] => 'I',
		       $qcMetricNames[4] => 'G',
		       $qcMetricNames[5] => 'X',
    );
%qcMetricInputs = ($qcMetricNames[0] => $sampleCrHet,
		   $qcMetricNames[1] => $sampleCrHet,
		   $qcMetricNames[2] => [$sampleCrHet, $duplicates],
		   $qcMetricNames[3] => $idents,
		   $qcMetricNames[4] => $genders,
		   $qcMetricNames[5] => $xydiff,
    );

# standard qc thresholds are in .json file
# duplicate threshold is currently hard-coded in /software/varinf/bin/genotype_qc/pairwise_concordance_bed

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

sub readQCResultHash {
    # read QC results data structure from JSON file
    # assumes top-level structure is a hash
    my $inPath = shift;
    open IN, "< $inPath";
    my @lines = <IN>;
    close IN;
    my %results = %{decode_json(join('', @lines))};
    return %results;
}

sub readSampleData {
    # read data for given sample names from space-delimited file; return array of arrays of data read
    # optional start, stop points counting from zero
    my ($inPath, $startLine, $stopLine) = @_;
    $startLine ||= 0;
    $stopLine ||= 0;
    my @data;
    open IN, "< $inPath" || die "Cannot open input path $inPath: $!";
    my $line = 0;
    while (<IN>) {
	$line++;
	if (/^#/ || $line <= $startLine) { next; } # comments start with a #
	elsif ($stopLine && $line+1 == $stopLine) { last; }
	my @fields = split;
	push(@data, \@fields);
    }
    close IN;
    return @data;    
}

sub readThresholds {
    # read QC metric thresholds from config path
    my $configPath = shift;
    open IN, "< $configPath";
    my @lines = <IN>;
    close IN;
    my %config = %{decode_json(join('', @lines))};
    my %thresholds = %{$config{"Metrics_thresholds"}};
    foreach my $name (keys(%thresholds)) { # validate metric names
	unless ($WTSI::Genotyping::QC::QCPlotShared::qcMetricNames{$name}) {
	    die "Unknown QC metric name: $!";
	}
    }
    return %thresholds;
}

