#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# define shared constants for QC plot scripts (and subroutines, if ever needed)

package WTSI::Genotyping::QC::QCPlotShared;

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
