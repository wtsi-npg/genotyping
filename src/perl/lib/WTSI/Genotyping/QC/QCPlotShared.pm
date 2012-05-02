#!/software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# March 2012

# define shared constants for QC plot scripts (and subroutines, if ever needed)

package WTSI::Genotyping::QC::QCPlotShared;

$RScriptPath = "/software/R-2.11.1/bin/Rscript";
# location of qcPlots scripts; trailing / may be assumed by code which uses this variable!
$parentDir = '/nfs/users/nfs_i/ib5/mygit/genotype_qc/';
$scriptDir = $parentDir.'qcPlots/'; 

# file and directory names
$sampleCrHet = 'sample_cr_het.txt'; # main source of input
$xyDiffExpr = "/*XYdiff.txt"; # use to glob for xydiff input
$mainIndex = 'index.html';
$plateHeatmapDir = 'plate_heatmaps';
$plateHeatmapIndex = 'index.html'; # written to $plateHeatmapDir, not main output directory

