# Tests operation of genotyping QC on larger (beta) test dataset

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# November 2012

use strict;
use warnings;
use Cwd qw/abs_path/;
use FindBin qw/$Bin/;
use File::Temp qw/tempdir/;
use Test::More tests => 20;

my $plinkB = "$Bin/qc_test_data/beta";
my $simB = "$Bin/qc_test_data/beta.sim";
my $outDirB = "$Bin/qc/beta/";
my $heatMapDir = "plate_heatmaps/";
my $titleB = "Beta";
my $iniPath = abs_path("$Bin/../etc/qc_test.ini"); 
my $dbnameB = "beta_pipeline.db";
my $dbfileMasterB = "$Bin/qc_test_data/$dbnameB";
my $config = "$Bin/../etc/qc_config.json";

my $start = time();
print "\tTesting dataset Beta.\n";
my $tempdir = tempdir(CLEANUP => 1);
system("cp $dbfileMasterB $tempdir");
my $dbfileB = $tempdir."/".$dbnameB;

chdir($outDirB);
system('rm -f *.png *.txt *.json *.html *.log *.csv *.pdf plate_heatmaps/* '.
       'supplementary/*'); # remove any previous output

my $cmd = "run_qc.pl --output-dir=. --dbpath=$dbfileB --sim=$simB $plinkB --run=test --inipath=$iniPath"; 
is(system($cmd), 0, "run_qc.pl bootstrap script exit status");

my @pdf = qw/crHetDensityHeatmap.pdf  failsCombined.pdf          
  scatter_duplicate_000.pdf  scatter_heterozygosity_000.pdf  
 scatter_xydiff_000.pdf crHetDensityScatter.pdf  failsIndividual.pdf        
 scatter_duplicate_001.pdf  scatter_heterozygosity_001.pdf  
 scatter_xydiff_001.pdf failScatterDetail.pdf    scatter_call_rate_000.pdf  
 scatter_gender_000.pdf     scatter_magnitude_000.pdf
 failScatterPlot.pdf      scatter_call_rate_001.pdf  
 scatter_gender_001.pdf     scatter_magnitude_001.pdf/;

my $pdf;
foreach $pdf (@pdf) {
    ok(-e "supplementary/".$pdf, "PDF output $pdf exists");
}
$pdf = "pipeline_summary.pdf";
ok(-e $pdf, "PDF output $pdf exists");

my $duration = time() - $start;
print "QC test Beta finished.  Duration: $duration s\n";
