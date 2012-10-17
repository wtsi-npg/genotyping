
# Tests operation of genotyping QC, both individual scripts and bootstrap

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Cwd;
use File::Temp qw/tempdir/;
use FindBin qw($Bin);
use Test::More tests => 95;
use WTSI::Genotyping::QC::QCPlotTests qw(jsonPathOK pngPathOK xmlPathOK);

my $start = time();
my $bin = "$Bin/../bin/"; # assume we are running from perl/t
my $plinkA = "$Bin/qc_test_data/alpha";
my $simA = "$Bin/qc_test_data/alpha.sim";
my $outDirA = "$Bin/qc/alpha/";
my $heatMapDir = "plate_heatmaps/";
my $titleA = "Alpha";
my $iniPath = "$bin/../etc/qc_test.ini"; # contains inipath relative to test output directory (works after chdir)
my $dbnameA = "alpha_pipeline.db";
my $dbfileMasterA = "$Bin/qc_test_data/$dbnameA";
my $config = "$bin/../etc/qc_config.json";
my $piperun = "pipeline_run"; # run name in pipeline DB
my ($cmd, $status);

# copy pipeline DB to temporary directory; edits are made to temporary copy, not "master" copy from github
my $tempdir = tempdir(CLEANUP => 1);
system("cp $dbfileMasterA $tempdir");
my $dbfileA = $tempdir."/".$dbnameA;

# may later include datasets 'beta', 'gamma', etc.

chdir($outDirA);
system('rm -f *.png *.txt *.json *.html *.log *.csv *.pdf plate_heatmaps/*'); # remove any previous output

### test creation of QC input files ### 

print "Testing dataset Alpha.\n";

## test identity check
$status = system("perl $bin/check_identity_bed.pl --config $config $plinkA");
is($status, 0, "check_identity_bed.pl exit status");

## test call rate & heterozygosity computation
my $crHetFinder = "/nfs/users/nfs_i/ib5/mygit/github/Gftools/snp_af_sample_cr_bed"; # TODO make more portable
$status = system("$crHetFinder $plinkA");
is($status, 0, "snp_af_sample_cr_bed exit status");

## test duplicate check
$status = system("perl $bin/check_duplicates_bed.pl $plinkA");
is($status, 0, "check_duplicates_bed.pl exit status");

## test gender check
$status = system("perl $bin/check_xhet_gender.pl --input=$plinkA");
is($status, 0, "check_xhet_gender.pl exit status");

## test xydiff computation
$status = system("perl $bin/intensity_metrics.pl --input=$simA --magnitude=magnitude.txt --xydiff=xydiff.txt");
is($status, 0, "intensity_metrics.pl exit status");

## test collation into summary
$status = system("perl $bin/write_qc_status.pl --dbpath=$dbfileA --inipath=$iniPath");
is($status, 0, "write_qc_status.pl exit status");
## test output
ok(jsonPathOK('qc_results.json'), "qc_results.json in valid format");

### test creation of plots ###

## PDF scatterplots for each metric
$status = system("plot_metric_scatter.pl --dbpath=$dbfileA");
is($status, 0, "plot_metric_scatter.pl exit status");

# identity plot expected to be missing!
my @metrics = qw(call_rate duplicate heterozygosity gender magnitude);
foreach my $metric (@metrics) {
    ok((-e 'scatter_'.$metric.'_000.pdf'), "PDF scatterplot exists: $metric");
}

## plate heatmap plots
my @modes = qw/cr het xydiff/;
foreach my $mode (@modes) {
    $cmd = "cat sample_cr_het.txt | perl $bin/plate_heatmap_plots.pl --mode=$mode --out_dir=$outDirA/$heatMapDir --dbpath=$dbfileA --inipath=$iniPath";
    is(system($cmd), 0, "plate_heatmap_plots.pl exit status: mode $mode");
    for (my $i=1;$i<=11;$i++) {
	my $png = "plate_heatmaps/plot_".$mode."_SS_plate".sprintf("%04d", $i).".png";
	ok(pngPathOK($png), "PNG output $png in valid format");
    }
}

## plate heatmap index
$cmd = "perl $bin/plate_heatmap_index.pl $titleA $heatMapDir index.html";
is(system($cmd), 0, "plate_heatmap_index.pl exit status");
## plate heatmap index output
ok(xmlPathOK('plate_heatmaps/index.html'), "plate_heatmaps/index.html in valid XML format");

## box/bean plots
my @inputs = qw/sample_cr_het.txt sample_cr_het.txt xydiff.txt/;
for (my $i=0;$i<@modes;$i++) {
    $cmd = "cat $inputs[$i] | perl $bin/plot_box_bean.pl --mode=$modes[$i] --out_dir=. --title=$titleA --dbpath=$dbfileA --inipath=$iniPath";
    is(system($cmd), 0, "plot_box_bean.pl exit status: mode $modes[$i]");
}

## cr/het density
$cmd = "cat sample_cr_het.txt | perl $bin/plot_cr_het_density.pl --out_dir=. --title=$titleA";
is(system($cmd), 0, "plot_cr_het_density.pl exit status");

## failure cause breakdown
$cmd = "perl $bin/plot_fail_causes.pl --title=$titleA --inipath=$iniPath";
is(system($cmd), 0, "plot_fail_causes.pl exit status");

## test PNG outputs in main directory
my @png = qw /cr_beanplot.png          crHetDensityScatter.png  failScatterPlot.png  het_beanplot.png  
sample_xhet_gender.png  xydiff_boxplot.png      cr_boxplot.png           crHistogram.png          
failsCombined.png    het_boxplot.png   total_samples_per_plate.png
crHetDensityHeatmap.png  failScatterDetail.png    failsIndividual.png  hetHistogram.png  xydiff_beanplot.png/;
foreach my $png (@png) {
    ok(pngPathOK($png), "PNG output $png in valid format");
}

## html index for all plots
$cmd = "perl $bin/main_plot_index.pl . qc_results.json $titleA";
is(system($cmd), 0, "main_plot_index.pl exit status");

## main index output
ok(xmlPathOK('index.html'), "Main index.html in valid XML format");

system('rm -f *.png *.txt *.json *.html plate_heatmaps/*'); # remove output from previous tests, again
system("cp $dbfileMasterA $tempdir");
print "\tRemoved output from previous tests; now testing main bootstrap script.\n";

## check run_qc.pl bootstrap script
# omit --title argument, to test default title function
$cmd = "perl $bin/run_qc.pl --output-dir=. --dbpath=$dbfileA --sim=$simA $plinkA --run=$piperun --inipath=$iniPath"; 
is(system($cmd), 0, "run_qc.pl bootstrap script exit status");

## check (non-heatmap) outputs again
foreach my $png (@png) {
    ok(pngPathOK("supplementary/".$png), "PNG output $png in valid format");
}
ok(xmlPathOK('supplementary/index.html'), 
   "Main index.html in valid XML format");

my $heatMapsOK = 1;
foreach my $mode (@modes) {
    for (my $i=1;$i<=11;$i++) {
	my $png = "plate_heatmaps/plot_".$mode."_SS_plate".sprintf("%04d", $i).".png";
	unless (pngPathOK($png)) {$heatMapsOK = 0; last; }
    }
    unless (xmlPathOK('plate_heatmaps/index.html')) { $heatMapsOK = 0; }
}
ok($heatMapsOK, "Plate heatmap outputs OK");

# check summary outputs
ok(-r 'pipeline_summary.csv', "CSV summary found");
ok(-r 'pipeline_summary.pdf', "PDF summary found");

print "\tTest dataset Alpha finished.\n";

my $duration = time() - $start;
print "QC test finished.  Duration: $duration s\n";
