
# Tests operation of genotyping QC, both individual scripts and bootstrap

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# April 2013

use strict;
use warnings;
use Carp;
use Cwd qw/abs_path/;
use Digest::MD5;
use File::Temp qw/tempdir/;
use FindBin qw($Bin);
use JSON;

use Test::More tests => 51;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/mergeJsonResults 
  readFileToString readSampleInclusion/;
use WTSI::NPG::Genotyping::QC::QCPlotTests qw(jsonPathOK pngPathOK xmlPathOK);

my $testName = 'small_test';
my $start = time();
my $bin = "$Bin/../bin/"; # assume we are running from perl/t
my $plink = "$Bin/qc_test_data/$testName";
my $sim = "$Bin/qc_test_data/$testName.sim";
my $outDir = "$Bin/qc/$testName/";
my $heatMapDir = "plate_heatmaps/";
my $iniPath = "$bin/../etc/qc_test.ini"; # contains inipath relative to test output directory (works after chdir)
my $dbname = "small_test.db";
my $dbfileMasterA = "$Bin/qc_test_data/$dbname";
my $inclusionMaster = "$Bin/qc_test_data/sample_inclusion.json";
my $mafhet = "$Bin/qc_test_data/small_test_maf_het.json";
my $config = "$bin/../etc/qc_config.json";
my $filterConfig = "$Bin/qc_test_data/zcall_prefilter_test.json";
my $piperun = "pipeline_run"; # run name in pipeline DB
my ($cmd, $status);

# The directories contains the R scripts and Perl scripts
$ENV{PATH} = join(':', abs_path('../r/bin'), abs_path('../bin'), $ENV{PATH});

# FIXME - hacked this in because scripts are calling scripts here
# The code being reused this way should be factored out into modules.
$ENV{PERL5LIB} = join(':', "$Bin/../blib/lib", $ENV{PERL5LIB});

# copy pipeline DB to temporary directory; edits are made to temporary copy, not "master" copy from github
my $tempdir = tempdir(CLEANUP => 1);
system("cp $dbfileMasterA $tempdir");
my $dbfile = $tempdir."/".$dbname;

# may later include datasets 'beta', 'gamma', etc.
unless (-e $outDir && -d $outDir) { mkdir($outDir); }
chdir($outDir) || croak "Cannot cd to output directory \"$outDir\"";
system('rm -f *.png *.txt *.json *.html *.log *.csv *.pdf plate_heatmaps/* '.
       'supplementary/*'); # remove any previous output

### test creation of QC input files ### 

print "Testing dataset $testName.\n";

## test identity check
$status = system("$bin/check_identity_bed.pl --config $config $plink");
is($status, 0, "check_identity_bed.pl exit status");

## test call rate & heterozygosity computation
my $crHetFinder = "snp_af_sample_cr_bed";
$status = system("$crHetFinder $plink");
is($status, 0, "snp_af_sample_cr_bed exit status");

## test duplicate check
$status = system("$bin/check_duplicates_bed.pl $plink");
is($status, 0, "check_duplicates_bed.pl exit status");

## test gender check
$status = system("$bin/check_xhet_gender.pl --input=$plink");
is($status, 0, "check_xhet_gender.pl exit status");

## test collation into summary
## first, generate intensity metrics using simtools
$cmd = "simtools qc --infile $sim --magnitude magnitude.txt ".
    "--xydiff xydiff.txt";
system($cmd);
$status = system("$bin/write_qc_status.pl --dbpath=$dbfile --inipath=$iniPath");
is($status, 0, "write_qc_status.pl exit status");
## test output
ok(jsonPathOK('qc_results.json'), "qc_results.json in valid format");

## test merge of .json results
my @mergeInputs = ('qc_results.json', $mafhet);
my $merged = 'qc_merged.json';
mergeJsonResults(\@mergeInputs, $merged);
ok(jsonPathOK($merged), "Merged QC results in valid format");

### test creation of plots ###

## PDF scatterplots for each metric
$status = system("$bin/plot_metric_scatter.pl --dbpath=$dbfile");
is($status, 0, "plot_metric_scatter.pl exit status");

# identity plot expected to be missing!
my @metrics = qw(call_rate duplicate heterozygosity gender magnitude xydiff);
foreach my $metric (@metrics) {
    ok((-e 'scatter_'.$metric.'_000.pdf'), "PDF scatterplot exists: $metric");
}

## plate heatmap plots
my @modes = qw/cr het magnitude/;
foreach my $mode (@modes) {
    $cmd = "cat sample_cr_het.txt | $bin/plate_heatmap_plots.pl --mode=$mode --out_dir=$outDir/$heatMapDir --dbpath=$dbfile --inipath=$iniPath";
    is(system($cmd), 0, "plate_heatmap_plots.pl exit status: mode $mode");
    for (my $i=0;$i<2;$i++) { 
        my $png = "plate_heatmaps/plot_".$mode."_ssbc0000$i.png";
        ok(pngPathOK($png), "PNG output $png in valid format");
    }
}

## plate heatmap index
$cmd = "$bin/plate_heatmap_index.pl $testName $heatMapDir index.html";
is(system($cmd), 0, "plate_heatmap_index.pl exit status");
## plate heatmap index output
ok(xmlPathOK('plate_heatmaps/index.html'), "plate_heatmaps/index.html in valid XML format");

## cr/het density
$cmd = "cat sample_cr_het.txt | $bin/plot_cr_het_density.pl --out_dir=. --title=$testName";
is(system($cmd), 0, "plot_cr_het_density.pl exit status");

## failure cause breakdown
$cmd = "$bin/plot_fail_causes.pl --title=$testName --inipath=$iniPath";
is(system($cmd), 0, "plot_fail_causes.pl exit status");

## test PNG outputs in main directory
# sample_xhet_gender.png absent for this test; no mixture model
my @png = qw /crHetDensityScatter.png  failScatterPlot.png  
  crHistogram.png  failsCombined.png  
  crHetDensityHeatmap.png  failScatterDetail.png
  failsIndividual.png  hetHistogram.png/;
foreach my $png (@png) {
    ok(pngPathOK($png), "PNG output $png in valid format");
}

## test exclusion of invalid results
$cmd = "$bin/filter_samples.pl --thresholds $filterConfig --in ".
    "qc_merged.json --db $dbfile";
is(system($cmd), 0, "Exit status of pre-filter script");
my $incMasterRef = decode_json(readFileToString($inclusionMaster));
my $incResultRef = readSampleInclusion($dbfile);
is_deeply($incResultRef, $incMasterRef, 
	  "Check sample inclusion status against master file");

system('rm -f *.png *.txt *.json *.html plate_heatmaps/*'); # remove output from previous tests, again
system("cp $dbfileMasterA $tempdir");
print "\tRemoved output from previous tests; now testing main bootstrap script.\n";

## check run_qc.pl bootstrap script
# omit --title argument, to test default title function
$cmd = "$bin/run_qc.pl --output-dir=. --dbpath=$dbfile --sim=$sim $plink --run=$piperun --inipath=$iniPath"; 
is(system($cmd), 0, "run_qc.pl bootstrap script exit status");

## check (non-heatmap) outputs again
foreach my $png (@png) {
    ok(pngPathOK("supplementary/".$png), "PNG output $png in valid format");
}

my $heatMapsOK = 1;
@modes = qw/cr het magnitude/;
foreach my $mode (@modes) {
    for (my $i=0;$i<2;$i++) {
        my $png = "plate_heatmaps/plot_".$mode."_ssbc0000$i.png";   
        unless (pngPathOK($png)) {$heatMapsOK = 0; last; }
    }
    unless (xmlPathOK('plate_heatmaps/index.html')) { $heatMapsOK = 0; }
}
ok($heatMapsOK, "Plate heatmap outputs OK");

# check summary outputs
ok(-r 'pipeline_summary.csv', "CSV summary found");
ok(-r 'pipeline_summary.pdf', "PDF summary found");

## test standalone report script
print "\tTesting standalone report generation script.\n";
system('rm -f pipeline_summary.*');
$cmd = "$bin/write_qc_reports.pl --database $dbfile --input ".
    "./supplementary";
system($cmd);
ok(-r 'pipeline_summary.csv', "CSV summary found from standalone script");
ok(-r 'pipeline_summary.pdf', "PDF summary found from standalone script");
system("rm -f pipeline_summary.log pipeline_summary.tex");
print "\tTest dataset $testName finished.\n";

my $duration = time() - $start;
print "QC test $testName finished.  Duration: $duration s\n";

