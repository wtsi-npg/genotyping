
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

use Test::More tests => 49;
use WTSI::NPG::Genotyping::QC::QCPlotShared qw/readFileToString readSampleInclusion/;
use WTSI::NPG::Genotyping::QC::QCPlotTests qw(jsonPathOK pngPathOK xmlPathOK);

my $testName = 'small_test';
my $start = time();
my $bin = "$Bin/../bin/"; # assume we are running from perl/t
my $data_dir = $Bin."/qc_test_data/";
my $plink = $data_dir.$testName;
my $sim = $data_dir.$testName.".sim";
my $outDir = "$Bin/qc/$testName/";
my $heatMapDir = "plate_heatmaps/";
my $iniPath = "$bin/../etc/qc_test.ini"; # contains inipath relative to test output directory (works after chdir)
my $dbname = "small_test.db";
my $dbfileMasterA = $data_dir.$dbname;
my $inclusionMaster = $data_dir."sample_inclusion.json";
my $mafhet = $data_dir."output_examples/small_test_maf_het.json";
my $config = "$bin/../etc/qc_config.json";
my $filterConfig = $data_dir."zcall_prefilter_test.json";
my $vcf = $data_dir."small_test_plex.vcf";
my $plexManifest = $data_dir."small_test_fake_manifest.tsv";
my $sampleJson = $data_dir."small_test_sample.json";
my $piperun = "pipeline_run"; # run name in pipeline DB
my ($cmd, $status);

# The directories containing the R scripts and Perl scripts
$ENV{PATH} = join(':', abs_path('../r/bin'), abs_path('../bin'), $ENV{PATH});

# copy pipeline DB to temporary directory; edits are made to temporary copy, not "master" copy from github
my $tempdir = tempdir(CLEANUP => 1);
system("cp $dbfileMasterA $tempdir");
my $dbfile = $tempdir."/".$dbname;

if (-e $outDir) {
    if (-d $outDir) {
	system("rm -Rf $outDir/*"); # remove previous output
    } else {
	croak "Output path $outDir exists and is not a directory!";
    }
} else {
    mkdir($outDir);
}

### test creation of QC input files ### 

print "Testing dataset $testName.\n";

## test identity check
$status = system("$bin/check_identity_bed.pl --outdir $outDir --config $config  --plink $plink --db $dbfile");
is($status, 0, "check_identity_bed.pl exit status");

## WIP identity check script is tested in WTSI::NPG::Genotyping::QC_wip::Check::IdentityTest

## test call rate & heterozygosity computation
my $crHetFinder = "snp_af_sample_cr_bed";
$status = system("$crHetFinder -r $outDir/snp_cr_af.txt -s $outDir/sample_cr_het.txt $plink");
is($status, 0, "snp_af_sample_cr_bed exit status");

## test duplicate check
$status = system("$bin/check_duplicates_bed.pl --dir $outDir $plink");
is($status, 0, "check_duplicates_bed.pl exit status");

## test gender check
$status = system("$bin/check_xhet_gender.pl --input=$plink --output-dir=$outDir");
is($status, 0, "check_xhet_gender.pl exit status");

## test collation into summary
## first, generate intensity metrics using simtools
$cmd = "simtools qc --infile $sim --magnitude $outDir/magnitude.txt ".
    "--xydiff $outDir/xydiff.txt";
system($cmd);

$cmd = "collate_qc_results.pl --input $outDir --status $outDir/qc_results.json --dbpath $dbfile --config $config"; 
is(system($cmd), 0, "collate_qc_results.pl exit status");
ok(jsonPathOK($outDir.'/qc_results.json'), "qc_results.json in valid format");

### test creation of plots ###

## PDF scatterplots for each metric
$status = system("$bin/plot_metric_scatter.pl --dbpath=$dbfile --inipath=$iniPath --config=$config --outdir=$outDir --qcdir=$outDir");
is($status, 0, "plot_metric_scatter.pl exit status");

# identity plot expected to be missing!
my @metrics = qw(call_rate duplicate heterozygosity gender magnitude xydiff);
foreach my $metric (@metrics) {
    my $plot = $outDir.'/scatter_'.$metric.'_000.pdf';
    ok((-e $plot), "PDF scatterplot exists: $metric");
}

## plate heatmap plots
my @modes = qw/cr het magnitude/;
foreach my $mode (@modes) {
    $cmd = "cat $outDir/sample_cr_het.txt | $bin/plate_heatmap_plots.pl --mode=$mode --out_dir=$outDir/$heatMapDir --dbpath=$dbfile --inipath=$iniPath";
    is(system($cmd), 0, "plate_heatmap_plots.pl exit status: mode $mode");
    for (my $i=0;$i<2;$i++) { 
	my $png = 'plot_'.$mode.'_ssbc0000'.$i.'.png';
        my $pngPath = $outDir."/plate_heatmaps/$png";
        ok(pngPathOK($pngPath), "PNG output $png in valid format");
    }
}

## plate heatmap index
$cmd = "$bin/plate_heatmap_index.pl $testName $outDir/$heatMapDir index.html";
is(system($cmd), 0, "plate_heatmap_index.pl exit status");
## plate heatmap index output
ok(xmlPathOK($outDir.'/plate_heatmaps/index.html'), "plate_heatmaps/index.html in valid XML format");

## cr/het density
$cmd = "cat $outDir/sample_cr_het.txt | $bin/plot_cr_het_density.pl --out_dir=$outDir --title=$testName";
is(system($cmd), 0, "plot_cr_het_density.pl exit status");

## failure cause breakdown
$cmd = "$bin/plot_fail_causes.pl --title=$testName --inipath=$iniPath  --config=$config --input $outDir/qc_results.json --cr-het $outDir/sample_cr_het.txt --output-dir $outDir";
is(system($cmd), 0, "plot_fail_causes.pl exit status");

## test PNG outputs in main directory
# sample_xhet_gender.png absent for this test; no mixture model
my @png = qw /crHetDensityScatter.png  failScatterPlot.png  
  crHistogram.png  failsCombined.png  
  crHetDensityHeatmap.png  failScatterDetail.png
  failsIndividual.png  hetHistogram.png/;
foreach my $png (@png) {
    ok(pngPathOK($outDir.'/'.$png), "PNG output $png in valid format");
}


system("rm -Rf $outDir/*"); # remove output from previous tests
system("cp $dbfileMasterA $tempdir");
print "\tRemoved output from previous tests; now testing main bootstrap script.\n";

## check run_qc.pl bootstrap script
# omit --title argument, to test default title function
my @args = ("--output-dir=$outDir",
            "--dbpath=$dbfile",
            "--sim=$sim",
            "--run=$piperun",
            "--inipath=$iniPath",
            "--config=$config",
            "--vcf=$vcf,$vcf",
            "--plex=$plexManifest",
            "--sample-json=$sampleJson",
            "--mafhet",
            "--plink=$plink");
is(system("$bin/run_qc.pl ".join(" ", @args)), 0,
   "run_qc.pl bootstrap script exit status");

## check work-in-progress output files
my $outDirWip = $outDir."/qc_wip";
my @output_wip = ("identity_wip.json", );
foreach my $file (@output_wip) {
  ok(-e $outDirWip."/".$file, "QC WIP output $file exists")
}

## check (non-heatmap) outputs again
foreach my $png (@png) {
    ok(pngPathOK($outDir."/supplementary/".$png), "PNG output $png in valid format");
}

my $heatMapsOK = 1;
@modes = qw/cr het magnitude/;
foreach my $mode (@modes) {
    for (my $i=0;$i<2;$i++) {
        my $png = $outDir."/plate_heatmaps/plot_".$mode."_ssbc0000$i.png";
        unless (pngPathOK($png)) {$heatMapsOK = 0; last; }
    }
    unless (xmlPathOK($outDir.'/plate_heatmaps/index.html')) { $heatMapsOK = 0; }
}
ok($heatMapsOK, "Plate heatmap outputs OK");

# check summary outputs
ok(-r $outDir.'/pipeline_summary.csv', "CSV summary found");
ok(-r $outDir.'/pipeline_summary.pdf', "PDF summary found");

## run_qc.pl again, without the arguments for alternate identity check
system("rm -Rf $outDir/*"); # remove output from previous tests
system("cp $dbfileMasterA $tempdir");
$cmd = "$bin/run_qc.pl --output-dir=$outDir --dbpath=$dbfile --sim=$sim --plink=$plink --run=$piperun --inipath=$iniPath --mafhet --config=$config";

is(system($cmd), 0,
   "run_qc.pl bootstrap script exit status, no alternate identity check");

## test standalone report script
print "\tTesting standalone report generation script.\n";
system("rm -f $outDir/pipeline_summary.pdf");
$cmd = "$bin/write_qc_reports.pl --database $dbfile --prefix $outDir/pipeline_summary --input $outDir/supplementary";
system($cmd);
ok(-r $outDir.'/pipeline_summary.pdf', "PDF summary found from standalone script");
system("rm -f $outDir/pipeline_summary.log $outDir/pipeline_summary.tex");
print "\tTest dataset $testName finished.\n";

my $duration = time() - $start;
print "QC test $testName finished.  Duration: $duration s\n";

