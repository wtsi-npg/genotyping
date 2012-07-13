
# Tests creation of input files for genotyping QC
# Start with running individual scripts and testing exit status
# Later try and validate inputs

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# July 2012

use strict;
use warnings;
use Cwd;
use FindBin qw($Bin);
use Test::More tests => 16;

my $bin = "$Bin/../bin/"; # assume we are running from perl/t
my $plink = "$Bin/qc_test_data/alpha";
my $sim = "$Bin/qc_test_data/alpha.sim";
my $outDir = "$Bin/qc/";
my $heatMapDir = $outDir."/plate_heatmaps/";
my $title = "Alpha";

my ($cmd, $status);
chdir($outDir);

### test creation of QC input files ### 

## test identity check
$status = system("perl $bin/check_identity_bed.pl $plink");
is($status, 0, "check_identity_bed.pl exit status");

## test call rate & heterozygosity computation
my $crHetFinder = "/nfs/users/nfs_i/ib5/mygit/github/Gftools/snp_af_sample_cr_bed"; # TODO make more portable
$status = system("$crHetFinder $plink");
is($status, 0, "snp_af_sample_cr_bed exit status");

## test duplicate check
$status = system("perl $bin/check_duplicates_bed.pl $plink");
is($status, 0, "check_duplicates_bed.pl exit status");

## test gender check
$status = system("perl $bin/write_gender_files.pl --qc-output=sample_xhet_gender.txt --plots-dir=. $plink");
is($status, 0, "write_gender_files.pl exit status");

## test xydiff computation
$status = system("perl $bin/xydiff.pl --input=$sim --output=xydiff.txt");
is($status, 0, "xydiff.pl exit status");

## test collation into summary
$status = system("perl $bin/write_qc_status.pl --config=$bin/../json/qc_threshold_defaults.json");
is($status, 0, "write_qc_status.pl exit status");

### test creation of plots ###

## plate heatmap plots
my @modes = qw/cr het xydiff/;
foreach my $mode (@modes) {
    $cmd = "cat sample_cr_het.txt | perl $bin/plate_heatmap_plots.pl --mode=$mode --out_dir=$heatMapDir";
    is(system($cmd), 0, "plate_heatmap_plots.pl exit status: mode $mode");
}

## plate heatmap index
$cmd = "perl $bin/plate_heatmap_index.pl $title $heatMapDir index.html";
is(system($cmd), 0, "plate_heatmap_index.pl exit status");

## box/bean plots
my @inputs = qw/sample_cr_het.txt sample_cr_het.txt xydiff.txt/;
for (my $i=0;$i<@modes;$i++) {
    $cmd = "cat $inputs[$i] | perl $bin/plot_box_bean.pl --mode=$modes[$i] --out_dir=. --title=$title";
    is(system($cmd), 0, "plot_box_bean.pl exit status: mode $modes[$i]");
}

## cr/het density
$cmd = "cat sample_cr_het.txt | perl $bin/plot_cr_het_density.pl --out_dir=. --title=$title";
is(system($cmd), 0, "plot_cr_het_density.pl exit status");

## failure cause breakdown
$cmd = "perl $bin/plot_fail_causes.pl --title=$title";
is(system($cmd), 0, "plot_fail_causes.pl exit status");

## html index for all plots
$cmd = "perl $bin/main_plot_index.pl . qc_results.json $title";
is(system($cmd), 0, "main_plot_index.pl exit status");

# TODO add test of run_qc.pl bootstrap script
