#! /usr/bin/env perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# extract information from PLINK data; generate QC plots and reports
# generic script intended to work with any caller producing PLINK output

# 1. Assemble information in text format; write to given 'qc directory'
# 2. Generate plots

use strict;
use warnings;
use Getopt::Long;
use Cwd;
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotShared; # qcPlots module to define constants
use WTSI::Genotyping::QC::QCPlotTests;

my ($help, $outDir, $plinkPrefix, $verbose);

GetOptions("help"         => \$help,
	   "output_dir=s" => \$outDir,
	   "verbose"      => \$verbose
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
Options:
--output-dir        Directory for QC output
--help              Print this help text and exit
--verbose           Print additional output to stdout
";
    exit(0);
}

$outDir ||= "./qc";
if (not -e $outDir) { mkdir($outDir); }

$plinkPrefix = $ARGV[0];
unless ($plinkPrefix) { die "ERROR: Must supply a PLINK filename prefix!"; }

run($plinkPrefix, $outDir);

sub checkInputs {
    # check that PLINK binary files exist and are readable
    my $plinkPrefix = shift;
    my @suffixes = qw(.bed .bim .fam);
    my $inputsOK = 1;
    foreach my $suffix (@suffixes) {
	my $path = $plinkPrefix.$suffix;
	unless (-r $path) { $inputsOK = 0; last; } 
    }
    return $inputsOK;
}

sub createPlots {
    # create plots from QC files
    my ($plinkPrefix, $outDir, $tests, $failures, $title, $verbose) = @_;
    $tests ||= 0;
    $failures ||= 0;
    my $startDir = getcwd;
    chdir($outDir);
    my @cmds = getPlotCommands(".", $title);
    my @omits = ();
    if ($verbose) { print WTSI::Genotyping::QC::QCPlotTests::timeNow()." Starting plot generation.\n"; }
    ($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommandList(\@cmds, $tests, $failures, 
									     $verbose, \@omits);
    chdir($startDir);
    return ($tests, $failures);
}

sub getPlotCommands {
    # generate commands to create plots; assume commands will be run from plots directory
    my $outDir = shift;
    my $title = shift;
    my @cmds = ();
    my $cmd;
    ### plate heatmaps ###
    my $crHetPath = $WTSI::Genotyping::QC::QCPlotShared::sampleCrHet;
    my $heatMapScript = "$Bin/plate_heatmap_plots.pl";
    my $hmOut = $outDir.'/'.$WTSI::Genotyping::QC::QCPlotShared::plateHeatmapDir; # heatmaps in subdirectory
    unless (-e $hmOut) { push(@cmds, "mkdir $hmOut"); }
    foreach my $mode ('cr', 'het') { # assumes $crHetPath exists and is readable
	$cmd = join(' ', ('cat', $crHetPath, '|', 'perl', $heatMapScript, '--mode='.$mode, '--out_dir='.$hmOut));
	push(@cmds, $cmd);
    }
    # TODO add xydiff command; get data from .sim file and pipe to plotting script?
    my $indexScript = "$Bin/plate_heatmap_index.pl";
    my $indexName = $WTSI::Genotyping::QC::QCPlotShared::plateHeatmapIndex;
    push (@cmds, "perl $indexScript $title $hmOut $indexName");
    ### boxplots ###
    my $boxPlotScript = "$Bin/plot_box_bean.pl";
    my @modes = ('cr', 'het');
    my @inputs = ($crHetPath, $crHetPath);
    for (my $i=0; $i<@modes; $i++) {
	 $cmd = join(' ', ('cat', $inputs[$i], '|', 'perl', $boxPlotScript, '--mode='.$modes[$i], 
			   '--out_dir='.$outDir, '--title='.$title));
	 push(@cmds, $cmd);
    }
    # TODO add xydiff box/bean plots
    ### global cr/het density plots: heatmap, scatterplot & histograms ###
    my $globalCrHetScript = "$Bin/plot_cr_het_density.pl";
    my $prefix = $outDir.'/crHetDensity';
    $cmd = join(' ', ('cat',$crHetPath,'|', 'perl', $globalCrHetScript, "--title=".$title, "--out_dir=".$outDir));
    push(@cmds, $cmd);
    ### failure cause breakdowns ###
    my $failPlotScript = "$Bin/plot_fail_causes.pl";
    $cmd = join(' ', ('perl', $failPlotScript, "--input_dir=.", "--output_dir=.", "--title=".$title));
    push(@cmds, $cmd);
    ### html index for all plots ###
    my $plotIndexScript = "$Bin/main_plot_index.pl";
    $cmd = join(' ', ('perl', $plotIndexScript, $title, $outDir));
    push(@cmds, $cmd);
    return @cmds;
}

sub writeInputFiles {
    # read PLINK output and write text files for input to QC.
    my ($plinkPrefix, $outDir, $tests, $failures, $verbose) = @_;
    $tests ||= 0;
    $failures ||= 0;
    my $crStatsExecutable = "/nfs/users/nfs_i/ib5/mygit/github/Gftools/snp_af_sample_cr_bed"; # TODO current path is a temporary measure for testing; needs to be made portable for production
    my $startDir = getcwd;
    chdir($outDir);
    my @cmds = ("perl $Bin/check_identity_bed.pl $plinkPrefix",
		"$crStatsExecutable $plinkPrefix",
		"perl $Bin/check_duplicates_bed.pl $plinkPrefix",
		"perl $Bin/write_gender_files.pl --qc-output=sample_xhet_gender.txt --plots-dir=. $plinkPrefix"
	);
    my @omits = (0,0,0,0);
    #my @omits = (1,1,1,1);
    if ($verbose) { print WTSI::Genotyping::QC::QCPlotTests::timeNow()." Starting QC checks.\n"; }
    WTSI::Genotyping::QC::QCPlotTests::wrapCommandList(\@cmds, $tests, $failures, $verbose, \@omits);
    chdir($startDir);
    return ($tests, $failures);
}

sub run {
    # main method to run script
    my ($plinkPrefix, $outDir, $title, $verbose) = @_;
    $title ||= "Untitled";
    $verbose ||= 1;
    my $inputsOK = checkInputs($plinkPrefix);
    if (not $inputsOK) { die "Cannot read PLINK inputs for $plinkPrefix; exiting"; }
    elsif ($verbose) { print "PLINK input files found.\n"; }
    my ($tests, $failures) = (0,0);
    ($tests, $failures) = writeInputFiles($plinkPrefix, $outDir, $tests, $failures, $verbose);
    ($tests, $failures) = createPlots($plinkPrefix, $outDir, $tests, $failures, $title, $verbose);
    if ($verbose) { print "Finished.\nTotal steps: $tests\nTotal failures: $failures\n"; }
}
