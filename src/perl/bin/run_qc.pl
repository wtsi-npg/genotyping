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
use Cwd qw(getcwd abs_path);
use FindBin qw($Bin);
use WTSI::Genotyping::QC::QCPlotShared; # qcPlots module to define constants
use WTSI::Genotyping::QC::QCPlotTests;

my ($help, $outDir, $simPath, $configPath, $title, $plinkPrefix, $noWrite, $noPlate, $noPlots, $verbose);

GetOptions("help"           => \$help,
	   "output-dir=s"   => \$outDir,
	   "config=s"       => \$configPath,
	   "sim=s"          => \$simPath,
	   "title=s"        => \$title,
	   "no-data-write"  => \$noWrite,
	   "no-plate"       => \$noPlate,
	   "no-plots"       => \$noPlots,
	   "verbose"        => \$verbose
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension)
May include directory names, eg. /home/foo/project where plink files are /home/foo/project.bed, etc.
Options:
--output-dir=PATH   Directory for QC output
--sim=PATH          Path to SIM intensity file for xydiff calculation
--config=PATH       Path to .json file with QC thresholds
--title             Title for this analysis; will appear in plots
--no-data-write     Do not write text input files for plots (plotting will fail unless files already exist)
--no-plate          Do not create plate heatmap plots
--no-plots          Do not create any plots
--help              Print this help text and exit
--verbose           Print additional output to stdout
";
    exit(0);
}

$outDir ||= "./qc";
if (not -e $outDir) { mkdir($outDir); }
$configPath ||= $Bin."/../json/qc_threshold_defaults.json";
$verbose ||= 0;

$plinkPrefix = $ARGV[0];
unless ($plinkPrefix) { die "ERROR: Must supply a PLINK filename prefix!"; }
# disassemble prefix to find absolute path to directory
my @terms = split("/", $plinkPrefix);
my $filePrefix = pop(@terms);
if (@terms) { 
    my $plinkDir = abs_path(join("/", @terms));
    $plinkPrefix = $plinkDir."/".$filePrefix;
}
run($plinkPrefix, $simPath, $configPath, $outDir, $title, $noWrite, $noPlate, $noPlots, $verbose);

sub checkPlinkInputs {
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
    my ($plinkPrefix, $outDir, $tests, $failures, $title, $noPlate, $verbose) = @_;
    $tests ||= 0;
    $failures ||= 0;
    my $startDir = getcwd;
    chdir($outDir);
    my @cmds = getPlotCommands(".", $title, $noPlate);
    my @omits = (0) x ($#cmds+1); 
    #@omits = (1,1,0,0,0,0,0,0,0); ### hack for testing
    if ($verbose) { print WTSI::Genotyping::QC::QCPlotTests::timeNow()." Starting plot generation.\n"; }
    ($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommandList(\@cmds, $tests, $failures, 
									     $verbose, \@omits);
    chdir($startDir);
    return ($tests, $failures);
}

sub getPlotCommands {
    # generate commands to create plots; assume commands will be run from plots directory
    my ($outDir, $title, $noPlate) = @_;
    my @cmds = ();
    my $cmd;
    my %fileNames = WTSI::Genotyping::QC::QCPlotShared::readQCFileNames();
    ### plate heatmaps ###
    my $crHetPath = $fileNames{'sample_cr_het'};
    my $xydiffPath = $fileNames{'xydiff'};
    unless ($noPlate) {
	# creating heatmap plots can take several minutes; may wish to omit for testing
	my $heatMapScript = "$Bin/plate_heatmap_plots.pl";
	my $hmOut = $outDir.'/'.$fileNames{'plate_dir'}; # heatmaps in subdirectory
	unless (-e $hmOut) { push(@cmds, "mkdir $hmOut"); }
	my @modes = ('cr', 'het');
	foreach my $mode (@modes) { # assumes $crHetPath exists and is readable
	    $cmd = join(' ', ('cat', $crHetPath, '|', 'perl', $heatMapScript,'--mode='.$mode,'--out_dir='.$hmOut));
	    push(@cmds, $cmd);
	}
	if  (-r $xydiffPath) { # silently omit xydiff plots if input not available
	    $cmd = join(' ', ('cat', $xydiffPath, '|', 'perl',$heatMapScript,'--mode=xydiff','--out_dir='.$hmOut));
	    push(@cmds, $cmd);
	}
	my $indexScript = "$Bin/plate_heatmap_index.pl";
	my $indexName = $fileNames{'plate_index'};
	push (@cmds, "perl $indexScript $title $hmOut $indexName");
    }
    ### boxplots ###
    my $boxPlotScript = "$Bin/plot_box_bean.pl";
    my @modes = ('cr', 'het');
    my @inputs = ($crHetPath, $crHetPath);
    if (-r $xydiffPath) { # silently omit xydiff plots if input not available
	push(@modes, 'xydiff');
	push(@inputs, $xydiffPath);
    }
    for (my $i=0; $i<@modes; $i++) {
	 $cmd = join(' ', ('cat', $inputs[$i], '|', 'perl', $boxPlotScript, '--mode='.$modes[$i], 
			   '--out_dir='.$outDir, '--title='.$title));
	 push(@cmds, $cmd);
    }
    ### global cr/het density plots: heatmap, scatterplot & histograms ###
    my $globalCrHetScript = "$Bin/plot_cr_het_density.pl";
    my $prefix = $outDir.'/crHetDensity';
    $cmd = join(' ', ('cat',$crHetPath,'|', 'perl', $globalCrHetScript, "--title=".$title, "--out_dir=".$outDir));
    push(@cmds, $cmd);
    ### failure cause breakdowns ###
    my $failPlotScript = "$Bin/plot_fail_causes.pl";
    $cmd = join(' ', ('perl', $failPlotScript, "--title=".$title));
    push(@cmds, $cmd);
    ### html index for all plots ###
    my $plotIndexScript = "$Bin/main_plot_index.pl";
    $cmd = join(' ', ('perl', $plotIndexScript, $outDir, $fileNames{'qc_results'}, $title));
    push(@cmds, $cmd);
    return @cmds;
}

sub writeInputFiles {
    # read PLINK output and write text files for input to QC.
    my ($plinkPrefix, $simPath, $configPath, $outDir, $tests, $failures, $verbose) = @_;
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
    if ($simPath) {
	push(@cmds, "perl $Bin/xydiff.pl --input=$simPath --output=xydiff.txt");
    }
    push(@cmds, "perl $Bin/write_qc_status.pl --config=$configPath");
    my @omits = (0) x ($#cmds+1); 
    #@omits = (1,0,0,0,1,0); ### hack for testing
    if ($verbose) { print WTSI::Genotyping::QC::QCPlotTests::timeNow()." Starting QC checks.\n"; }
    ($tests, $failures) = WTSI::Genotyping::QC::QCPlotTests::wrapCommandList(\@cmds, $tests, $failures, 
									     $verbose, \@omits);
    chdir($startDir);
    return ($tests, $failures);
}

sub run {
    # main method to run script
    my ($plinkPrefix, $simPath, $configPath, $outDir, $title, $noWrite, $noPlate, $noPlots, $verbose) = @_;
    $title ||= "Untitled";
    $verbose ||= 1;
    if (not -d $outDir || not -w $outDir) { die "Output directory $outDir not writable: $!"; }
    my ($tests, $failures) = (0,0);
    unless ($noWrite) {
	my $inputsOK = checkPlinkInputs($plinkPrefix);
	if (not $inputsOK) { die "Cannot read PLINK inputs for $plinkPrefix; exiting"; }
	elsif ($verbose) { print "PLINK input files found.\n"; }
	if (not $simPath) { print "Path to .sim intensity file not supplied; omitting xydiff calculation.\n"; }
	elsif (not -r $simPath) { die "Cannot read .sim input path $simPath: $!"; }
	elsif ($verbose) { print ".sim input file found.\n"; }
	($tests, $failures) = writeInputFiles($plinkPrefix, $simPath, $configPath,
					      $outDir, $tests, $failures, $verbose);
    }
    unless ($noPlots) {
	($tests, $failures) = createPlots($plinkPrefix, $outDir, $tests, $failures, $title, $noPlate, $verbose);
    }
    if ($verbose) { print "Finished.\nTotal steps: $tests\nTotal failures: $failures\n"; }
}
