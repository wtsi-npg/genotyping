#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# extract information from PLINK data; generate QC plots and reports

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Cwd qw(getcwd abs_path);
use FindBin qw($Bin);
use WTSI::Genotyping::QC::PlinkIO qw(checkPlinkBinaryInputs);
use WTSI::Genotyping::QC::QCPlotShared qw(defaultJsonConfig defaultTexIntroPath readQCFileNames);
use WTSI::Genotyping::QC::Reports qw(createReports);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $CR_STATS_EXECUTABLE = "/software/varinf/bin/genotype_qc/snp_af_sample_cr_bed";

my ($help, $outDir, $simPath, $dbPath, $iniPath, $configPath, $title, $plinkPrefix, $boxtype, $runName);

GetOptions("help"           => \$help,
	   "output-dir=s"   => \$outDir,
	   "config=s"       => \$configPath,
	   "sim=s"          => \$simPath,
	   "dbpath=s"       => \$dbPath,
	   "inipath=s"      => \$iniPath,
	   "title=s"        => \$title,
	   "boxmode=s"      => \$boxtype,
	   "run=s"          => \$runName,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] PLINK_GTFILE
PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension). May include directory names, eg. /home/foo/project where plink files are /home/foo/project.bed, etc.
Options:
--output-dir=PATH   Directory for QC output
--sim=PATH          Path to SIM intensity file for xydiff calculation
--dbpath=PATH       Path to pipeline database .db file
--inipath=PATH      Path to .ini file containing general pipeline and database configuration; local default is $DEFAULT_INI
--run=NAME          Name of run in pipeline database (needed for database update from gender check)
--config=PATH       Path to .json file with QC thresholds; default is taken from inipath
--title             Title for this analysis; will appear in plots
--boxtype           Keyword for boxplot type; must be one of 'box', 'bean', or 'both'; defaults to 'both'
";
    exit(0);
}

### process options and validate inputs
$plinkPrefix = processPlinkPrefix($ARGV[0]);
$iniPath ||= $DEFAULT_INI;
$iniPath = verifyAbsPath($iniPath);
$configPath ||= defaultJsonConfig($iniPath);
$configPath = verifyAbsPath($configPath);
if ($simPath) { $simPath = verifyAbsPath($simPath); }
if ($dbPath) { $dbPath = verifyAbsPath($dbPath); }
$outDir ||= "./qc";
if (not -e $outDir) { mkdir($outDir); }
elsif (not -w $outDir) { croak "Cannot write to output directory ".$outDir; }
$outDir = abs_path($outDir);
$title ||= getDefaultTitle($outDir); 
$boxtype ||= "both";
my $texIntroPath = defaultTexIntroPath($iniPath);
$texIntroPath = verifyAbsPath($texIntroPath);

### run QC
run($plinkPrefix, $simPath, $dbPath, $iniPath, $configPath, $runName, $outDir, $title, $boxtype, $texIntroPath);

sub cleanup {
    # create a 'supplementary' directory in current working directory
    # move less important files (not directories) into supplementary
    my @retain = qw(pipeline_summary.pdf pipeline_summary.csv 
                    plate_heatmaps.html);
    my %retain;
    my $sup = "supplementary";
    foreach my $name (@retain) { $retain{$name} = 1; }
    system("rm -f Rplots.pdf"); # empty default output from R scripts
    system("mkdir -p $sup");
    my $heatmapIndex = "plate_heatmaps/index.html";
    if (-e $heatmapIndex) {
        system("ln -s $heatmapIndex plate_heatmaps.html");
    }
    foreach my $name (glob("*")) {
        if (-d $name || $retain{$name} ) { next; }
        else { system("mv $name $sup"); }
    }
    return 1;
}

sub getBoxBeanCommands {
    my ($dbopt, $iniPath, $outDir, $title, $xydiff, $boxPlotType, $fileNamesRef) = @_;
    my %fileNames = %$fileNamesRef;
    my $boxPlotScript = "$Bin/plot_box_bean.pl";
    my @modes = ('cr', 'het');
    my @inputs = ($fileNames{'sample_cr_het'}, $fileNames{'sample_cr_het'});
    if ($xydiff) {
	push(@modes, 'xydiff');
	push(@inputs, $fileNames{'xydiff'});
    }
    my @cmds;
    for (my $i=0; $i<@modes; $i++) {
	my $cmd = join(' ', ('cat', $inputs[$i], '|', $boxPlotScript, '--mode='.$modes[$i], 
			   '--out_dir='.$outDir, '--title='.$title, $dbopt, '--inipath='.$iniPath,
			   '--type='.$boxPlotType));
	push(@cmds, $cmd);
    }
    return @cmds;
}

sub getDefaultTitle {
    # default title made up of last 2 non-empty items in path
    my $outDir = shift;
    $outDir = abs_path($outDir);
    my @terms = split(/\//, $outDir);
    my $total = @terms;
    my $title = $terms[$total-2]."/".$terms[$total-1];
    return $title;
}

sub getPlateHeatmapCommands {
    my ($dbopt, $iniPath, $outDir, $title, $xydiff, $fileNamesRef) = @_;
    my %fileNames = %$fileNamesRef;
    my @cmds;
    my $hmOut = $outDir.'/'.$fileNames{'plate_dir'}; # heatmaps in subdirectory
    unless (-e $hmOut) { push(@cmds, "mkdir $hmOut"); }
    my @modes = qw/cr het/;
    my @inputs = ($fileNames{'sample_cr_het'}, $fileNames{'sample_cr_het'});
    if ($xydiff) {
	push(@modes, 'xydiff');
	push(@inputs, $fileNames{'xydiff'});
    }
    foreach my $i (0..@modes-1) {
	push(@cmds, join(" ", ('cat', $inputs[$i], '|', "$Bin/plate_heatmap_plots.pl", "--mode=$modes[$i]", 
			       "--out_dir=$hmOut", $dbopt, "--inipath=$iniPath")));
    }
    push (@cmds, "$Bin/plate_heatmap_index.pl $title $hmOut ".$fileNames{'plate_index'});
    return @cmds;
}

sub processPlinkPrefix {
    # want PLINK prefix to include absolute path, so plink I/O will still work after change of working directory
    # also check that PLINK binary files exist and are readable
    my $plinkPrefix = shift;
    unless ($plinkPrefix) { 
	croak "ERROR: Must supply a PLINK filename prefix!"; 
    } elsif ($plinkPrefix =~ "/") { # prefix is "directory-like"; disassemble to find absolute path
	my @terms = split("/", $plinkPrefix);
	my $filePrefix = pop(@terms);
	$plinkPrefix = abs_path(join("/", @terms))."/".$filePrefix;
    } else {
	$plinkPrefix = getcwd()."/".$plinkPrefix;
    }
    my $ok = checkPlinkBinaryInputs($plinkPrefix);
    unless ($ok) { die "Cannot read plink binary inputs for prefix $plinkPrefix"; }
    return $plinkPrefix;
}

sub verifyAbsPath {
    my $path = shift;
    $path = abs_path($path);
    unless (-r $path) { croak "Cannot read path $path"; }
    return $path;
}

sub run {
    my ($plinkPrefix, $simPath, $dbPath, $iniPath, $configPath, $runName, $outDir, $title, $boxPlotType, $texIntroPath) = @_;
    my $startDir = getcwd;
    my %fileNames = readQCFileNames($configPath);
    ### input file generation ###
    my @cmds = ("$Bin/check_identity_bed.pl --config $configPath $plinkPrefix",
		"$CR_STATS_EXECUTABLE $plinkPrefix",
		"$Bin/check_duplicates_bed.pl $plinkPrefix",
	);
    my $genderCmd = "$Bin/check_xhet_gender.pl --input=$plinkPrefix";
    if ($dbPath) { 
	unless (defined($runName)) { croak "Must supply pipeline run name for database gender update"; }
	$genderCmd.=" --dbfile=".$dbPath." --run=".$runName; 
    }
    push(@cmds, $genderCmd);
    my $xydiff = 0;
    if ($simPath) {
        push(@cmds, "$Bin/intensity_metrics.pl --input=$simPath --magnitude=magnitude.txt --xydiff=xydiff.txt");
        $xydiff = 1;
    }
    my $dbopt = "";
    if ($dbPath) { $dbopt = "--dbpath=$dbPath "; }
    push(@cmds, "$Bin/write_qc_status.pl --config=$configPath $dbopt --inipath=$iniPath");
    ### plot generation ###
    if ($dbopt) { 
        my $cmd = "plot_metric_scatter.pl $dbopt";
        if (!$simPath) { $cmd = $cmd." --no-intensity "; }
        push(@cmds, $cmd); 
    }
    push(@cmds, getPlateHeatmapCommands($dbopt, $iniPath, $outDir, $title, $xydiff, \%fileNames));
    push(@cmds, getBoxBeanCommands($dbopt, $iniPath, $outDir, $title, $xydiff, $boxPlotType, \%fileNames));
    push(@cmds, join(' ', ('cat', $fileNames{'sample_cr_het'}, '|', "$Bin/plot_cr_het_density.pl", 
			   "--title=".$title, "--out_dir=".$outDir)));
    push(@cmds, "$Bin/plot_fail_causes.pl --title=$title");
    push(@cmds, join(' ', ("$Bin/main_plot_index.pl", $outDir, $fileNames{'qc_results'}, $title)));
    ### execute commands ###
    chdir($outDir);
    foreach my $cmd (@cmds) { 
	my $result = system($cmd); 
	unless ($result==0) { croak "Command finished with non-zero exit status: \"$cmd\""; } 
    }
    ### create CSV & PDF reports
    my $resultPath = "qc_results.json";
    my $csvPath = "pipeline_summary.csv";
    my $texPath = "pipeline_summary.tex";
    my $genderThresholdPath = "sample_xhet_gender_thresholds.txt";
    my $qcDir = ".";
    createReports($csvPath, $texPath, $resultPath, $configPath, $dbPath, 
                  $genderThresholdPath, $qcDir, $texIntroPath);
    cleanup();
    chdir($startDir);
    return 1;
}
