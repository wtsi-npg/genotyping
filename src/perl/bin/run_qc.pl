#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# extract information from PLINK data; generate QC plots and reports

use strict;
use warnings;
use Getopt::Long;
use Carp;
use Cwd qw(getcwd abs_path);
use File::Basename;
use FindBin qw($Bin);
use WTSI::NPG::Genotyping::Version qw(write_version_log);
use WTSI::NPG::Genotyping::QC::Collation qw(collate readMetricThresholds);
use WTSI::NPG::Genotyping::QC::Identity;
use WTSI::NPG::Genotyping::QC::PlinkIO qw(checkPlinkBinaryInputs);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultConfigDir defaultJsonConfig defaultTexIntroPath readQCFileNames);
use WTSI::NPG::Genotyping::QC::Reports qw(createReports);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $CR_STATS_EXECUTABLE = "snp_af_sample_cr_bed";
our $MAF_HET_EXECUTABLE = "het_by_maf.py";

my ($help, $outDir, $simPath, $dbPath, $iniPath, $configPath, $title,
    $plinkPrefix, $runName, $mafHet, $filterConfig, $zcallFilter,
    $illuminusFilter, $include, $plexManifest);

GetOptions("help"              => \$help,
           "output-dir=s"      => \$outDir,
           "config=s"          => \$configPath,
           "sim=s"             => \$simPath,
           "dbpath=s"          => \$dbPath,
           "inipath=s"         => \$iniPath,
           "title=s"           => \$title,
           "run=s"             => \$runName,
           "mafhet"            => \$mafHet,
	   "filter=s"          => \$filterConfig,
	   "zcall-filter"      => \$zcallFilter,
	   "illuminus-filter"  => \$illuminusFilter,
	   "include"           => \$include,
           "plex-manifest"     => \$plexManifest,
    );

if ($help) {
    print STDERR "Usage: $0 [ options ] PLINK_GTFILE

PLINK_GTFILE is the prefix for binary plink files (without .bed, .bim, .fam extension). May include directory names, eg. /home/foo/project where plink files are /home/foo/project.bed, etc.

Options:
--output-dir=PATH   Directory for QC output
--sim=PATH          Path to SIM file for intensity metrics. See note [1] below.
--dbpath=PATH       Path to pipeline database .db file. Required.
--inipath=PATH      Path to .ini file containing general pipeline and database
                    configuration; local default is $DEFAULT_INI
--run=NAME          Name of run in pipeline database (needed for database
                    update from gender check)
--config=PATH       Path to JSON config file; default is taken from inipath
--mafhet            Find heterozygosity separately for SNP populations with
                    minor allele frequency greater than 1%, and less than 1%.
--title             Title for this analysis; will appear in plots
--zcall-filter      Apply default zcall filter; see note [2] below.
--illuminus-filter  Apply default illuminus filter; see note [2] below.
--filter=PATH       Read custom filter criteria from PATH. See note [2] below.
--include           Do not exclude failed samples from the pipeline DB.
                    See note [2] below.

[1] If --sim is not specified, but the intensity files magnitude.txt and
xydiff.txt are present in the pipeline output directory, intensity metrics
will be read from the files. This allows intensity metrics to be computed only
once when multiple callers are used on the same dataset.

[2] The --zcall, --illuminus, and --filter options enable \"prefilter\" mode:
    * Samples which fail the filter criteria are excluded in the pipeline
      SQLite DB. This ensures that failed samples are not input to subsequent
      analyses using the same DB.
    * Filter criteria are determined by one of three options:
      --illuminus     Default illuminus criteria
      --zcall         Default zcall criteria
      --filter=PATH   Custom criteria, given by the JSON file at PATH.
    * If more than one of the above options is specified, an error is raised.
      If none of them is specified, no filtering is carried out.
    * Additional CSV and JSON summary files are written to describe the
      prefilter results.
    * If the --include option is in effect, filter summary files will be
      written but samples will not be excluded from the SQLite DB.
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
$dbPath = verifyAbsPath($dbPath); 
$outDir ||= "./qc";
$mafHet ||= 0;
if (not -e $outDir) { mkdir($outDir); }
elsif (not -w $outDir) { croak "Cannot write to output directory ".$outDir; }
$outDir = abs_path($outDir);
$title ||= getDefaultTitle($outDir); 
my $texIntroPath = defaultTexIntroPath($iniPath);
$texIntroPath = verifyAbsPath($texIntroPath);

$filterConfig = getFilterConfig($filterConfig, $zcallFilter, $illuminusFilter);
$include ||= 0;
my $exclude = !($include);

### run QC
run($plinkPrefix, $simPath, $dbPath, $iniPath, $configPath,
$runName, $outDir, $title, $texIntroPath, $mafHet, $filterConfig, $exclude,
$plexManifest);

sub cleanup {
    # create a 'supplementary' subdirectory of the output directory
    # move less important files (not directories) into supplementary
    my $dir = shift;
    my $cwd = getcwd();
    system("rm -f $cwd/Rplots.pdf"); # empty default output from R scripts
    my @retain = qw(pipeline_summary.pdf pipeline_summary.csv 
                    plate_heatmaps.html filter_results.csv);
    my %retain;
    foreach my $name (@retain) { $retain{$name} = 1; }
    my $heatmapIndex = $dir."/plate_heatmaps/index.html";
    if (-e $heatmapIndex) {
        system("ln -s $heatmapIndex $dir/plate_heatmaps.html");
    }
    my $sup = $dir."/supplementary";
    system("mkdir -p $sup");
    foreach my $name (glob("$dir/*")) {
        if (-d $name || $retain{basename($name)} ) { next; }
        else { system("mv $name $sup"); }
    }
    system("touch ".$sup."/finished.txt");
    return 1;
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

sub getFilterConfig {
    # first check for conflicting filter options
    my @filterOpts = @_;
    my $filters = 0;
    foreach my $opt (@filterOpts) {
	if ($opt) { $filters++; }
    }
    if ($filters > 1) { 
	croak "Incorrect options; must specify at most one of --filter, --illuminus-filter, --zcall-filter";
    }   
    my ($fConfig, $zcallFilter, $illuminusFilter) = @filterOpts;
    # if filter options are OK, check existence of appropriate config file
    my $configDir = defaultConfigDir();
    if ($zcallFilter) { 
	$fConfig = verifyAbsPath($configDir."/zcall_prefilter.json");  
    } elsif ($illuminusFilter) {
	$fConfig = verifyAbsPath($configDir."/illuminus_prefilter.json");  
    } elsif ($fConfig) {
	$fConfig = verifyAbsPath($fConfig); # custom filter
    } else {
	$fConfig = 0; # no filtering
    }
    return $fConfig;    
}

sub getPlateHeatmapCommands {
    my ($dbopt, $iniPath, $dir, $title, $simPathGiven, $fileNamesRef) = @_;
    my %fileNames = %$fileNamesRef;
    my @cmds;
    my $hmOut = $dir.'/'.$fileNames{'plate_dir'}; # heatmaps in subdirectory
    unless (-e $hmOut) { push(@cmds, "mkdir $hmOut"); }
    my @modes = qw/cr het/;
    my $crHet = $dir.'/'.$fileNames{'sample_cr_het'};
    my @inputs = ($crHet, $crHet); 
    if ($simPathGiven) {
        push(@modes, 'magnitude');
        push(@inputs, $dir.'/'.$fileNames{'magnitude'});
    }
    foreach my $i (0..@modes-1) {
        push(@cmds, join(" ", ('cat', $inputs[$i], '|', 
                               "$Bin/plate_heatmap_plots.pl", 
                               "--mode=$modes[$i]", 
                               "--out_dir=$hmOut", $dbopt, 
                               "--inipath=$iniPath")));
    }
    push (@cmds, "$Bin/plate_heatmap_index.pl $title $hmOut ".
          $fileNames{'plate_index'});
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
    my $cwd = getcwd();
    unless (-e $path) { 
	croak "Path '$path' does not exist relative to current ".
	    "directory '$cwd'"; 
    }
    $path = abs_path($path);
    return $path;
}

sub run_qc_wip {
  # run the work-in-progess refactored QC in parallel with the old one
  my ($plinkPrefix, $dbPath, $iniPath, $outDir, $plexManifest) = @_;
  $plexManifest ||= "/nfs/srpipe_references/genotypes/W30467_snp_set_info_1000Genomes.tsv";
  $outDir = $outDir."/qc_wip";
  mkdir($outDir);
  my $script = "check_identity_bed_wip.pl";
  my $outPath = $outDir."/identity_wip.json";
  my @args = ("--config=$iniPath",
	      "--dbfile=$dbPath",
	      "--out=$outPath",
	      "--plink=$plinkPrefix",
	      "--plex_manifest=$plexManifest"
	     );
  my $cmd = $script." ".join(" ", @args);
  my $result = system($cmd);
  if ($result!=0) {
    croak "Command finished with non-zero exit status: \"$cmd\"";
  }

}

sub run {
    my ($plinkPrefix, $simPath, $dbPath, $iniPath, $configPath,
        $runName, $outDir, $title, $texIntroPath, $mafHet, $filter,
        $exclude, $plexManifest) = @_;
    run_qc_wip($plinkPrefix, $dbPath, $iniPath, $outDir, $plexManifest);
    write_version_log($outDir);
    my %fileNames = readQCFileNames($configPath);
    ### input file generation ###
    my @cmds = ("$CR_STATS_EXECUTABLE -r $outDir/snp_cr_af.txt -s $outDir/sample_cr_het.txt $plinkPrefix",
		"$Bin/check_duplicates_bed.pl  --dir $outDir $plinkPrefix",
	);
    my $genderCmd = "$Bin/check_xhet_gender.pl --input=$plinkPrefix --output-dir=$outDir";
    if (!defined($runName)) {
	croak "Must supply pipeline run name for database gender update";
    }
    $genderCmd.=" --dbfile=".$dbPath." --run=".$runName; 
    push(@cmds, $genderCmd);
    if ($mafHet) {
	my $mhout = $outDir.'/'.$fileNames{'het_by_maf'};
	push(@cmds, "$MAF_HET_EXECUTABLE --in $plinkPrefix --out $mhout");
    }
    my $intensity = 0;
    my $magPath = $outDir.'/magnitude.txt';
    my $xydPath = $outDir.'/xydiff.txt';
    if ($simPath) {
        push(@cmds, "simtools qc --infile=$simPath ".
             "--magnitude=$magPath --xydiff=$xydPath");
        $intensity = 1;
    } elsif (-e $magPath && -e $xydPath) {
	# using previously calculated metric values
	$intensity = 1;
    }
    my $dbopt = "--dbpath=$dbPath "; 
    ### run QC data generation commands ###
    foreach my $cmd (@cmds) {
        my $result = system($cmd); 
        if ($result!=0) {
            croak "Command finished with non-zero exit status: \"$cmd\""; 
        }
    }
    ### run identity check ###
    WTSI::NPG::Genotyping::QC::Identity->new(
        db_path => $dbPath,
        ini_path => $iniPath,
        output_dir => $outDir,
        plink_path => $plinkPrefix,
    )->run_identity_check();
    my $idJson = $outDir.'/'.$fileNames{'id_json'};
    if (!(-e $idJson)) { croak "Identity JSON file '$idJson' does not exist!"; }
    ### collate inputs, write JSON and CSV ###
    my $csvPath = $outDir."/pipeline_summary.csv";
    my $statusJson = $outDir."/qc_results.json";
    my $metricJson = "";
    # first pass -- standard thresholds, no DB update
    my @allMetricNames = keys(%{readMetricThresholds($configPath)});
    my @metricNames = ();
    foreach my $metric (@allMetricNames) {
	if ($intensity || ($metric ne 'magnitude' && $metric ne 'xydiff')) {
	    push(@metricNames, $metric);
	}
    }
    collate($outDir, $configPath, $configPath, $dbPath, $iniPath, 
	    $statusJson, $metricJson, $csvPath, 0, \@metricNames);
    ### plot generation ###
    @cmds = ();
    if ($dbopt) { 
        my $cmd = "$Bin/plot_metric_scatter.pl $dbopt --inipath=$iniPath --config=$configPath --outdir=$outDir --qcdir=$outDir";
        if (!$simPath) { $cmd = $cmd." --no-intensity "; }
        push(@cmds, $cmd); 
    }
    push(@cmds, getPlateHeatmapCommands($dbopt, $iniPath, $outDir, $title, 
                                        $intensity, \%fileNames));
    my @densityTerms = ('cat', $outDir.'/'.$fileNames{'sample_cr_het'}, '|', 
                        "$Bin/plot_cr_het_density.pl",  "--title=".$title, 
                        "--out_dir=".$outDir);
    push(@cmds, join(' ', @densityTerms));
    push(@cmds, "$Bin/plot_fail_causes.pl --title=$title --inipath=$iniPath  --config=$configPath --input $outDir/qc_results.json --cr-het $outDir/sample_cr_het.txt --output-dir $outDir");
    ### execute commands ###
    foreach my $cmd (@cmds) { 
        my $result = system($cmd); 
        if ($result!=0) { 
            croak "Command finished with non-zero exit status: \"$cmd\""; 
        } 
    }
    ### create PDF report
    my $texPath = $outDir."/pipeline_summary.tex";
    my $genderThresholdPath = $outDir."/sample_xhet_gender_thresholds.txt";
    createReports($texPath, $statusJson, $idJson, $configPath, $dbPath, 
                  $genderThresholdPath, $outDir, $texIntroPath);
    ### exclude failed samples from pipeline DB
    if ($filter) {
	# second pass -- evaluate filter metrics/thresholds
	# update DB unless the --include option is in effect
	$csvPath = $outDir."/filter_results.csv"; 
	$statusJson = $outDir."/filter_results.json";
	collate($outDir, $configPath, $filter, $dbPath, $iniPath, 
		$statusJson, $metricJson, $csvPath, $exclude);
    }
    ## create 'supplementary' directory and move files
    cleanup($outDir);
    return 1;
}
