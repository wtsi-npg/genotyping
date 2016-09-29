#! /software/bin/perl

# Author:  Iain Bancarz, ib5@sanger.ac.uk
# May 2012

# extract information from PLINK data; generate QC plots and reports

use strict;
use warnings;
use Getopt::Long;
use Cwd qw(getcwd abs_path);
use File::Basename;
use FindBin qw($Bin);
use Log::Log4perl qw(:levels);
use Pod::Usage;

use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);
use WTSI::NPG::Genotyping::Version qw(write_version_log);
use WTSI::NPG::Genotyping::QC::Collation qw(collate readMetricThresholds);
use WTSI::NPG::Genotyping::QC::Identity;
use WTSI::NPG::Genotyping::QC::PlinkIO qw(checkPlinkBinaryInputs);
use WTSI::NPG::Genotyping::QC::QCPlotShared qw(defaultConfigDir defaultJsonConfig defaultTexIntroPath readQCFileNames);
use WTSI::NPG::Genotyping::QC::Reports qw(createReports);
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';
our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $CR_STATS_EXECUTABLE = "snp_af_sample_cr_bed";
our $MAF_HET_EXECUTABLE = "het_by_maf.py";

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'run_qc');
my $log;

run() unless caller();

sub run {

    my ($outDir, $simPath, $dbPath, $iniPath, $configPath, $title,
        $plinkPrefix, $runName, $mafHet, $filterConfig, $zcallFilter,
        $illuminusFilter, $include, $plexManifests, $vcf, $sampleJson,
        $log4perl_config, $verbose, $debug, $plinkRaw);

    GetOptions("help"              => sub { pod2usage(-verbose => 2,
                                                      -exitval => 0) },
               "plink=s"           => \$plinkRaw,
               "output-dir=s"      => \$outDir,
               "config=s"          => \$configPath,
               "sim=s"             => \$simPath,
               "dbpath=s"          => \$dbPath,
               "inipath=s"         => \$iniPath,
               "title=s"           => \$title,
               "run=s"             => \$runName,
               "vcf=s"             => \$vcf,
               "mafhet"            => \$mafHet,
               "filter=s"          => \$filterConfig,
               "zcall-filter"      => \$zcallFilter,
               "illuminus-filter"  => \$illuminusFilter,
               "include"           => \$include,
               "plex-manifests=s"  => \$plexManifests,
               "sample-json=s"     => \$sampleJson,
               "logconf=s"         => \$log4perl_config,
               "verbose"           => \$verbose,
               "debug"             => \$debug,
           );


    my @log_levels;
    if ($debug) { push @log_levels, $DEBUG; }
    if ($verbose) { push @log_levels, $INFO; }
    log_init(config => $log4perl_config,
             file   => $session_log,
             levels => \@log_levels);
    $log = Log::Log4perl->get_logger('main');

    ### process options and validate inputs
    if (defined($plinkRaw)) {
        $plinkPrefix = processPlinkPrefix($plinkRaw);
    }
    $iniPath ||= $DEFAULT_INI;
    $iniPath = verifyAbsPath($iniPath);
    $configPath ||= defaultJsonConfig($iniPath);
    $configPath = verifyAbsPath($configPath);

    if ($simPath) { $simPath = verifyAbsPath($simPath); }
    $dbPath = verifyAbsPath($dbPath);
    $outDir ||= "./qc";
    $mafHet ||= 0;
    if (not -e $outDir) { mkdir($outDir); }
    elsif (not -w $outDir) {
        $log->logcroak("Cannot write to output directory $outDir");
    }
    $outDir = abs_path($outDir);
    $title ||= getDefaultTitle($outDir);
    my $texIntroPath = defaultTexIntroPath($iniPath);
    $texIntroPath = verifyAbsPath($texIntroPath);

    $filterConfig = getFilterConfig($filterConfig, $zcallFilter,
                                    $illuminusFilter);
    $include ||= 0;
    my $exclude = !($include);

    # split comma-separated path lists for identity check
    # Use instead of eg. "--config foo.json --config bar.json" for
    # compatibility with Percolate cli_args_map function
    my @vcf;
    my @plexManifests;
    if ($vcf && $plexManifests) {
        @vcf = split(/,/msx, $vcf);
        foreach my $vcf_path (@vcf) {
            unless (-e $vcf_path) {
                $log->logcroak("VCF path '", $vcf_path,
                               "' does not exist. Paths must be supplied as ",
                               "a comma-separated list; individual paths ",
                               "cannot contain commas.");
            }
        }
        @plexManifests = split(/,/msx, $plexManifests);
        foreach my $plex_path (@plexManifests) {
            unless (-e $plex_path) {
                $log->logcroak("Plex manifest path '", $plex_path,
                               "' does not exist. Paths must be supplied as ",
                               "a comma-separated list; individual paths ",
                               "cannot contain commas.");
            }
        }
    } elsif ($vcf && !$plexManifests) {
        $log->logcroak("--vcf argument must be accompanied by a",
                       " --plex-manifests argument");
    } elsif (!$vcf && $plexManifests) {
        $log->logcroak("--plex-manifests argument must be accompanied by a",
                       " --vcf argument");
    }
    ### run QC
    run_qc($plinkPrefix, $simPath, $dbPath, $iniPath, $configPath,
           $runName, $outDir, $title, $texIntroPath, $mafHet, $filterConfig,
           $exclude, \@plexManifests, \@vcf, $sampleJson);

}

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
    my @terms = split /\//msx, $outDir;
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
	$log->logcroak("Incorrect options; must specify at most one of",
                       " --filter, --illuminus-filter, --zcall-filter");
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
    if ($plinkPrefix =~ "/") {
        # prefix is "directory-like"; disassemble to find absolute path
	my @terms = split("/", $plinkPrefix);
	my $filePrefix = pop(@terms);
	$plinkPrefix = abs_path(join("/", @terms))."/".$filePrefix;
    } else {
	$plinkPrefix = getcwd()."/".$plinkPrefix;
    }
    my $ok = checkPlinkBinaryInputs($plinkPrefix);
    unless ($ok) {
      $log->logcroak("Cannot read plink binary inputs for prefix '",
                     $plinkPrefix, "'");
    }
    return $plinkPrefix;
}

sub verifyAbsPath {
    my $path = shift;
    my $cwd = getcwd();
    unless (-e $path) { 
      $log->logcroak("Path '", $path, "' does not exist relative to ",
                     "current directory '", $cwd, "'");
    }
    $path = abs_path($path);
    return $path;
}

sub run_qc_wip {
  # run the work-in-progess refactored QC in parallel with the old one
  my ($plinkPrefix, $outDir, $plexManifestRef, $vcfRef, $sampleJson) = @_;
  $outDir = $outDir."/qc_wip";
  mkdir($outDir);
  my $script = "check_identity_bed_wip.pl";
  my $jsonPath = $outDir."/identity_wip.json";
  my $csvPath = $outDir."/identity_wip.csv";
  my $vcf = join(',', @{$vcfRef});
  my $plexManifest = join(',', @{$plexManifestRef});
  my @args = ("--json=$jsonPath",
              "--csv=$csvPath",
	      "--plink=$plinkPrefix",
	      "--plex=$plexManifest",
              "--sample_json=$sampleJson",
              "--vcf=$vcf"
	     );
  my $cmd = $script." ".join(" ", @args);
  my $result = system($cmd);
  if ($result!=0) {
    $log->logcroak("Command finished with non-zero exit status: '",
                   $cmd, "'");
  }

}

sub run_qc {
    my ($plinkPrefix, $simPath, $dbPath, $iniPath, $configPath,
        $runName, $outDir, $title, $texIntroPath, $mafHet, $filter,
        $exclude, $plexManifest, $vcf, $sampleJson) = @_;
    if ($plexManifest && $vcf && $sampleJson) {
        run_qc_wip($plinkPrefix, $outDir, $plexManifest, $vcf, $sampleJson);
    }
    write_version_log($outDir);
    my %fileNames = readQCFileNames($configPath);
    ### input file generation ###
    my @cmds = ("$CR_STATS_EXECUTABLE -r $outDir/snp_cr_af.txt -s $outDir/sample_cr_het.txt $plinkPrefix",
		"$Bin/check_duplicates_bed.pl  --dir $outDir $plinkPrefix",
	);
    my $genderCmd = "$Bin/check_xhet_gender.pl --input=$plinkPrefix --output-dir=$outDir";
    if (!defined($runName)) {
      $log->logcroak("Must supply pipeline run name for database ",
                     "gender update");
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
          $log->logcroak("Command finished with non-zero exit status: '",
                         $cmd, "'");
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
    if (!(-e $idJson)) {
      $log->logcroak("Identity JSON file '", $idJson, "' does not exist");
    }
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
           $log->logcroak("Command finished with non-zero exit status: '",
                          $cmd, "'");
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


__END__


=head1 NAME

run_qc

=head1 SYNOPSIS

run_qc.pl [ options ] PLINK_STEM

PLINK_STEM is the prefix for binary plink files (without .bed, .bim, .fam
extension). May include directory names, eg. /home/foo/project where plink
files are /home/foo/project.bed, etc.

Options:

  --plink               Prefix for binary plink files (without .bed, .bim,
                        .fam extension). May include directory names,
                        eg. /home/foo/project where files are
                        /home/foo/project.bed, etc.

  --output-dir=PATH     Directory for QC output

  --sim=PATH            Path to SIM file for intensity metrics.
                        See note [1] below.

  --dbpath=PATH         Path to pipeline database .db file. Required.

  --inipath=PATH        Path to .ini file containing general pipeline and
                        database configuration; local default is $DEFAULT_INI

  --vcf=STR             Comma-separated list of paths to VCF files containing
                        QC plex calls for alternate identity check. See
                        note [2] below.

  --plex-manifests=STR  Comma-separated list of paths to .tsv manifests for
                        QC plexes. See note [2].

  --run=NAME            Name of run in pipeline database (needed for database
                        update from gender check)

  --config=PATH         Path to JSON config file; default is taken from
                        inipath

  --mafhet              Find heterozygosity separately for SNP populations
                        with minor allele frequency greater than 1%, and
                        less than 1%.

  --sample-json=PATH    Sample JSON file to relate Sanger sample IDs in VCF
                        to sample URIs in Plink data.

  --title               Title for this analysis; will appear in plots

  --zcall-filter        Apply default zcall filter; see note [3] below.

  --illuminus-filter    Apply default illuminus filter; see note [3].

  --filter=PATH         Read custom filter criteria from PATH. See note [3].

  --include             Do not exclude failed samples from the pipeline DB.
                        See note [3] below.

=head2 NOTES

=over

=item 1.

If --sim is not specified, but the intensity files magnitude.txt and
xydiff.txt are present in the pipeline output directory, intensity metrics
will be read from the files. This allows intensity metrics to be computed only
once when multiple callers are used on the same dataset.

=item 2.

The --plex-manifest and --vcf options, with appropriate arguments,
are required to run the alternate identity check. If both these
options are not specified, the check will be omitted. Arguments to both
options are comma-separated lists of file paths; the individual paths may
not contain commas. The order of paths is not significant.

=item 3.

The --zcall, --illuminus, and --filter options enable \"prefilter\" mode:

=over 2

=item *

Samples which fail the filter criteria are excluded in the pipeline
SQLite DB. This ensures that failed samples are not input to
subsequent analyses using the same DB.

=item *

Filter criteria are determined by one of the following options:

=over 3

=item 1.

--illuminus     Default illuminus criteria

=item 2.

--zcall         Default zcall criteria

=item 3.

--filter=PATH   Custom criteria, given by the JSON file at PATH.

=back

=item *

If more than one of the above options is specified, an error is
raised. If none of them is specified, no filtering is carried out.

=item *

Additional CSV and JSON summary files are written to describe the
prefilter results.

=item *

If the --include option is in effect, filter summary files will be
written but samples will not be excluded from the SQLite DB.

=back


=back


=head1 DESCRIPTION

Main QC script for genotyping datasets. Runs a suite of QC metrics and
produces reports, plots, and supplementary data files.

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012, 2013, 2014, 2015, 2016 Genome Research Limited.
All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
