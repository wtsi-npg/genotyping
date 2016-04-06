#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;

use Cwd qw(getcwd abs_path);
use File::Basename qw(fileparse);
use File::Copy qw(copy);
use File::Slurp qw(read_file);
use File::Spec::Functions qw(catfile);
use FindBin qw($Bin);
use Getopt::Long;
use JSON;
use List::AllUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;
use YAML qw /DumpFile/;

use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::VCF::PlexResultFinder;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $PERCOLATE_LOG_NAME = 'percolate.log';
our $GENOTYPING_DB_NAME = 'genotyping.db';
our $MODULE_ILLUMINUS = 'Genotyping::Workflows::GenotypeIlluminus';
our $MODULE_ZCALL = 'Genotyping::Workflows::GenotypeZCall';

our $DEFAULT_HOST = 'farm3-head2';
our $DEFAULT_CHUNK_SIZE_SNP = 4000;
our $DEFAULT_CHUNK_SIZE_SAMPLE = 40;
our $DEFAULT_MEMORY = 2048;
our $DEFAULT_ZSTART = 7;
our $DEFAULT_ZTOTAL = 1;

our $VCF_SUBDIRECTORY = 'vcf';
our $PLEX_MANIFEST_SUBDIRECTORY = 'plex_manifests';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'ready_workflow');

my $embedded_conf = "
   log4perl.logger.npg.genotyping.ready_workflow = ERROR, A1, A2

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2           = Log::Log4perl::Appender::File
   log4perl.appender.A2.filename  = $session_log
   log4perl.appender.A2.utf8      = 1
   log4perl.appender.A2.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.syswrite  = 1
";

my $log;

run() unless caller();

sub run {
    my $workdir;
    my $manifest;
    my $smaller;
    my $debug;
    my $dbfile;
    my $run;
    my $egt;
    my $inifile;
    my $log4perl_config;
    my $verbose;
    my $host;
    my $workflow;
    my $chunk_size;
    my $memory;
    my $zstart;
    my $ztotal;
    my @plex_config;

    GetOptions('workdir=s'       => \$workdir,
	       'manifest=s'      => \$manifest,
               'plex_config=s'   => \@plex_config,
	       'host=s'          => \$host,
	       'dbfile=s'        => \$dbfile,
	       'run=s'           => \$run,
	       'egt=s'           => \$egt,
               'inifile=s'       => \$inifile,
	       'verbose'         => \$verbose,
	       'workflow=s'      => \$workflow,
	       'chunk_size=i'    => \$chunk_size,
               'smaller'         => \$smaller,
	       'memory=i'        => \$memory,
	       'zstart=i'        => \$zstart,
	       'ztotal=i'        => \$ztotal,
               'logconf=s'       => \$log4perl_config,
               'debug'           => \$debug,
	       'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
	);

    ### set up logging ###
    if ($log4perl_config) {
        Log::Log4perl::init($log4perl_config);
        $log = Log::Log4perl->get_logger('npg.genotyping.ready_workflow');
    }
    else {
        Log::Log4perl::init(\$embedded_conf);
        $log = Log::Log4perl->get_logger('npg.genotyping.ready_workflow');
        if ($verbose) {
            $log->level($INFO);
        }
        elsif ($debug) {
            $log->level($DEBUG);
        }
    }

    ### validate command-line arguments ###
    $inifile ||= $DEFAULT_INI;
    if (! -e $inifile) {
        $log->logcroak("--inifile argument '", $inifile, "' does not exist");
    }
    if ($workdir) {
        $workdir = abs_path($workdir);
        $log->info("Working directory absolute path is '", $workdir, "'");
    } else {
        $log->logcroak("--workdir argument is required");
    }
    if (!$run) {
        $log->logcroak("--run argument is required");
    }
    if (!$dbfile) {
        $log->logcroak("--dbfile argument is required");
    } elsif (! -e $dbfile) {
        $log->logcroak("--dbfile argument '", $dbfile, "' does not exist");
    }
    if (!$manifest) {
        $log->logcroak("--manifest argument is required");
    } elsif (! -e $manifest) {
        $log->logcroak("--manifest argument '", $manifest,
                       "' does not exist");
    }
    if (defined($egt) && !(-e $egt)) {
        $log->logcroak("--egt argument '", $egt, "' does not exist");
    }
    if (scalar @plex_config == 0) { # get defaults from perl/etc directory
        my $etc_dir = catfile($Bin, "..", "etc");
        foreach my $name (qw/ready_qc_fluidigm.json ready_qc_sequenom.json/) {
            push @plex_config, catfile($etc_dir, $name);
        }
    }
    foreach my $plex_config (@plex_config) {
        if (! -e $plex_config) {
            $log->logcroak("--plex_config argument '", $plex_config,
                           "' does not exist");
        }
    }

    $host ||= $DEFAULT_HOST;
    # illuminus paralellizes by SNP, other callers by sample
    if ($workflow eq 'illuminus') { $chunk_size ||= $DEFAULT_CHUNK_SIZE_SNP; }
    else { $chunk_size ||= $DEFAULT_CHUNK_SIZE_SAMPLE; }
    $memory ||= $DEFAULT_MEMORY;

    # ensure $zstart, $ztotal are initialized before comparison
    $zstart ||= $DEFAULT_ZSTART;
    $ztotal ||= $DEFAULT_ZTOTAL;
    if ($zstart <=0) { $log->logcroak("zstart must be > 0"); }
    if ($ztotal <=0) { $log->logcroak("ztotal must be > 0"); }

    ### create and populate the working directory ###
    make_working_directory($workdir);
    write_config_yml($workdir, $host);

    ### read sample identifiers from pipeline DB & create PlexResultFinder ###
    my @initargs = (name        => 'pipeline',
                    inifile     => $inifile,
                    dbfile      => $dbfile);
    my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
        (@initargs)->connect
            (RaiseError     => 1,
             sqlite_unicode => 1,
             on_connect_do  => 'PRAGMA foreign_keys = ON');
    my @samples = $pipedb->sample->all;
    my @sample_ids = uniq map { $_->sanger_sample_id } @samples;
    my $finder = WTSI::NPG::Genotyping::VCF::PlexResultFinder->new(
        sample_ids => \@sample_ids,
        logger     => $log,
        subscriber_config => \@plex_config,
    );

    ### write plex manifests and VCF to working directory ###
    my $manifest_dir = catfile($workdir, $PLEX_MANIFEST_SUBDIRECTORY);
    my $plex_manifests = $finder->write_manifests($manifest_dir);
    my $vcf_dir = catfile($workdir, $VCF_SUBDIRECTORY);
    my $vcf = $finder->write_vcf($vcf_dir);

    ### if required, copy manifest, database and EGT to working directory ###
    unless ($smaller) {
        $dbfile = copy_file_to_directory($dbfile, $workdir);
        $manifest = copy_file_to_directory($manifest, $workdir);
        if (defined($egt)) {
            $egt = copy_file_to_directory($egt, $workdir);
        }
    }
    write_workflow_yml($workdir, $workflow, $dbfile, $run, $manifest,
                       $chunk_size, $memory, $vcf, $plex_manifests,
                       $egt, $zstart, $ztotal);
    $log->info("Finished; genotyping pipeline directory '", $workdir,
               "' is ready to run Percolate.");
}

sub copy_file_to_directory {
    # convenience method to copy a file and return the destination file path
    my ($source, $dir) = @_;
    my $filename = fileparse($source);
    my $dest = catfile($dir, $filename);
    copy($source, $dest) || $log->logcroak("Cannot copy '", $source,
                                           "' to '", $dest, "'");
    return $dest;
}

sub make_working_directory {
    # make in, pass, fail if needed; copy dbfile to working directory
    # if $include_qc_plex, also create vcf and plex_manifest subdirs
    my ($workdir) = @_;
    if (-e $workdir) {
        if (-d $workdir) {
            $log->info("Working directory '", $workdir, "' already exists");
        } else {
            $log->logcroak("--workdir argument '", $workdir,
                           "' exists and is not a directory");
        }
    } else {
        mkdir $workdir || $log->logcroak("Cannot create directory '",
                                         $workdir, "'");
        $log->info("Created working directory '", $workdir, "'");
    }
    # create subdirectories
    my @names = ('in', 'pass', 'fail', $VCF_SUBDIRECTORY,
                 $PLEX_MANIFEST_SUBDIRECTORY);
    foreach my $name (@names) {
        my $subdir = catfile($workdir, $name);
        if (-e $subdir) {
            if (-d $subdir) {
                $log->debug("Subdirectory '", $subdir, "' already exists");
            } else {
                $log->logcroak("Expected subdirectory path '", $subdir,
                               "' exists and is not a directory");
            }
        } else {
            mkdir($subdir) || $log->logcroak("Cannot create subdirectory '",
                                             $subdir, "'");
            $log->debug("Created subdirectory '", $subdir, "'");
        }
    }
}

sub write_config_yml {
    my ($workdir, $host) = @_;
    my %config = (
	'root_dir' => $workdir,
	'log' => catfile($workdir, $PERCOLATE_LOG_NAME),
	'log_level' => 'DEBUG',
	'msg_host' => $host,
	'msg_port' => '11300',
	'async' => 'lsf',
	'max_processes' => '250'
    );
    my $config_path = catfile($workdir, 'config.yml');
    $log->info("Wrote config YML to '", $config_path, "'");
    DumpFile($config_path, (\%config));
    return $config_path;
}

sub write_workflow_yml {
    my ($workdir, $workflow, $dbpath, $run, $manifest, $chunk_size,
        $memory, $vcf, $plex_manifests, $egt, $zstart, $ztotal) = @_;
    my %workflow_args = (
	'chunk_size' => $chunk_size,
	'memory' => $memory,
	'manifest' => $manifest,
        'plex_manifest' => $plex_manifests,
        'vcf' => $vcf,
    );
    my $workflow_module;
    if ($workflow eq 'illuminus') {
        $workflow_args{'gender_method'} = 'Supplied';
        $workflow_module = $MODULE_ILLUMINUS;
    } elsif ($workflow eq 'zcall') {
        if (!($egt && $zstart && $ztotal)) {
            $log->logcroak("Must specify EGT, zstart, and ztotal for ",
                           "zcall workflow");
        } elsif (! -e $egt) {
            $log->logcroak("EGT file '", $egt, "' does not exist.");
        } else {
            $workflow_args{'egt'} = $egt;
            $workflow_args{'zstart'} = $zstart;
            $workflow_args{'ztotal'} = $ztotal;
            $workflow_module = $MODULE_ZCALL;
        }
    } else {
        $log->logcroak("Invalid workflow argument '", $workflow,
                       "'; must be one of illuminus, zcall");
    }
    my @args = ($dbpath, $run, $workdir, \%workflow_args);
    my %params = (
	'library'   => 'genotyping',
	'workflow'  => $workflow_module,
	'arguments' => \@args,
    );
    my $out = catfile($workdir, "in", "genotype_".$workflow.".yml");
    $log->info("Wrote workflow YML to '", $out, "'");
    DumpFile($out, (\%params));
}


__END__


=head1 NAME

ready_workflow

=head1 SYNOPSIS

ready_workflow [--dbfile <SQLite file path>] [--help]
  --manifest <manifest_path> --run <run_name> [--egt <egt_path>]
  [--memory <memory>] [--host <hostname>] [--plex_config <json_path>]
  [--verbose] --workdir <directory path> --workflow <workflow_name>

Options:

  --chunk_size    Chunk size for parallelization. Optional, defaults to
                  4000 (SNPs) for Illuminus or 40 (samples) for zCall.
  --dbfile        Path to an SQLite pipeline database file. Required.
  --egt           Path to an Illumina .egt cluster file. Required for zcall.
  --help          Display help.
  --host          Name of host machine for the beanstalk message queue.
                  Optional, defaults to farm3-head2.
  --manifest      Path to the .bpm.csv manifest file. Required.
  --memory        Memory limit hint for LSF, in MB. Default = 2048.
  --plex_config   Path to a JSON file with parameters to query iRODS and
                  write QC plex data as VCF. May be supplied more than once
                  to specify multiple files. Optional, defaults to a standard
                  set of config files.
  --run           The pipeline run name in the database. Required.
  --smaller       Do not copy the .egt, manifest, and plex manifest files to
                  the workflow directory. Uses less space, but makes the
                  analysis directory less self-contained.
  --verbose       Print messages while processing. Optional.
  --workdir       Working directory for pipeline run. Required.
  --workflow      Pipeline workflow for which to create a .yml file. If
                  supplied, must be 'illuminus' or 'zcall'.
                  If absent, only config.yml will be generated.
  --zstart        Start of zscore range, used for zCall only. Default = 7.
  --ztotal        Number of zscores in range, for zCall only. Default = 1.

=head1 DESCRIPTION

Create and populate a working directory for the genotyping pipeline.
Items in the populated directory include:

=over

=item *

The config.yml file for Percolate

=item *

The genotyping YML file with parameters for the pipeline workflow,
placed in the 'in' subdirectory

=item *

VCF files containing the qc plex calls (if any)

=item *

A copy of the SQLite genotyping database file

=back

=head2 Configuration file format

The script requires one or more JSON files with config parameters. If none
are specified by the user, default files will be used. The defaults are
located in the perl/etc directory.

Each configuration file must be a single hash in JSON format. Keys and values
correspond to construction arguments for Subscriber objects, with one
exception: The 'platform' key denotes a genotyping platform (eg. 'sequenom'
or 'fluidigm').

B<Required> key/value pairs are:

=over

=item *

I<platform>: String denoting a genotyping platform: 'sequenom' or 'fluidigm'

=item *

I<snpset_name>: Name of the QC plex SNP set: Eg. "W35961".

=back

Other key/value pairs are optional, and will receive default values if
not specified in the JSON config. These are:

=over

=item *

I<callset>: Identifier for the callset read by the Subscriber

=item *

I<data_path>: iRODS path under which the input data are found

=item *

I<reference_path>: iRODS path under which the reference and SNP set data
are found

=item *

I<repository>: Root directory containing NPG genome references

=item *

I<read_snpset_version>: SNP set version in iRODs metadata, used to read assay
results

=item *

I<write_snpset_version>: SNP set version in iRODs metadata, used to write VCF

=back


=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
