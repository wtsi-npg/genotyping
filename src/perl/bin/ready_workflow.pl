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
use List::AllUtils qw(none uniq);
use Log::Log4perl qw(:levels);
use Pod::Usage;
use Try::Tiny;
use YAML qw /DumpFile/;

use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);
use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::VCF::PlexResultFinder;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $PERCOLATE_LOG_NAME = 'percolate.log';
our $GENOTYPING_DB_NAME = 'genotyping.db';

our $MODULE_GENCALL = 'Genotyping::Workflows::GenotypeGencall';
our $MODULE_ILLUMINUS = 'Genotyping::Workflows::GenotypeIlluminus';
our $MODULE_ZCALL = 'Genotyping::Workflows::GenotypeZCall';
our $GENCALL = 'gencall';
our $ILLUMINUS = 'illuminus';
our $ZCALL = 'zcall';

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
my $log;

run() unless caller();

sub run {
    my $chunk_size;
    my $config_out;
    my $dbfile;
    my $debug;
    my $egt;
    my $host;
    my $inifile;
    my $local;
    my $log4perl_config;
    my $manifest;
    my $memory;
    my $no_filter;
    my $no_plex;
    my $queue;
    my $run;
    my $smaller;
    my $verbose;
    my $workdir;
    my $workflow;
    my $zstart;
    my $ztotal;
    my @plex_config;

    GetOptions('help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
	       'chunk_size=i'    => \$chunk_size,
               'config_out=s'    => \$config_out,
	       'dbfile=s'        => \$dbfile,
               'debug'           => \$debug,
	       'egt=s'           => \$egt,
	       'host=s'          => \$host,
               'inifile=s'       => \$inifile,
               'local'           => \$local,
               'logconf=s'       => \$log4perl_config,
	       'manifest=s'      => \$manifest,
	       'memory=i'        => \$memory,
               'no_filter'       => \$no_filter,
               'no_plex'         => \$no_plex,
               'plex_config=s'   => \@plex_config,
               'queue=s'         => \$queue,
	       'run=s'           => \$run,
               'smaller'         => \$smaller,
	       'verbose'         => \$verbose,
               'workdir=s'       => \$workdir,
	       'workflow=s'      => \$workflow,
	       'zstart=i'        => \$zstart,
	       'ztotal=i'        => \$ztotal,
	);

    my @log_levels;
    if ($debug) { push @log_levels, $DEBUG; }
    if ($verbose) { push @log_levels, $INFO; }
    log_init(config => $log4perl_config,
             file   => $session_log,
             levels => \@log_levels);
    $log = Log::Log4perl->get_logger('main');

    ### process command-line arguments ###

    # required arguments for all workflows
    my %required_args = (
        workdir  => $workdir,
        run      => $run,
        workflow => $workflow,
        dbfile   => $dbfile,
        manifest => $manifest
    );
    my @required_args = sort keys %required_args;
    my @workflows = ($GENCALL, $ILLUMINUS, $ZCALL);
    foreach my $name (@required_args) {
        if (! defined $required_args{$name}) {
            $log->logcroak("Argument --$name is required");
        }
        if ($name eq 'workflow') {
            if (none { $_ eq $required_args{$name} } @workflows) {
                $log->logcroak('--$name argument must be one of: ',
                               join(', ', @workflows));
            }
        } elsif ($name eq 'dbfile' || $name eq 'manifest') {
            if (! -e $required_args{$name}) {
                $log->logcroak("Path argument to --$name does not exist: '",
                               $required_args{$name}, "'");
            }
        }
    }

    # optional arguments for all workflows
    $memory  ||= $DEFAULT_MEMORY;
    $host    ||= $DEFAULT_HOST;
    $inifile ||= $DEFAULT_INI;
    if (! -e $inifile) {
        $log->logcroak("--inifile argument '", $inifile, "' does not exist");
    }
    if (! defined $config_out) {
        $config_out = workflow_config_path($workdir, $workflow, $local);
    }
    foreach my $plex_config (@plex_config) {
        if (! -e $plex_config) {
            $log->logcroak("--plex_config argument '", $plex_config,
                           "' does not exist");
        }
    }

    # arguments for illuminus and zcall only
    if (defined $no_filter) {
        if ($workflow eq $GENCALL) {
            $log->logcroak('--nofilter option is not compatible with the ',
                           $GENCALL, 'workflow');
        }
        $no_filter = 'true';  # Boolean value for Ruby
    } else {
        $no_filter = 'false';
    }
    if (defined $chunk_size) {
        if ($workflow eq $GENCALL) {
            $log->logcroak('--chunk_size option is not compatible with the ',
                           $GENCALL, 'workflow');
        }
    } elsif ($workflow eq $ILLUMINUS) {
        $chunk_size = $DEFAULT_CHUNK_SIZE_SNP;
    } else {
        $chunk_size = $DEFAULT_CHUNK_SIZE_SAMPLE;
    }

    # arguments for zcall only
    my $msg = " argument is only compatible with the $ZCALL workflow;".
        " given workflow is '$workflow'";
    if ($workflow eq $ZCALL) {
        # check EGT & assign zstart/ztotal defaults
        if (! defined $egt) {
            $log->logcroak("--egt argument is required for $ZCALL workflow");
        } elsif (! -e $egt) {
            $log->logcroak("--egt argument '$egt' does not exist");
        }
        $zstart ||= $DEFAULT_ZSTART;
        $ztotal ||= $DEFAULT_ZTOTAL;
        if ($zstart <=0) { $log->logcroak("zstart must be > 0"); }
        if ($ztotal <=0) { $log->logcroak("ztotal must be > 0"); }
    } elsif (defined $egt) {
        $log->logcroak('--egt', $msg);
    } elsif (defined $zstart) {
        $log->logcroak('--zstart', $msg);
    } elsif (defined $ztotal) {
        $log->logcroak('--ztotal', $msg);
    }

    ### create and populate the working directory ###
    make_working_directory($workdir, $local);
    if ($local) { write_config_yml($workdir, $host); }

    ### find QC plex VCF and manifests (if required) ###
    my ($plex_manifests, $vcf) = write_plex_results(
        $inifile, $dbfile, \@plex_config, $workdir, $log, $no_plex);

    ### if required, copy manifest, database and EGT to working directory ###
    unless ($smaller) {
        $dbfile = copy_file_to_directory($dbfile, $workdir);
        $manifest = copy_file_to_directory($manifest, $workdir);
        if (defined($egt)) {
            $egt = copy_file_to_directory($egt, $workdir);
        }
    }
    ### generate the workflow config and write as YML ###
    my %workflow_args = (
        'manifest'      => $manifest,
        'chunk_size'    => $chunk_size,
        'memory'        => $memory,
        'queue'         => $queue,
        'vcf'           => $vcf,
        'plex_manifest' => $plex_manifests,
    );
    if (defined $no_filter) {
        $workflow_args{'nofilter'} = $no_filter;
    }

    my $workflow_module;
    if ($workflow eq $GENCALL) {
        $workflow_module = $MODULE_GENCALL;
    } elsif ($workflow eq $ILLUMINUS) {
        $workflow_args{'gender_method'} = 'Supplied';
        $workflow_module = $MODULE_ILLUMINUS;
    } elsif ($workflow eq $ZCALL) {
        $workflow_module = $MODULE_ZCALL;
        $workflow_args{'egt'} = $egt;
        $workflow_args{'zstart'} = $zstart;
        $workflow_args{'ztotal'} = $ztotal;
    } else {
        $log->logcroak("Invalid workflow argument '", $workflow,
                       "'; must be one of $GENCALL, $ILLUMINUS, $ZCALL");
    }
    my @args = ($dbfile, $run, $workdir, \%workflow_args);
    my %params = (
	'library'   => 'genotyping',
	'workflow'  => $workflow_module,
	'arguments' => \@args,
    );
    $log->info("Wrote workflow YML to '", $config_out, "'");
    DumpFile($config_out, (\%params));
    $log->info("Finished; genotyping pipeline directory '", $workdir,
               "' was created successfully.");
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
    my ($workdir, $local) = @_;
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
    my @names = ($VCF_SUBDIRECTORY, $PLEX_MANIFEST_SUBDIRECTORY);
    if ($local) {
        push @names, qw/in pass fail/;
    }
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

sub workflow_config_path {
    my ($workdir, $workflow, $local) = @_;
    my $config_dir;
    if ($local) { $config_dir = catfile($workdir, 'in'); }
    else { $config_dir = $workdir; }
    my $config_path = catfile($config_dir, 'genotype_'.$workflow.'.yml');
    return $config_path;
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

sub write_plex_results {
    my ($inifile, $dbfile, $plex_config, $workdir, $log, $no_plex) = @_;
    my $plex_manifests= [];
    my $vcf = [];
    if (!($no_plex)) {
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
        try {
            my $finder = WTSI::NPG::Genotyping::VCF::PlexResultFinder->new(
                sample_ids => \@sample_ids,
                subscriber_config => $plex_config,
            );
            my $manifest_dir = catfile($workdir, $PLEX_MANIFEST_SUBDIRECTORY);
            $plex_manifests = $finder->write_manifests($manifest_dir);
            $log->info("Wrote plex manifests: ",
                       join(', ', @{$plex_manifests}));
            my $vcf_dir = catfile($workdir, $VCF_SUBDIRECTORY);
            $vcf = $finder->write_vcf($vcf_dir);
            $log->info("Wrote VCF: ", join(', ', @{$vcf}));
        } catch {
            $log->logwarn("Unexpected error finding QC plex data in ",
                          "iRODS; VCF and plex manifests not written; ",
                          "run with --verbose for details");
            $log->info("Caught PlexResultFinder error: $_");
        }
    }
    return ($plex_manifests, $vcf);
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
  --config_out    Output path for .yml workflow config file. Optional,
                  defaults to 'genotype_${WORKFLOW_NAME}.yml' in the
                  workflow directory (or the 'in' subdirectory, if --local
                  is in effect).
  --dbfile        Path to an SQLite pipeline database file. Required.
  --egt           Path to an Illumina .egt cluster file. Required for zcall.
  --help          Display help.
  --host          Name of host machine for the beanstalk message queue in
                  Percolate config. Relevant only if --local is in effect.
                  Optional, defaults to farm3-head2.
  --local         Create in/pass/fail subdirectories, to run Percolate
                  locally in the pipeline working directory. Write workflow
                  config to the 'in' subdirectory.  Write a config.yml file
                  for Percolate to the working directory.
  --manifest      Path to the .bpm.csv manifest file. Required.
  --memory        Memory limit hint for LSF, in MB. Default = 2048.
  --no_filter     Enable the 'nofilter' option in workflow config.
  --no_plex       Do not query iRODS for QC plex results.
  --plex_config   Path to a JSON file with parameters to query iRODS and
                  write QC plex data as VCF. May be supplied more than once
                  to specify multiple files. Optional, defaults to a standard
                  set of config files.
  --queue         LSF queue hint for workflow config YML. Optional; if not
                  supplied, LSF will use its default queue.
  --run           The pipeline run name in the database. Required.
  --smaller       Do not copy the .egt, manifest, and plex manifest files to
                  the workflow directory. Uses less space, but makes the
                  analysis directory less self-contained.
  --verbose       Print messages while processing. Optional.
  --workdir       Working directory for pipeline run. Required.
  --workflow      Pipeline workflow for which to create a .yml file.
                  Required; must be 'illuminus' or 'zcall'.
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
