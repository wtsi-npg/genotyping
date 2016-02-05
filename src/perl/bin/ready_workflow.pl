#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;

use Cwd qw(getcwd abs_path);
use File::Copy qw(copy);
use File::Spec::Functions qw(catfile);
use FindBin qw($Bin);
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;
use YAML qw /DumpFile/;

use WTSI::NPG::Utilities qw(user_session_log);

# Prototype script for simplifying use of the genotyping pipeline
# Generate appropriate .yml files for use by Percolate

our $VERSION = '';
our $PERCOLATE_LOG_NAME = 'percolate.log';
our $GENOTYPING_DB_NAME = 'genotyping.db';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'ready_qc_calls');

my $embedded_conf = "
   log4perl.logger.npg.ready_qc_calls = ERROR, A1, A2

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
    my $debug;
    my $dbfile;
    my $run;
    my $egt;
    my $log4perl_config;
    my $verbose;
    my $host;
    my $workflow;
    my $chunk_size;
    my $memory;
    my $zstart;
    my $ztotal;
    my $logconf;
    my @plex_config;
    my @plex_manifests;

    GetOptions('workdir=s'       => \$workdir,
	       'manifest=s'      => \$manifest,
               'plex_config=s'   => \@plex_config,
               'plex_manifest=s' => \@plex_manifests,
	       'host=s'          => \$host,
	       'dbfile=s'        => \$dbfile,
	       'run=s'           => \$run,
	       'egt=s'           => \$egt,
	       'verbose'         => \$verbose,
	       'workflow=s'      => \$workflow,
	       'chunk_size=i'    => \$chunk_size,
	       'memory=i'        => \$memory,
	       'zstart=i'        => \$zstart,
	       'ztotal=i'        => \$ztotal,
               'logconf=s'       => \$log4perl_config,
               'debug=s'         => \$debug,
	       'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
	);

    ### set up logging ###
    if ($log4perl_config) {
        Log::Log4perl::init($log4perl_config);
        $log = Log::Log4perl->get_logger('npg.vcf.qc');
    }
    else {
        Log::Log4perl::init(\$embedded_conf);
        $log = Log::Log4perl->get_logger('npg.vcf.qc');
        if ($verbose) {
            $log->level($INFO);
        }
        elsif ($debug) {
            $log->level($DEBUG);
        }
    }

    ### validate command-line arguments ###
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
    if (scalar @plex_manifests == 0) {
        $log->logcroak("Must supply at least one QC plex manifest");
    }
    foreach my $plex_manifest (@plex_manifests) {
        if (! -e $plex_manifest) {
            $log->logcroak("--plex_manifest argument '", $plex_manifest,
                           "' does not exist");
        }
    }
    if (scalar @plex_config == 0) { # get defaults from perl etc directory
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

    $host ||= 'farm3-head2';
    # illuminus paralellizes by SNP, other callers by sample
    if ($workflow eq 'illuminus') { $chunk_size ||= 4000; }
    else { $chunk_size ||= 40; }
    $memory ||= 2048,
    $zstart ||= 7;
    $ztotal ||= 1;

    ### create and populate the working directory ###
    make_working_directory($workdir, $dbfile);
    write_config_yml($workdir, $host);
    #my $vcf = generate_vcf($workdir, \@plex_manifests, \@plex_config);
    # dummy values for initial test
    my $vcf = ['foo.vcf',];
    #my $plex_manifests = ['bar.tsv',];
    write_workflow_yml($workdir, $workflow, $run, $manifest,
                       $chunk_size, $memory, $vcf, \@plex_manifests,
                       $zstart, $ztotal);
    $log->info("Finished; genotyping pipeline directory '", $workdir,
               "' is ready to run Percolate.");
}

sub generate_vcf {

    # read sample identifiers from pipeline DB
    # query irods and write VCF

}

sub make_working_directory {
    # make in, pass, fail if needed; copy dbfile to working directory
    my ($workdir, $dbfile) = @_;
    if (-e $workdir) {
        if (-d $workdir) {
            $log->info("Directory '", $workdir, "' already exists");
        } else {
            $log->logcroak("--workdir argument '", $workdir,
                           "' exists and is not a directory");
        }
    } else {
        mkdir $workdir || $log->logcroak("Cannot create directory '",
                                         $workdir, "'");
    }
    my $workdb = catfile($workdir, $GENOTYPING_DB_NAME);
    copy($dbfile, $workdb) || $log->logcroak("Cannot copy '", $dbfile,
                                             "' to '", $workdb, "'");
    $log->info("Copied '", $dbfile, "' to '", $workdb, "'");
    foreach my $name (qw/in pass fail/) {
        my $subdir = catfile($workdir, $name);
        if (-e $subdir) {
            if (-d $subdir) {
                $log->info("Subdirectory '", $subdir, "' already exists");
            } else {
                $log->logcroak("Expected subdirectory path '", $subdir,
                               "' exists and is not a directory");
            }
        } else {
            mkdir($subdir) || $log->logcroak("Cannot create subdirectory '",
                                             $subdir, "'");
            $log->info("Created subdirectory '", $subdir, "'");
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
    DumpFile($config_path, (\%config));
    return $config_path;
}

sub write_illuminus {
    my ($workdir, $dbpath, $run, $manifest, $vcf, $plex_manifest,
        $chunk_size, $memory) = @_;

    my %illuminus_args = (
	'chunk_size' => $chunk_size,
	'memory' => $memory,
	'manifest' => $manifest,
        'plex_manifest' => $plex_manifest,
        'vcf' => $vcf,
	'gender_method' => 'Supplied'
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeIlluminus';
    write_final($dbpath, $run, $workdir, $workflow_name, \%illuminus_args,
                catfile($workdir, 'in', 'genotype_illuminus.yml'));
}

sub write_zcall {
    my ($paramsRef, $egt, $zstart, $ztotal) = @_;
    my ($workdir, $dbpath, $run, $manifest, $vcf, $plex_manifest,
        $chunk_size, $memory) = @{$paramsRef};
    my %zcall_args = (
	'chunk_size' => $chunk_size,
	'memory' => $memory,
	'manifest' => $manifest,
        'plex_manifest' => $plex_manifest,
        'vcf' => $vcf,
	'egt' => $egt,
	'zstart' => $zstart,
	'ztotal' => $ztotal
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeZCall';
    write_final($dbpath, $run, $workdir, $workflow_name, \%zcall_args,
                catfile($workdir, 'in', 'genotype_zcall.yml'));
}

sub write_final {
    my ($dbpath, $run, $workdir, $workflow_name, $extra_args_ref, $out) = @_;
    my @workflow_args = ($dbpath, $run, $workdir, $extra_args_ref);
    my %args = (
	'library' => 'genotyping',
	'workflow' => $workflow_name,
	'arguments' => \@workflow_args,
	);
    DumpFile($out, (\%args));
}

sub write_workflow_yml {
    my ($workdir, $workflow, $run, $manifest, $chunk_size,
        $memory, $vcf, $plex_manifests, $egt, $zstart, $ztotal) = @_;

    my $dbpath = catfile($workdir, $GENOTYPING_DB_NAME);

    my @params = ($workdir, $dbpath, $run, $manifest,
                  $vcf, $plex_manifests, $chunk_size, $memory);

    if ($workflow eq 'illuminus') {
        write_illuminus(@params);
    } elsif ($workflow eq 'zcall') {
        if (!$egt) {
            $log->logcroak("Must specify --egt for zcall workflow");
        } elsif (! -e $egt) {
            $log->logcroak("EGT file '", $egt, "' does not exist.");
        } else {
            write_zcall(\@params, $egt, $zstart, $ztotal);
        }
    } else {
        $log->logcroak("Invalid workflow argument '", $workflow,
                       "'; must be one of illuminus, zcall");
    }
}


# TODO modify, and rename, as ready_workflow.pl
# - generate VCF from (default or user supplied) query configs
# - generate YML, including VCF
# - populate analysis directory
#   - copy SQLite DB file
#   - generate YML config and workflow files

# TODO copy_all option to copy egt, manifest, and plex manifest files?


__END__


=head1 NAME

ready_workflow

=head1 SYNOPSIS

ready_workflow [--dbfile <SQLite file path>] [--help]
  --manifest <manifest_path> --run <run_name> [--egt <egt_path>]
  [--memory <memory>] [--host <hostname>]
  [--plex_config <json_path>] [--plex_manifest <tsv_path>]
  [--verbose] --workdir <directory path> --workflow <workflow_name>

Options:

  --chunk_size    Chunk size for parallelization. Optional, defaults to
                  4000 (SNPs) for Illuminus or 40 (samples) for zCall.
  --dbfile        Path to an SQLite database file for analysis. Required.
  --egt           Path to an Illumina .egt cluster file. Required for zcall.
  --help          Display help.
  --host          Name of host machine for the beanstalk message queue.
                  Optional, defaults to farm3-head2.
  --manifest      Path to the .bpm.csv manifest file.
  --memory        Memory limit hint for LSF, in MB. Default = 2048.
  --plex_config   Path to a JSON file with parameters to query iRODS and
                  write QC plex data as VCF. May be supplied more than once
                  to specify multiple files. Optional, defaults to a standard
                  set of config files.
  --plex_manifest Path to a .tsv QC plex manifest. May be supplied more
                  than once to specify multiple files. At least one file is
                  required.
  --run           The pipeline run name in the database. Required.
  --verbose       Print messages while processing. Optional.
  --workdir       Working directory for pipeline run. Required.
  --workflow      Pipeline workflow for which to create a .yml file. If
                  supplied, must be one of: illuminus, genosnp, zcall.
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
