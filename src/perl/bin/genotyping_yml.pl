#!/software/bin/perl

use utf8;

package main;

use warnings;
use strict;
use Getopt::Long;
use Log::Log4perl qw(:easy);
use Pod::Usage;
use YAML qw /DumpFile/;

# Prototype script for simplifying use of the genotyping pipeline
# Generate appropriate .yml files for use by Percolate

Log::Log4perl->easy_init($ERROR);

our $PERCOLATE_LOG_NAME = 'percolate.log';

run() unless caller();

sub run {

    my ($outdir, $workdir, $manifest, $dbfile, $run, $egt, $help, $verbose, 
	$host, $workflow, $chunk_size, $memory, $zstart, $ztotal);

    my $log = Log::Log4perl->get_logger();

    GetOptions('outdir=s'      => \$outdir,
               'workdir=s'     => \$workdir,
	       'manifest=s'    => \$manifest,
	       'host=s'        => \$host,
	       'dbfile=s'      => \$dbfile,
	       'run=s'         => \$run,
	       'egt=s'         => \$egt,
	       'verbose'       => \$verbose,
	       'workflow=s'    => \$workflow,
	       'chunk_size=i'  => \$chunk_size,
	       'memory=i'      => \$memory,
	       'zstart=i'      => \$zstart,
	       'ztotal=i'      => \$ztotal,
	       'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
	);
    $outdir ||= '.';
    if (!($workdir && $run)) {
	$log->logcroak("Must specify pipeline run name and working directory");
    } elsif (!(-e $outdir && -d $outdir)) {
	$log->logcroak("Output path '$outdir' does not exist or is not a directory");
    } elsif (-e $workdir && !(-d $workdir)) {
	$log->logcroak("Working directory path '$workdir' already exists, and is not a directory");
    } elsif (!(-e $workdir)) {
	$log->logwarn("Warning: Pipeline working directory '$workdir' does not exist; must be created before running workflow.");
    }
    if ($workdir !~ '/$') { $workdir .= '/'; } # ensure $workdir ends with /
    if ($outdir !~ '/$') { $outdir .= '/'; } # similarly for $outdir
    if ($verbose) { print "WORKDIR: $workdir\n"; }

    $dbfile ||= 'genotyping.db';
    my $dbpath = $workdir.$dbfile;
    if (! -e $dbpath) {
	$log->logwarn("Warning: Pipeline database '$dbpath' does not exist; must be created before running workflow.");
    }
    $host ||= 'farm3-head2'; 
    # illuminus paralellizes by SNP, other callers by sample
    if ($workflow && $workflow eq 'illuminus') { $chunk_size ||= 4000; }
    else { $chunk_size ||= 40; } 
    $memory ||= 2048,
    $zstart ||= 7;
    $ztotal ||= 1;

    my %config = (
	'root_dir' => $workdir,
	'log' => $workdir.$PERCOLATE_LOG_NAME,
	'log_level' => 'DEBUG',
	'msg_host' => $host,
	'msg_port' => '11300',
	'async' => 'lsf',
	'max_processes' => '250'
	);
    DumpFile($outdir.'config.yml', (\%config));

    if ($workflow) {
	my @params = ($outdir, $dbpath, $run, $workdir, $manifest, $chunk_size, $memory);
	if (! $manifest) { 
	    $log->logcroak("Must specify --manifest for workflow!");
	} elsif (! -e $manifest) {
	    $log->logwarn("Warning: Manifest '$manifest' does not exist, must be created before running workflow.");
	} elsif ($workflow eq 'illuminus') { 
	    write_illuminus(@params); 
	} elsif ($workflow eq 'genosnp') {
	    write_genosnp(@params);  
	} elsif ($workflow eq 'zcall') {
	    if (!$egt) {
		$log->logcroak("Must specify --egt for zcall workflow");
	    } elsif (! -e $egt) {
		 $log->logwarn("Warning: EGT file '$egt' does not exist, must be created before running workflow.");
	    } else {
		write_zcall(\@params, $egt, $zstart, $ztotal);  
	    }
	} else {
	    $log->logcroak("Invalid workflow argument $workflow; must be one of illuminus, genosnp, zcall");
	}
    }
}

sub write_genosnp {
    my ($outdir, $dbpath, $run, $workdir, $manifest, $chunk_size, $memory) = @_;
    my %genosnp_args = (
	'chunk_size' => $chunk_size,
	'memory' => $memory,
	'manifest' => $manifest,
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeGenoSNP';
    write_workflow($dbpath, $run, $workdir, $workflow_name, \%genosnp_args,
		   $outdir.'genotype_genosnp.yml');
}

sub write_illuminus {
    my ($outdir, $dbpath, $run, $workdir, $manifest, $chunk_size, $memory) = @_;
   
    my %illuminus_args = (
	'chunk_size' => $chunk_size,
	'memory' => $memory,
	'manifest' => $manifest,
	'gender_method' => 'Supplied'
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeIlluminus';
    write_workflow($dbpath, $run, $workdir, $workflow_name, \%illuminus_args,
		   $outdir.'genotype_illuminus.yml');
}

sub write_zcall {
    my ($paramsRef, $egt, $zstart, $ztotal) = @_;
    my ($outdir, $dbpath, $run, $workdir, $manifest, $chunk_size, $memory) = @{$paramsRef};
    my %zcall_args = (
	'chunk_size' => $chunk_size,
	'memory' => $memory,
	'manifest' => $manifest,
	'egt' => $egt,
	'zstart' => $zstart,
	'ztotal' => $ztotal
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeZCall';
    write_workflow($dbpath, $run, $workdir, $workflow_name, \%zcall_args,
		   $outdir.'genotype_zcall.yml');
}

sub write_workflow {
    my ($dbpath, $run, $workdir, $workflow_name, $extra_args_ref, $out) = @_;
    my @workflow_args = ($dbpath, $run, $workdir, $extra_args_ref);
    my %args = (
	'library' => 'genotyping',
	'workflow' => $workflow_name,
	'arguments' => \@workflow_args,
	);
    DumpFile($out, (\%args));
}


__END__


=head1 NAME

genotyping_yml

=head1 SYNOPSIS

genotyping_yml [--dbfile <SQLite filename>] [--help] 
  --manifest <manifest_path> --run <run_name> [--egt <egt_path>]
  [--verbose] --workdir <directory path> --workflow <workflow_name>

Options:

  --chunk_size  Chunk size for parallelization. Optional, defaults to 
                4000 for Illuminus or 40 for zCall/GenoSNP.
  --dbfile      The SQLite database filename (not the full path). Optional,
                defaults to genotyping.db.
  --egt         Path to an Illumina .egt cluster file. Required for zcall. 
  --help        Display help.
  --host        Name of host machine for the beanstalk message queue.
                Optional, defaults to farm3-head2.
  --manifest    Path to the .bpm.csv manifest file.
  --memory      Memory limit hint for LSF, in MB. Default = 2048.
  --outdir      Directory in which to write YML files. Optional, defaults 
                to current working directory.
  --run         The pipeline run name in the database. Required.
  --verbose     Print messages while processing. Optional.
  --workdir     Working directory for pipeline run. Required.
  --workflow    Pipeline workflow for which to create a .yml file. If 
                supplied, must be one of: illuminus, genosnp, zcall.
                If absent, only config.yml will be generated.
  --zstart      Start of zscore range, used for zCall only. Default = 7.
  --ztotal      Number of zscores in range, for zCall only. Default = 1.

=head1 DESCRIPTION

Generates .yml files to run the genotyping pipeline. Output is the generic
config.yml file, and optionally a workflow file for one of the available
genotype callers. The workflow file can then be placed in the Percolate 'in' 
directory while the config file is supplied as an argument to the Percolate 
executable.

The script assumes that the named genotyping database file will be present 
in the given working directory.

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
