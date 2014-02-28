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

run() unless caller();

sub run {

    my $workdir;
    my $manifest;
    my $dbfile;
    my $run;
    my $egt;
    my $help;
    my $verbose;
    my $host;
    my $workflow = "";

    my $log = Log::Log4perl->get_logger();

    GetOptions('workdir=s' => \$workdir,
	       'manifest=s' => \$manifest,
	       'host=s' => \$host,
	       'help' => sub { pod2usage(-verbose => 2, -exitval => 0) },
	       'dbfile=s' => \$dbfile,
	       'run=s' => \$run,
	       'egt=s' => \$egt,
	       'verbose' => \$verbose,
	       'workflow=s' => \$workflow,
	);

    if (!($workdir && $run)) {
	$log->logcroak("Must specify pipeline run name and working directory");
    }
    if ($workdir !~ '/$') { $workdir .= '/'; } # ensure $workdir ends with /
    if ($verbose) { print "WORKDIR: $workdir\n"; }

    $dbfile ||= 'genotyping.db';
    my $dbpath = $workdir.$dbfile;
    $host ||= 'farm3-head2'; 

    my %config = (
	'root_dir' => $workdir,
	'log' => $workdir."percolate.log",
	'log_level' => 'DEBUG',
	'msg_host' => $host,
	'msg_port' => '11300',
	'async' => 'lsf',
	'max_processes' => '250'
	);
    DumpFile('config.yml', (\%config));

    if ($workflow) {
	if (! $manifest) {
	    $log->logcroak("Must specify --manifest for workflow!");
	} elsif ($workflow eq 'illuminus') { 
	    write_illuminus($dbpath, $run, $workdir, $manifest); 
	} elsif ($workflow eq 'genosnp') {
	    write_genosnp($dbpath, $run, $workdir, $manifest);  
	} elsif ($workflow eq 'zcall') {
	    if (!$egt) {
		$log->logcroak("Must specify --egt for zcall workflow");
	    } else {
		write_zcall($dbpath, $run, $workdir, $manifest, $egt);  
	    }
	} else {
	    $log->logcroak("Invalid workflow argument $workflow; must be one of illuminus, genosnp, zcall");
	}
    }
}

sub write_genosnp {
    my ($dbpath, $run, $workdir, $manifest) = @_;
    my %genosnp_args = (
	'chunk_size' => '40',
	'memory' => '2048',
	'manifest' => $manifest,
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeGenoSNP';
    write_workflow($dbpath, $run, $workdir, $workflow_name, \%genosnp_args,
		   'genotype_genosnp.yml');
}

sub write_illuminus {
    my ($dbpath, $run, $workdir, $manifest) = @_;
   
    my %illuminus_args = (
	'chunk_size' => '4000',
	'memory' => '2048',
	'manifest' => $manifest,
	'gender_method' => 'Supplied'
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeIlluminus';
    write_workflow($dbpath, $run, $workdir, $workflow_name, \%illuminus_args,
		   'genotype_illuminus.yml');
}

sub write_zcall {
    my ($dbpath, $run, $workdir, $manifest, $egt) = @_;
    my %zcall_args = (
	'chunk_size' => '40',
	'memory' => '2048',
	'manifest' => $manifest,
	'zstart' => '7',
	'ztotal' => '1',
	'egt' => $egt
	);
    my $workflow_name = 'Genotyping::Workflows::GenotypeZCall';
    write_workflow($dbpath, $run, $workdir, $workflow_name, \%zcall_args,
		   'genotype_zcall.yml');

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


}

__END__

=head1 NAME

generate_yml

=head1 SYNOPSIS

generate_yml [--dbfile <SQLite filename>] [--help] 
  --manifest <manifest_path> --run <run_name> [--egt <egt_path>]
  [--verbose] --workdir <directory path> --workflow <workflow_name>

Options:

  --dbfile      The SQLite database filename (not the full path). Optional,
                defaults to genotyping.db.
  --help        Display help.
  --manifest    Path to the .bpm.csv manifest file.
  --run         The pipeline run name in the database. Required.
  --egt         Path to an Illumina .egt cluster file. Required for zcall. 
  --verbose     Print messages while processing. Optional.
  --workdir     Working directory for pipeline run. Required.
  --workflow    Pipeline workflow for which to create a .yml file. If 
                supplied, must be one of: illuminus, genosnp, zcall.
                If absent, only config.yml will be generated.
  --host        Name of host machine for the beanstalk message queue.
                Optional, defaults to farm3-head2.
    

=head1 DESCRIPTION

Generates .yml files to run the genotyping pipeline. Output is the generic
config.yml file, and optionally a workflow file for one of the available
genotype callers. The workflow file can then be placed in the Percolate 'in' 
directory while the config file is supplied as an argument to the Percolate 
executable.

The script assumes that a copy of the named genotyping database file is 
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
