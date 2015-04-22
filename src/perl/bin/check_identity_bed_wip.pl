#! /software/bin/perl

use warnings;
use strict;
use Carp;
use Config::IniFiles;
use Getopt::Long;
use JSON;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::Database::Pipeline;
use WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Utilities qw(user_session_log);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'check_identity_bed_wip');

my $embedded_conf = "
   log4perl.logger.npg.genotyping.qc.identity = ERROR, A1, A2

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

    my $config;
    my $debug;
    my $dbfile;
    my $log4perl_config;
    my $min_shared_snps;
    my $outPath;
    my $pass_threshold;
    my $plex_manifest;
    my $plink;
    my $swap_threshold;
    my $verbose;

    GetOptions(
        'config=s'          => \$config,
        'dbfile=s'          => \$dbfile,
        'debug'             => \$debug,
        'help'              => sub { pod2usage(-verbose => 2,
                                               -exitval => 0) },
        'logconf=s'         => \$log4perl_config,
        'min_shared_snps=i' => \$min_shared_snps,
        'out=s'             => \$outPath,
        'pass_threshold=f'  => \$pass_threshold,
        'plex_manifest=s'   => \$plex_manifest,
        'plink=s'           => \$plink,
        'swap_threshold=f'  => \$swap_threshold,
        'verbose'           => \$verbose);

    if ($log4perl_config) {
        Log::Log4perl::init($log4perl_config);
        $log = Log::Log4perl->get_logger('npg.genotyping.qc.identity');
    }
    else {
        Log::Log4perl::init(\$embedded_conf);
        $log = Log::Log4perl->get_logger('npg.genotyping.qc.identity');
        if ($verbose) {
            $log->level($INFO);
        }
        elsif ($debug) {
            $log->level($DEBUG);
        }
    }

    my @file_args = ($dbfile, $plex_manifest);
    my @file_arg_names = qw/dbfile plex_manifest/;
    for (my $i=0; $i<@file_args; $i++) {
        if (!defined($file_args[$i])) {
            $log->logcroak("Must supply a --", $file_arg_names[$i],
                           " argument");
        } elsif (! -e $file_args[$i]) {
            $log->logcroak("Given --", $file_arg_names[$i], " argument '",
                           $file_args[$i], "' does not exist");
        }
    }

    if (!defined($plink)) {
        $log->logcroak("Must supply a --plink argument");
    }
    my @plink_files = ($plink.'.bed', $plink.'.bim', $plink.'.fam');
    foreach my $plink_file (@plink_files) {
        if (! -e $plink_file) {
            $log->logcroak("Plink binary input file '", $plink_file,
                           "' does not exist");
        }
    }
    $config ||= $DEFAULT_INI;
    if (! -e $config) {
        $log->logcroak("Config .ini file '", $config, "' does not exist");
    }

    my $snpset = WTSI::NPG::Genotyping::SNPSet->new($plex_manifest);
    $log->debug("Read QC plex snpset from ", $plex_manifest);

    my $qc_calls = read_qc_calls($dbfile, $config, $snpset);

    # $min_shared_snps, $swap_threshold, $pass_threshold may be null
    # if so, Identity.pm uses internal defaults
    my %args = (plink_path         => $plink,
                snpset             => $snpset,
                logger             => $log);
    if (defined($min_shared_snps)) {
        $args{'min_shared_snps'} = $min_shared_snps;
    }
    if (defined($swap_threshold)) {$args{'swap_threshold'} = $swap_threshold;}
    if (defined($pass_threshold)) {$args{'pass_threshold'} = $pass_threshold;}

    $log->debug("Creating identity check object");
    my $checker = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new(%args);

    my $result = $checker->run_identity_checks_json_spec($qc_calls);

    my $out;
    if (defined($outPath)) {
        open $out, ">", $outPath || $log->logcroak("Cannot open output ",
                                                   "path '", $outPath, "'");
    } else {
        $out = \*STDOUT;
    }
    print $out encode_json($result);
    if (defined($outPath)) {
        close $out || $log->logcroak("Cannot open output path '",
                                     $outPath, "'");
    }

}

# For now, read QC calls from the pipeline SQLITE database
# TODO options to read calls directly from Sequenom/Fluidigm subscribers
# cf. 'insert calls' methods in ready_infinium.pl

sub read_qc_calls {
    # required output: $sample => [ [$snp_name_1, $call_1], ... ]
    my ($dbfile, $inifile, $snpset) = @_;
    my $pipedb = WTSI::NPG::Genotyping::Database::Pipeline->new
	(name => 'pipeline',
	 inifile => $inifile,
	 dbfile => $dbfile);
    $pipedb->connect(RaiseError => 1,
                 on_connect_do => 'PRAGMA foreign_keys = ON');
    my @samples = $pipedb->sample->all;
    $log->debug("Read ", scalar(@samples), " samples from pipeline DB");
    my @snps = $pipedb->snp->all;
    my $snpTotal = @snps;
    $log->debug("Read $snpTotal SNPs from pipeline DB");
    my %snpNames;
    foreach my $snp (@snps) { $snpNames{$snp->id_snp} = $snp->name; }
    # read QC calls for each sample and SNP
    my $snpResultTotal = 0;
    my %results;
    my $i = 0;
    foreach my $sample (@samples) {
        if ($sample->include == 0) { next; }
        my $sampleURI = $sample->uri;
        my @results = $sample->results->all;
        $i++;
        if ($i % 100 == 0) {
            $log->debug("Read ", scalar(@results),
                        " results for sample ", $i, " of ",
                        scalar(@samples));
        }
        my @snp_calls;
        foreach my $result (@results) {
            my @snpResults = $result->snp_results->all;
            $snpResultTotal += @snpResults;
            foreach my $snpResult (@snpResults) {
                my $snpName = $snpNames{$snpResult->id_snp};
                my $call = WTSI::NPG::Genotyping::Call->new
                    (snp      => $snpset->named_snp($snpName),
                     genotype => $snpResult->value);
                push(@snp_calls, $call);
            }
        }
        $results{$sampleURI} = \@snp_calls;
    }
    $log->debug("Read ", $snpResultTotal, " QC SNP results from pipeline DB");
    return \%results;
}



__END__

=head1 NAME

check_identity_bed_wip

=head1 SYNOPSIS

check_identity_bed_wip [--config <database .ini file>] --dbfile <SQLite file> [-- min-shared-snps <n>] [--pass-threshold <f>] --plex-manifest <path> [--out <path>] --plink <path stem> [--swap_threshold <f>] [--help] [--verbose]

Options:

  --config=PATH          Load database configuration from a user-defined .ini
                         file. Optional, defaults to $HOME/.npg/genotyping.ini
  --dbfile=PATH          Path to pipeline SQLITE database file. Required.
  --help                 Display help.
  --logconf=PATH         Path to Perl logger configuration file. Optional.
  --min_shared_snps=NUM  Minimum number of shared SNPs between production and
                         QC plex to carry out identity check. Optional.
  --out=PATH             Path for JSON output. Optional, defaults to STDOUT.
  --pass_threshold=NUM   Minimum similarity to pass identity check. Optional.
  --plex_manifest=PATH   Path to .csv manifest for the QC plex SNP set.
  --plink=STEM           Plink binary stem (path omitting the .bed, .bim, .fam
                         suffix) for production data.
  --swap_threshold=NUM   Minimum cross-similarity to warn of sample swap.
                         Optional.
  --verbose              Print messages while processing. Optional.

=head1 DESCRIPTION

Check identity of genotyped data against a QC plex run on an alternate
platform, such as Sequenom or Fluidigm. The QC plex has a panel of SNPs
common to the production data and the QC platform.

=head1 METHODS

None

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
