#! /software/bin/perl

use warnings;
use strict;
use Carp;
use Config::IniFiles;
use File::Slurp qw (read_file);
use Getopt::Long;
use JSON;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess;
use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::VCF::Slurper;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';

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

    my $debug;
    my $log4perl_config;
    my $min_shared_snps;
    my $outPath;
    my $pass_threshold;
    my $plex_manifest;
    my $plink;
    my $snpset_irods_json;
    my $swap_threshold;
    my $vcf;
    my $verbose;

    GetOptions(
        'debug'             => \$debug,
        'help'              => sub { pod2usage(-verbose => 2,
                                               -exitval => 0) },
        'logconf=s'         => \$log4perl_config,
        'min_shared_snps=i' => \$min_shared_snps,
        'out=s'             => \$outPath,
        'pass_threshold=f'  => \$pass_threshold,
        'plex_manifest=s'   => \$plex_manifest,
        'plink=s'           => \$plink,
        'snpset_paths=s'    => \$snpset_irods_json,
        'swap_threshold=f'  => \$swap_threshold,
        'vcf=s'             => \$vcf,
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

    ### read SNPSet object from manifest, or snpset_paths from JSON ###
    my ($snpset, $snpset_paths);
    if (defined($plex_manifest)) {
        $snpset = WTSI::NPG::Genotyping::SNPSet->new($plex_manifest);
    } elsif (defined($snpset_irods_json)) {
        $snpset_paths = decode_json(read_file($snpset_irods_json));
    } else {
        $log->logcroak("Must supply either a --plex-manifest or a ",
                       "--snpset-paths argument");
    }

    ### check existence of plink dataset ###
    if (!defined($plink)) {
        $log->logcroak("Must supply a --plink argument");
    }
    my @plink_suffixes = qw(.bed .bim .fam);
    foreach my $suffix (@plink_suffixes) {
        my $plink_path = $plink.$suffix; # needed to mollify perlcritic
        if (!(-e $plink_path)) {
            $log->logcroak("Plink binary input file '", $plink.$suffix,
                           "' does not exist");
        }
    }

    ### create identity check object ###
    my %args = (plink_path         => $plink,
                snpset             => $snpset,
                logger             => $log);
    if (defined($min_shared_snps)) {
        $args{'min_shared_snps'} = $min_shared_snps;
    }
    if (defined($swap_threshold)) {$args{'swap_threshold'} = $swap_threshold;}
    if (defined($pass_threshold)) {$args{'pass_threshold'} = $pass_threshold;}
    # Identity.pm has defaults for min_shared_snps, and swap/pass thresholds
    $log->debug("Creating identity check object");
    my $checker = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new(%args);

    ### read QC plex calls from VCF ###
    my $vcf_fh;
    if (!defined($vcf)) {
        $log->logcroak("A --vcf argument is required");
    } elsif ($vcf eq '-') {
        $vcf_fh = \*STDIN;
    } elsif (! -e $vcf) {
        $log->logcroak("File argument to --vcf does not exist");
    } else {
        open $vcf_fh, "<", $vcf || $log->logcroak("Cannot open VCF input '",
                                                  $vcf, "'");
    }
    my %slurp_args = (input_filehandle => $vcf_fh);
    if (defined($snpset)) {
        $slurp_args{'snpset'} = $snpset;
    } elsif (defined($snpset_paths)) {
        $slurp_args{'snpset_irods_paths'} = $snpset_paths;
    } else {
        $log->logcroak("Missing snpset object or paths");
    }
    my $vcf_data = WTSI::NPG::Genotyping::VCF::Slurper->new(
        %slurp_args)->read_dataset();
    if ($vcf_fh ne '-') {
        close $vcf_fh || $log->logcroak("Cannot close VCF input '",
                                        $vcf, "'");
    }
    my $qc_calls = $vcf_data->calls_by_sample();

    ### run identity check and write output ###
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



__END__

=head1 NAME

check_identity_bed_wip

=head1 SYNOPSIS

check_identity_bed_wip [--config <database .ini file>] --vcf <VCF file> [-- min-shared-snps <n>] [--pass-threshold <f>] --plex-manifest <path> [--out <path>] --plink <path stem> [--swap_threshold <f>] [--help] [--verbose]

Options:

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
  --vcf=PATH             Path to VCF input file, or - to read from STDIN.
                         Required.
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
