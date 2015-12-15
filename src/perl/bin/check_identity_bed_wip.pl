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

use WTSI::NPG::Genotyping::QC_wip::Check::Identity;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::VCF::Slurper;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
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

    my $csv_path;
    my $debug;
    my $ecp_json;
    my $ecp_default;
    my $expected_error_rate;
    my $log4perl_config;
    my $json_path;
    my $pass_threshold;
    my @plex_manifests;
    my @plex_manifests_irods;
    my $plink;
    my $sample_json;
    my $sample_mismatch_prior;
    my $swap_threshold;
    my @vcf; # array for (maybe) multiple VCF inputs
    my $verbose;

    GetOptions(
        'csv=s'             => \$csv_path,
        'debug'             => \$debug,
        'ecp_json=s'        => \$ecp_json,
        'ecp_default=f'     => \$ecp_default,
        'help'              => sub { pod2usage(-verbose => 2,
                                               -exitval => 0) },
        'logconf=s'         => \$log4perl_config,
        'json=s'            => \$json_path,
        'prior=f'           => \$sample_mismatch_prior,
        'pass_threshold=f'  => \$pass_threshold,
        'plex=s'            => \@plex_manifests,
        'plex_irods=s'      => \@plex_manifests_irods,
        'plink=s'           => \$plink,
        'sample_json=s'     => \$sample_json,
        'swap_threshold=f'  => \$swap_threshold,
        'vcf=s'             => \@vcf,
        'verbose'           => \$verbose,
        'xer=f'             => \$expected_error_rate);

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

    ### set up iRODS connection and make it use same logger as script ###
    my $irods = WTSI::NPG::iRODS->new;
    $irods->logger($log);

    ### read equivalent calls probability by SNP from JSON, if given
    my $ecp;
    if (defined($ecp_json)) {
        if (defined($ecp_default)) {
            $log->logcroak("Cannot supply both JSON path and default for ",
                            "equivalent call probabilities by SNP")
        }
        $ecp = decode_json(read_file($ecp_json));
    }

    ### read sample JSON file (required) to map SSID to URI
    my %ssid_to_uri;
    if (defined($sample_json)) {
        unless (-e $sample_json) {
            $log->logcroak("Cannot read sample JSON path '$sample_json'");
        }
        my $sample_data = decode_json(read_file($sample_json));
        # generate a hash mapping sanger sample ID to URI
        foreach my $sample (@{$sample_data}) {
            my $ssid = $sample->{'sanger_sample_id'};
            if ($ssid_to_uri{$ssid}) {
                $log->logwarn("Multiple URIs for Sanger sample ID '",
                              $ssid, "', omitting '", $sample->{'uri'}, "'");
            }
            $ssid_to_uri{$ssid} = $sample->{'uri'};
        }

    } else {
        $log->logcroak("--sample_json argument is required");
    }

    ### read SNPSet object(s) from file and/or iRODS, create union set ###
    my @snpsets;
    foreach my $plex (@plex_manifests) {
        push @snpsets, WTSI::NPG::Genotyping::SNPSet->new($plex);
    }
    foreach my $plex (@plex_manifests_irods) {
        my $plex_obj = WTSI::NPG::iRODS::DataObject->new($irods, $plex);
        push @snpsets, WTSI::NPG::Genotyping::SNPSet->new($plex_obj);
    }
    if (scalar @snpsets == 0) {
        $log->logcroak("Must supply at least one plex manifest using ",
                       "--plex or --plex_irods options");
    }
    my $first_snpset = shift @snpsets;
    my $snpset = $first_snpset->union(\@snpsets);

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

    ### check output path arguments ###
    if (!defined($json_path)) {
        $log->logcroak("--json argument is required");
    }
    elsif (!defined($csv_path)) {
        $log->logcroak("--csv argument is required");
    }

    ### create identity check object ###
    my %args = (plink_path         => $plink,
                snpset             => $snpset,
                logger             => $log);
    if (defined($swap_threshold)) {$args{'swap_threshold'} = $swap_threshold;}
    if (defined($pass_threshold)) {$args{'pass_threshold'} = $pass_threshold;}
    if (defined($ecp)) { $args{'equivalent_calls_probability'} = $ecp;  }
    if (defined($ecp_default)) { $args{'ecp_default'} = $ecp_default;  }
    if (defined($sample_mismatch_prior)) {
        $args{'sample_mismatch_prior'} = $sample_mismatch_prior;
    }
    if (defined($expected_error_rate)) {
        $args{'expected_error_rate'} = $expected_error_rate;
    }
    $log->debug("Creating identity check object");
    my $checker = WTSI::NPG::Genotyping::QC_wip::Check::Identity->new(%args);

    ### read QC plex calls from VCF file(s) ###
    my %qc_calls;
    if (!(@vcf)) {
        $log->logcroak("At least one --vcf argument is required");
    }
    foreach my $vcf (@vcf) {
        my $vcf_fh;
        if (! -e $vcf) {
            $log->logcroak("File argument to --vcf does not exist: '",
                           $vcf, "'");
        } else {
            open $vcf_fh, "<", $vcf ||
                $log->logcroak("Cannot open VCF input '", $vcf, "'");
        }
        my %slurp_args = (
            input_filehandle => $vcf_fh,
            snpset           => $snpset,
        );
        my $vcf_data = WTSI::NPG::Genotyping::VCF::Slurper->new(
            %slurp_args)->read_dataset();
        close $vcf_fh || $log->logcroak("Cannot close VCF input '",
                                        $vcf, "'");
        my %vcf_calls = %{$vcf_data->calls_by_sample()};
        foreach my $ssid (keys %vcf_calls) {
            my $uri = $ssid_to_uri{$ssid};
            if ($uri) {
                push @{$qc_calls{$uri}}, @{$vcf_calls{$ssid}};
            } else {
                # samples in QC plex results do not necessarily appear in
                # production, eg. sample exclusion in Illuminus/zCall
                $log->info("No URI for Sanger sample ID ", $ssid);
            }

        }
    }

    ### run identity check and write output ###
    $checker->write_identity_results(\%qc_calls, $json_path, $csv_path);
}



__END__

=head1 NAME

check_identity_bed_wip

=head1 SYNOPSIS

check_identity_bed_wip --vcf <VCF file> --plink <path stem>
--sample_json <path > [--plex <path>] [--plex-irods <irods location>]
[--pass-threshold <f>] [--swap_threshold <f>] [--help] [--verbose]

Options:

  --csv=PATH             Path for CSV output. Required.
  --help                 Display help.
  --logconf=PATH         Path to Perl logger configuration file. Optional.
  --json=PATH            Path for JSON output. Required. May be '-' for
                         STDOUT.
  --pass_threshold=NUM   Minimum similarity to pass identity check. Optional.
  --plex=PATH            Path to .tsv manifest for a QC plex SNP set. Can
                         give multiple arguments for multiple plex files, eg.
                         '--plex file1.tsv --plex file2.tsv'. At least one
                         manifest must be supplied using the --plex and/or
                         --plex_irods arguments.
  --plex_irods=PATH      Location of iRODS data object corresponding to .tsv
                         manifest for a QC plex SNP set. Can give multiple
                         arguments, similarly to --plex. At least one
                         manifest must be supplied using the --plex and/or
                         --plex_irods arguments.
  --plink=STEM           Plink binary stem (path omitting the .bed, .bim, .fam
                         suffix) for production data.
  --sample_json=PATH     JSON file for translating between Sanger sample ID
                         and sample URI. Required.
  --swap_threshold=NUM   Minimum cross-similarity to warn of sample swap.
                         Optional.
  --vcf=PATH             Path to VCF input file. Can give multiple arguments
                         for multiple VCF inputs, similarly to --plex.
  --verbose              Turn on verbose logging. Optional.

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
