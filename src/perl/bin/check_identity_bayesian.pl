#! /software/bin/perl

use warnings;
use strict;
use Carp;
use Config::IniFiles;
use File::Slurp qw (read_file);
use Getopt::Long;
use JSON;
use Pod::Usage;
use Log::Log4perl qw(:levels);
use WTSI::DNAP::Utilities::ConfigureLogger qw/log_init/;

use WTSI::NPG::Genotyping::QC::BayesianIdentity::Check;
use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::VCF::Slurper;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'check_identity_bed_wip');

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
    my $plex_manifests;
    my $plex_manifests_irods;
    my $plink;
    my $sample_json;
    my $sample_mismatch_prior;
    my $swap_threshold;
    my $vcf;
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
        'plex=s'            => \$plex_manifests,
        'plex_irods=s'      => \$plex_manifests_irods,
        'plink=s'           => \$plink,
        'sample_json=s'     => \$sample_json,
        'swap_threshold=f'  => \$swap_threshold,
        'vcf=s'             => \$vcf,
        'verbose'           => \$verbose,
        'xer=f'             => \$expected_error_rate);

    my @log_levels;
    if ($debug) { push @log_levels, $DEBUG; }
    if ($verbose) { push @log_levels, $INFO; }
    log_init(config => $log4perl_config,
             file   => $session_log,
             levels => \@log_levels);
    my $log = Log::Log4perl->get_logger('main');

    ### set up iRODS connection and make it use same logger as script ###
    my $irods = WTSI::NPG::iRODS->new;
    $irods->logger();

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
        $log->debug("Read sample JSON data for ", scalar @{$sample_data},
                    " samples");
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
    if ($plex_manifests) {
        my @plex_manifests = split(/,/msx, $plex_manifests);
        foreach my $plex (@plex_manifests) {
            unless (-e $plex) {
                $log->logcroak("Plex manifest filesystem path '", $plex,
                               "' does not exist. Paths must be supplied as ",
                               "a comma-separated list; individual paths ",
                               "cannot contain commas.");
            }
            push @snpsets, WTSI::NPG::Genotyping::SNPSet->new($plex);
        }
    }
    if ($plex_manifests_irods) {
        my @plex_manifests_irods = split(/,/msx, $plex_manifests_irods);
        foreach my $plex (@plex_manifests_irods) {
            unless ($irods->is_object($plex)) {
                $log->logcroak("Plex manifest iRODS path '", $plex,
                               "' does not exist. Paths must be supplied as ",
                               "a comma-separated list; individual paths ",
                               "cannot contain commas.");
            }
            my $plex_obj = WTSI::NPG::iRODS::DataObject->new($irods, $plex);
            push @snpsets, WTSI::NPG::Genotyping::SNPSet->new($plex_obj);
        }
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
                snpset             => $snpset);
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
    my $checker =
        WTSI::NPG::Genotyping::QC::BayesianIdentity::Check->new(%args);

    ### read QC plex calls from VCF file(s) ###
    my %qc_calls;
    if (!$vcf) {
        $log->logcroak("At least one VCF path is required. Multiple paths ",
                       "may be supplied as a comma-separated list; ",
                       "individual paths cannot contain commas.");
    }
    my @vcf= split(/,/msx, $vcf);
    foreach my $vcf_path (@vcf) {
        my $vcf_fh;
        if (! -e $vcf_path) {
            $log->logcroak("File argument to --vcf does not exist: '",
                           $vcf_path, "'");
        } else {
            open $vcf_fh, "<", $vcf_path ||
                $log->logcroak("Cannot open VCF input '", $vcf_path, "'");
        }
        my %slurp_args = (
            input_filehandle => $vcf_fh,
            snpset           => $snpset,
        );
        my $vcf_data = WTSI::NPG::Genotyping::VCF::Slurper->new(
            %slurp_args)->read_dataset();
        close $vcf_fh || $log->logcroak("Cannot close VCF input '",
                                        $vcf_path, "'");
        my %vcf_calls = %{$vcf_data->calls_by_sample()};
        foreach my $ssid (keys %vcf_calls) {
            my $uri = $ssid_to_uri{$ssid};
            if ($uri) {
                push @{$qc_calls{$uri}}, @{$vcf_calls{$ssid}};
                $log->debug("Appending calls for URI '", $uri, "', SSID '",
                            $ssid, "'");
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

check_identity_bayesian

=head1 SYNOPSIS

check_identity_bayesian --vcf <VCF file> --plink <path stem>
--sample_json <path > [--plex <path>] [--plex-irods <irods location>]
[--pass-threshold <f>] [--swap_threshold <f>] [--help] [--verbose]

Options:

  --csv=PATH             Path for CSV output. Required.
  --help                 Display help.
  --logconf=PATH         Path to Perl logger configuration file. Optional.
  --json=PATH            Path for JSON output. Required. May be '-' for
                         STDOUT.
  --pass_threshold=NUM   Minimum similarity to pass identity check. Optional.
  --plex=PATH            Path to one or more .tsv manifests for QC plex
                         SNP sets. Multiple plex manifests are given as a
                         comma-separated list; the paths themselves may not
                         contain commas.
  --plex_irods=PATH      Location of one or more iRODS data objects
                         corresponding to .tsv manifest for QC plex SNP
                         sets. Can give multiple arguments, similarly to
                         --plex. At least one manifest must be supplied
                         using the --plex and/or --plex_irods arguments.
  --plink=STEM           Plink binary stem (path omitting the .bed, .bim, .fam
                         suffix) for production data.
  --sample_json=PATH     JSON file for translating between Sanger sample ID
                         and sample URI. Required.
  --swap_threshold=NUM   Minimum cross-similarity to warn of sample swap.
                         Optional.
  --vcf=PATH             Path to one or more VCF input files. Can give
                         multiple paths as a comma-separated list,
                         similarly to --plex. Must supply at least one VCF
                         path.
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

Copyright (c) 2015, 2016, 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
