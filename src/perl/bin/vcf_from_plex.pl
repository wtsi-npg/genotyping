#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::VCF::VCFConverter;
use WTSI::NPG::Genotyping::VCF::VCFGtcheck;
use WTSI::NPG::Utilities qw(user_session_log);

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'vcf_from_plex');

my $embedded_conf = "
   log4perl.logger.npg.vcf = ERROR, A1, A2

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

my ($input, $inputType, $plexColl, $vcfPath, $gtCheck, $jsonOut, $textOut,
    $log, $logConfig, $verbose, $use_irods, $debug, $manifest,
    $chromosome_json);

my $SEQUENOM_TYPE = 'sequenom'; # TODO avoid repeating these across modules
my $FLUIDIGM_TYPE = 'fluidigm';

GetOptions('chromosomes=s'     => \$chromosome_json,
           'debug'             => \$debug,
           'help'              => sub { pod2usage(-verbose => 2,
                                                -exitval => 0) },
           'input=s'           => \$input,
           'irods'             => \$use_irods,
           'json=s'            => \$jsonOut,
           'logconf=s'         => \$logConfig,
           'manifest=s'        => \$manifest,
           'plex_coll=s'       => \$plexColl,
           'plex_type=s'       => \$inputType,
           'text=s'            => \$textOut,
           'vcf=s'             => \$vcfPath,
           'gtcheck'           => \$gtCheck,
           'verbose'           => \$verbose,
       );

### set up logging ###
if ($logConfig) {
    Log::Log4perl::init($logConfig);
    $log = Log::Log4perl->get_logger('npg.vcf.plex');
} else {
    Log::Log4perl::init(\$embedded_conf);
    $log = Log::Log4perl->get_logger('npg.vcf.plex');
}
if ($verbose) {
    $log->level($INFO);
}
elsif ($debug) {
    $log->level($DEBUG);
}

### process command-line options and make sanity checks ###
if ($inputType ne $SEQUENOM_TYPE && $inputType ne $FLUIDIGM_TYPE) {
    $log->logcroak(
        "Must specify $SEQUENOM_TYPE or $FLUIDIGM_TYPE as plex type");
}
my $plexCollOpt;
if ($use_irods) {
    if ($inputType eq $SEQUENOM_TYPE) {
        $plexCollOpt = 'sequenom_plex_coll';
        $plexColl ||= '/seq/sequenom/multiplexes';
    } elsif ($inputType eq $FLUIDIGM_TYPE) {
        $plexCollOpt = 'fluidigm_plex_coll';
        $plexColl ||= '/seq/fluidigm/multiplexes';
    }
} else {
    if ($plexColl) {
        my $msg = "plex_coll option does not apply to non-iRODS input ".
            "and will be ignored";
        $log->warn($msg);
    }
    if (!$manifest) {
        $log->logcroak("--manifest is required for non-iRODS input");
    } elsif (!(-e $manifest)) {
        $log->logcroak("Manifest path '$manifest' does not exist");
    } elsif (!$chromosome_json) {
        $log->logcroak("--chromosomes is required for non-iRODS input");
    } elsif (!(-e $chromosome_json)) {
        $log->logcroak("Chromosome path '$chromosome_json' does not exist");
    }
}

### read inputs ###
my $in;
if (!($input) || $input eq '-') {
    $log->debug("Input from STDIN");
    $input = '-';
    $in = *STDIN;
} else {
    $log->debug("Opening input path $input");
    open $in, "<", $input || $log->logcroak("Cannot open input '$input'");
}
my @inputs = ();
while (<$in>) {
    chomp;
    push(@inputs, $_);
}
$log->debug(scalar(@inputs)." input paths read");
if ($input ne '-') {
    close $in || $log->logcroak("Cannot close input '$input'");
}

### construct AssayResultSet objects from the given inputs ###
my (@results);
my $total = 0;
if ($use_irods) {
    my $irods = WTSI::NPG::iRODS->new;
    foreach my $input (@inputs) {
        my $resultSet;
        if ($inputType eq $SEQUENOM_TYPE) {
            my $d_obj = WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new(
                $irods, $input);
            $resultSet = WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
                data_object => $d_obj);
        } else {
            my $d_obj = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new(
                $irods, $input);
            $resultSet = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
                data_object => $d_obj);
        }
        $total += scalar(@{$resultSet->assay_results()});
        push(@results, $resultSet);
    }
} else {
    foreach my $input (@inputs) {
        my $resultSet;
        if ($inputType eq $SEQUENOM_TYPE) {
            $resultSet = WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
                $input);
        } else {
            $resultSet = WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
                $input);
        }
        $total += scalar(@{$resultSet->assay_results()});
        push(@results, $resultSet);
    }
}
$log->info("Found $total assay results");

### convert to VCF, and do genotype consistency check if requested ###
my $converter;
if ($use_irods) {
    $converter = WTSI::NPG::Genotyping::VCF::VCFConverter->new(
        resultsets => \@results, input_type => $inputType,
        $plexCollOpt => $plexColl);
} else {
    $converter = WTSI::NPG::Genotyping::VCF::VCFConverter->new(
        resultsets => \@results, input_type => $inputType);
}
my $vcf = $converter->convert($vcfPath);

if ($gtCheck) {
    my $checker = WTSI::NPG::Genotyping::VCF::VCFGtcheck->new(verbose => $verbose);
    my ($resultRef, $maxDiscord) = $checker->run_with_string($vcf);
    my $msg = sprintf "VCF consistency check complete. Maximum pairwise difference %.4f", $maxDiscord;
    $log->info($msg);
    if ($jsonOut) {
        $log->info("Writing JSON output to $jsonOut");
        $checker->write_results_json($resultRef, $maxDiscord, $jsonOut);
    }
    if ($textOut) {
        $log->info("Writing text output to $textOut");
        $checker->write_results_text($resultRef, $maxDiscord, $textOut);
    }
} elsif ($textOut || $jsonOut) {
    $log->logwarn("Warning: Text/JSON output of concordance metrics will not be written unless the --gtcheck option is in effect. Run with --help for details.");
}


__END__

=head1 NAME

vcf_from_plex

=head1 SYNOPSIS

vcf_from_plex (options)

Options:

  --chromosomes=PATH  Path to a JSON file with chromosome lengths, used to
                      produce the VCF header. PATH must be on the local
                      filesystem (not iRODS). Optional for iRODS inputs,
                      required otherwise.
  --gtcheck           Run the bcftools gtcheck function to find consistency
                      of calls between samples; computes pairwise difference
                      metrics. Metrics are written to file if --json and/or
                      --text is specified.
  --help              Display this help and exit
  --input=PATH        List of input paths, one per line. The inputs may be
                      on a locally mounted filesystem, or locations of iRODS
                      data objects. In the former case, the --chromosomes
                      and --manifest options must be specified;
                      otherwise default values can be found from iRODS
                      metadata. The inputs are Sequenom or Fluidigm "CSV"
                      files. The input list is read from the given PATH, or
                      from standard input if PATH is omitted or equal to '-'.
                      Fluidigm and Sequenom file formats may not be mixed.
  --irods             Indicates that inputs are in iRODS. If absent, inputs
                      are assumed to be in the local filesystem, and the
                      --manifest and --chromosomes options are required.
  --manifest=PATH     Path to the tab-separated manifest file giving SNP
                      information for the QC plex. PATH must be on the local
                      filesystem (not iRODS). Optional for iRODS inputs,
                      required otherwise.
  --plex_type=NAME    Either fluidigm or sequenom. Required.
  --plex_coll=PATH    Path of an iRODS data collection containing QC plex
                      manifest files. Optional, defaults to
                      /seq/$PLEX_NAME/multiplexes. Has no effect for
                      non-iRODS inputs.
  --vcf=PATH          Path for VCF file output. Optional; if not given, VCF
                      is not written. If equal to '-', output is written to
                      STDOUT.
  --json=PATH         Path for JSON output of gtcheck metrics.
                      Optional; if not given, JSON is not written.
  --text=PATH         Path for text output of gtcheck metrics.
                      Optional; if not given, text is not written.
  --logconf=PATH      Path to Log4Perl configuration file. Optional.
  --verbose           Print additional status information to STDERR.


=head1 DESCRIPTION

Script to read QC plex output files (Sequenom or Fluidigm) from iRODS;
convert to VCF; and check the VCF file for consistency of calls between
samples. Can be used when multiple "samples" originate from the same
individual, but were taken from different tissues or at different times.

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
