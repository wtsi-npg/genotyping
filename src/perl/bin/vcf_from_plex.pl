#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use Getopt::Long;
use JSON;
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
    $log, $logConfig, $verbose, $use_irods, $debug,
    $snpset_path, $chromosome_json);

my $SEQUENOM_TYPE = 'sequenom'; # TODO avoid repeating these across modules
my $FLUIDIGM_TYPE = 'fluidigm';
my $CHROMOSOME_JSON_KEY = 'chromosome_json';

GetOptions('chromosomes=s'     => \$chromosome_json,
           'snpset=s'          => \$snpset_path,
           'debug'             => \$debug,
           'help'              => sub { pod2usage(-verbose => 2,
                                                  -exitval => 0) },
           'input=s'           => \$input,
           'irods'             => \$use_irods,
           'json=s'            => \$jsonOut,
           'logconf=s'         => \$logConfig,
           'plex_type=s'       => \$inputType,
           'text=s'            => \$textOut,
           'vcf=s'             => \$vcfPath,
           'gtcheck'           => \$gtCheck,
           'verbose'           => \$verbose,
       );

### set up logging ###
if ($logConfig) { Log::Log4perl::init($logConfig); } 
else { Log::Log4perl::init(\$embedded_conf); }
$log = Log::Log4perl->get_logger('npg.vcf.plex');
if ($verbose) { $log->level($INFO); }
elsif ($debug) { $log->level($DEBUG); }

### process command-line options and make sanity checks ###
if ($inputType ne $SEQUENOM_TYPE && $inputType ne $FLUIDIGM_TYPE) {
    $log->logcroak(
        "Must specify $SEQUENOM_TYPE or $FLUIDIGM_TYPE as plex type");
}
unless ($snpset_path) { 
    $log->logcroak("Must specify a snpset path in iRODS or local filesystem");
}
my ($snpset, $chroms);
if ($use_irods) {
    my $irods = WTSI::NPG::iRODS->new();
    $irods->logger($log);
    my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
        ($irods, $snpset_path);
    $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_obj);
    if ($chromosome_json) {
        $chroms = _read_json($chromosome_json);
    } else {
        $chroms = _chromosome_lengths_irods($irods, $snpset_obj);
    }
} else {
    $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_path);
    if (!$chromosome_json) {
        $log->logcroak("--chromosomes is required for non-iRODS input");
    } elsif (!(-e $chromosome_json)) {
        $log->logcroak("Chromosome path '$chromosome_json' does not exist");
    }
    $chroms = _read_json($chromosome_json);
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
        snpset => $snpset, chromosome_lengths => $chroms);
} else {
    $converter = WTSI::NPG::Genotyping::VCF::VCFConverter->new(
        resultsets => \@results, input_type => $inputType,
        snpset => $snpset, chromosome_lengths => $chroms);
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

sub _chromosome_lengths_irods {
    # get reference to a hash of chromosome lengths
    # read from JSON file, identified by snpset metadata in iRODS
    my ($irods, $snpset_obj) = @_;
    my @avus = $snpset_obj->find_in_metadata($CHROMOSOME_JSON_KEY);
    if (scalar(@avus)!=1) {
        $log->logcroak("Must have exactly one $CHROMOSOME_JSON_KEY value",
                       " in iRODS metadata for SNP set file");
    }
    my %avu = %{ shift(@avus) };
    my $chromosome_json = $avu{'value'};
    my $data_object = WTSI::NPG::iRODS::DataObject->new
        ($irods, $chromosome_json);
    return decode_json($data_object->slurp());
}

sub _read_json {
    # read given path into a string and decode as JSON
    my $input = shift;
    open my $in, '<:encoding(utf8)', $input ||
        log->logcroak("Cannot open input '$input'");
    my $data = decode_json(join("", <$in>));
    close $in || $log->logcroak("Cannot close input '$input'");
    return $data;
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
                      and --snpset options must be specified;
                      otherwise default values can be found from iRODS
                      metadata. The inputs are Sequenom or Fluidigm "CSV"
                      files. The input list is read from the given PATH, or
                      from standard input if PATH is omitted or equal to '-'.
                      Fluidigm and Sequenom file formats may not be mixed.
  --irods             Indicates that inputs are in iRODS. If absent, inputs
                      are assumed to be in the local filesystem, and the
                      --snpset and --chromosomes options are required.
  --plex_type=NAME    Either fluidigm or sequenom. Required.
  --snpset            Path to a tab-separated manifest file with information
                      on the SNPs in the QC plex. Path must be in iRODS if the
                      --irods option is in effect, or on the local filesystem 
                      otherwise.
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
