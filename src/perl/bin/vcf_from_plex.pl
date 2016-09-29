#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use File::Basename qw(fileparse);
use File::Slurp qw(read_file);
use Getopt::Long;
use JSON;
use List::MoreUtils qw(uniq);
use Log::Log4perl qw(:levels);
use Pod::Usage;

use WTSI::DNAP::Utilities::ConfigureLogger qw(log_init);
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::Genotyping::Types qw(:all);
use WTSI::NPG::Utilities qw(user_session_log);
use WTSI::NPG::Genotyping::VCF::AssayResultParser;
use WTSI::NPG::Genotyping::VCF::ReferenceFinder;
use WTSI::NPG::Genotyping::VCF::VCFDataSet;

our $VERSION = '';

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


my ($input, $inputType, $vcfPath, $log4perl_config, $use_irods,
    $debug, $quiet, $repository, $snpset_path, $chromosome_json,
    $metadata_json, $callset_name, $verbose);

my $CHROMOSOME_JSON_KEY = 'chromosome_json';
our $SEQUENOM_TYPE = 'sequenom';
our $FLUIDIGM_TYPE = 'fluidigm';

my $irods;

GetOptions('callset=s'         => \$callset_name,
           'chromosomes=s'     => \$chromosome_json,
           'metadata=s'        => \$metadata_json,
           'snpset=s'          => \$snpset_path,
           'debug'             => \$debug,
           'help'              => sub { pod2usage(-verbose => 2,
                                                  -exitval => 0) },
           'input=s'           => \$input,
           'irods'             => \$use_irods,
           'logconf=s'         => \$log4perl_config,
           'plex_type=s'       => \$inputType,
           'repository=s'      => \$repository,
           'vcf=s'             => \$vcfPath,
           'quiet'             => \$quiet,
           'verbose'           => \$verbose,
       );

my @log_levels;
if ($debug) { push @log_levels, $DEBUG; }
if ($verbose) { push @log_levels, $INFO; }
log_init(config => $log4perl_config,
         file   => $session_log,
         levels => \@log_levels);
my $log = Log::Log4perl->get_logger('main');

### process command-line options and make sanity checks ###
unless ($inputType eq $SEQUENOM_TYPE || $inputType eq $FLUIDIGM_TYPE) {
    $log->logcroak("Incorrect plex type: Must be '", $SEQUENOM_TYPE,
                   "' or '", $FLUIDIGM_TYPE, "'");
}
unless ($snpset_path) {
    $log->logcroak("Must specify a snpset path in iRODS or local filesystem");
}

my $snpsetFile = fileparse($snpset_path);

$repository ||= $ENV{NPG_REPOSITORY_ROOT};
unless (-d $repository) {
    $log->logcroak("Repository path '", $repository,
                   "' does not exist or is not a directory.");
}

### read snpset and chromosome data
my ($snpset, $chromosome_lengths);
if ($use_irods) {
    $irods = WTSI::NPG::iRODS->new();
    my $snpset_obj = WTSI::NPG::iRODS::DataObject->new
        ($irods, $snpset_path);
    $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_obj);
    if ($chromosome_json) {
        $chromosome_lengths = decode_json(read_file($chromosome_json));
    } else {
        $chromosome_lengths = _chromosome_lengths_irods($irods, $snpset_obj);
    }
} else {
    $snpset = WTSI::NPG::Genotyping::SNPSet->new($snpset_path);
    if (!$chromosome_json) {
        $log->logcroak("--chromosomes is required for non-iRODS input");
    } elsif (!(-e $chromosome_json)) {
        $log->logcroak("Chromosome path '$chromosome_json' does not exist");
    }
    $chromosome_lengths = decode_json(read_file($chromosome_json));
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

### process inputs and write VCF output ###

my $resultsets = _build_resultsets(\@inputs, $inputType, $irods);
my $metadata;
if ($metadata_json) {
    $metadata = decode_json(read_file($metadata_json));
} elsif ($use_irods) {
    $metadata = _metadata_from_irods(\@inputs, $irods);
} else {
    $metadata = {};
}
if (defined($callset_name)) {
    $metadata->{'callset_name'} = [ $callset_name, ];
}

my %parserArgs = (resultsets => $resultsets,
                  assay_snpset => $snpset,
                  contig_lengths => $chromosome_lengths,
                  metadata => $metadata);

my $parser = WTSI::NPG::Genotyping::VCF::AssayResultParser->new
    (\%parserArgs);
my $vcf_dataset = $parser->get_vcf_dataset();
$vcf_dataset->write_vcf($vcfPath);


sub _metadata_from_irods {
    my ($inputs, $irods) = @_;
    # create VCF metadata from iRODS metadata
    # $inputs is an ArrayRef of iRODS paths
    # TODO fix redundancy with vcf_metadata_from_irods in Subscription.pm
    my %vcf_meta;
    foreach my $input (@{$inputs}) { # check iRODS metadata
        my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $input);
        my @obj_meta = @{$obj->metadata};
        foreach my $pair (@obj_meta) {
            my $key = $pair->{'attribute'};
            my $val = $pair->{'value'};
            if ($key eq 'fluidigm_plex') {
                push @{ $vcf_meta{'plex_type'} }, 'fluidigm';
                push @{ $vcf_meta{'plex_name'} }, $val;
            } elsif ($key eq 'sequenom_plex') {
                push @{ $vcf_meta{'plex_type'} }, 'sequenom';
                push @{ $vcf_meta{'plex_name'} }, $val;
            } elsif ($key eq 'reference') {
                my $rf = WTSI::NPG::Genotyping::VCF::ReferenceFinder->new(
                    reference_genome => $val,
                    repository       => $repository,
                );
                push @{ $vcf_meta{'reference'} }, $rf->get_reference_uri();
            }
        }
    }
    foreach my $key (keys %vcf_meta) {
        my @values = @{$vcf_meta{$key}};
        $vcf_meta{$key} = [ uniq @values ];
    }
    return \%vcf_meta;
}


sub _build_resultsets {
    my ($inputs, $input_type, $irods) = @_;
    my @results;
    if (defined($irods)) { # read input from iRODS
        foreach my $input (@{$inputs}) {
            my $resultSet;
            if ($input_type eq $SEQUENOM_TYPE) {
                my $data_obj =
                    WTSI::NPG::Genotyping::Sequenom::AssayDataObject->new(
                        $irods, $input);
                $resultSet =
                    WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
                        data_object => $data_obj);
            } elsif ($input_type eq $FLUIDIGM_TYPE) {
                my $data_obj =
                    WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new(
                        $irods, $input);
                $resultSet =
                    WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
                        data_object => $data_obj);
            } else {
                $log->logcroak();
            }
            push @results, $resultSet;
        }
    } else { # read input from local filesystem
         foreach my $input (@{$inputs}) {
             my $resultSet;
             if ($input_type eq $SEQUENOM_TYPE) {
                 $resultSet =
                     WTSI::NPG::Genotyping::Sequenom::AssayResultSet->new(
                         $input);
             } elsif ($input_type eq $FLUIDIGM_TYPE) {
                 $resultSet =
                     WTSI::NPG::Genotyping::Fluidigm::AssayResultSet->new(
                         $input);
             } else {
                 $log->logcroak();
             }
             push @results, $resultSet;
         }
    }
    return \@results;
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
  --logconf=PATH      Path to Log4Perl configuration file. Optional.
  --plex_type=NAME    Either fluidigm or sequenom. Required.
  --quiet             Only print warning messages to the default log.
  --repository=DIR    Location of the root directory for NPG genome
                      reference repository. Defaults to the value of the
                      NPG_REPOSITORY_ROOT environment variable.
  --snpset            Path to a tab-separated manifest file with information
                      on the SNPs in the QC plex. Path must be in iRODS if the
                      --irods option is in effect, or on the local filesystem
                      otherwise. Required.
  --vcf=PATH          Path for VCF file output. Optional; if not given, VCF
                      is not written. If equal to '-', output is written to
                      STDOUT.
  --verbose           Print additional messages to the default log.


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

Copyright (c) 2014, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
