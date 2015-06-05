#! /software/bin/perl

use warnings;
use strict;

use File::Basename qw/fileparse/;
use Getopt::Long;
use JSON;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess;

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

    my $debug;
    my $log4perl_config;
    my $outPath;
    my @inputJson;
    my @inputNames;
    my $verbose;

    GetOptions(
        'debug'             => \$debug,
        'help'              => sub { pod2usage(-verbose => 2,
                                               -exitval => 0) },
        'logconf=s'         => \$log4perl_config,
        'in=s'              => \@inputJson,
        'out=s'             => \$outPath,
        'name=s'            => \@inputNames,
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
    my $nameTotal = scalar @inputNames;
    my $inputTotal = scalar @inputJson;
    if ($nameTotal != 0 && $nameTotal != $inputTotal) {
        $log->logcroak("If names are specified, number of names and inputs ",
                       "must be equal; but ", $nameTotal,
                       "names were given for ", $inputTotal, " inputs.");
    }
    my %inputs;
    for (my $i=0;$i<$inputTotal;$i++) {
        my $key;
        if (defined($inputNames[$i])) {
            $key = $inputNames[$i];
        } else {
            $key = fileparse($inputJson[$i]); # use filename as key
        }
        if (defined($inputs{$key})) {
            $log->logcroak("Input name '", $key, "' appears more than once; ",
                           "names must be unique.");
        }
        $inputs{$key} = $inputJson[$i];
    }

    my $processor =
        WTSI::NPG::Genotyping::QC_wip::Check::IdentityPostProcess->new();
    $processor->mergeIdentityFiles(\%inputs, $outPath);

}



__END__

=head1 NAME

write_identity_csv

=head1 SYNOPSIS

check_identity_bed_wip  [--help] [--verbose] --name <identity name 1> \
--in <identity JSON path 1>  [--name <identity name 2>] \
[--in <identity JSON path 2>] [...]

Options:

  --help                 Display help.
  --logconf=PATH         Path to Perl logger configuration file. Optional.
  --min-shared-snps=NUM  Minimum number of shared SNPs between production and
                         QC plex to carry out identity check. Optional.
  --in                   Input file, in JSON format produced by the identity
                         check. May be specified more than once to create a
                         merged CSV file.
  --name                 Name of each input dataset, used in column headers
                         for CSV. If any names are given, there must be
                         exactly one name per input file. Names are assigned
                         to input files in the same order as they appear on
                         the command line. If no names are given, default is
                         the filename (omitting any parent directory). Each
                         dataset must have a unique name.
  --out=PATH             Path for JSON output. Optional, defaults to STDOUT.
  --pass-threshold=NUM   Minimum similarity to pass identity check. Optional.
  --plex-manifest=PATH   Path to .csv manifest for the QC plex SNP set.
  --plink=STEM           Plink binary stem (path omitting the .bed, .bim, .fam
                         suffix) for production data.
  --swap-threshold=NUM   Minimum cross-similarity to warn of sample swap.
                         Optional.
  --verbose              Print messages while processing. Optional.

=head1 DESCRIPTION

Convert one or more JSON files output by the pipeline identity check to CSV
format. CSV output has one line per (snp, sample) pair. If more than one file
is input, the QC plex calls will appear in adjacent columns. An error is
raised if production calls conflict for any (snp, sample) pair in the inputs.

=head1 EXAMPLES

check_identity_bed_wip --in /foo/bar/id_1.json --name identity_A
--in /foo/bar/id_2.json --name identity_B --out /foo/bar/merged.csv

check_identity_bed_wip --in /foo/bar/id_1.json --in /foo/bar/id_2.json

The second command above produces exactly the same output as the first, but
with column headers "id_1.json" and "id_2.json" instead of "identity_A" and
"identity_B", respectively.

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
