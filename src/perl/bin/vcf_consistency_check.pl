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

use WTSI::NPG::Genotyping::VCF::GtcheckWrapper;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'vcf_consistency_check');

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


my ($input, $jsonOut, $log, $logConfig, $textOut, $verbose);

GetOptions('help'        => sub { pod2usage(-verbose => 2,
                                            -exitval => 0) },
           'input=s'     => \$input,
           'json=s'      => \$jsonOut,
           'logconf=s'   => \$logConfig,
           'text=s'      => \$textOut,
           'verbose'     => \$verbose
       );


### set up logging ###
if ($logConfig) { Log::Log4perl::init($logConfig); }
else { Log::Log4perl::init(\$embedded_conf); }
$log = Log::Log4perl->get_logger('npg.vcf.consistency');


### read input and do consistency check

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
    push(@inputs, $_);
}
my $vcf = join('', @inputs);
if ($input ne '-') {
    close $in || $log->logcroak("Cannot close input '$input'");
}
my $checker = WTSI::NPG::Genotyping::VCF::GtcheckWrapper->new(
    verbose => $verbose);
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

# TODO add a --irods option


__END__

=head1 NAME

vcf_consistency_check

=head1 SYNOPSIS

vcf_consistency_check (options)

Options:

  --input=PATH        Path to an input file in VCF format, or - to read from
                      standard input.
  --help              Display this help and exit
  --json=PATH         Path for JSON output of gtcheck metrics.
                      Optional; if not given, JSON is not written.
  --text=PATH         Path for text output of gtcheck metrics, or '-' for
                      standard output. Optional; if not given, text is not
                      written.
  --logconf=PATH      Path to Log4Perl configuration file. Optional.
  --quiet             Suppress printing of status information.


=head1 DESCRIPTION

Uses the bcftools application to check a VCF file for consistency of
genotype calls between samples. Can be used when multiple "samples"
originate from the same individual, but were taken from different tissues or
at different times. Outputs a set of pairwise differences, and the maximum
pairwise difference across the entire input file.

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
