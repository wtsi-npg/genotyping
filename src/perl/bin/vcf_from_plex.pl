#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Carp;
use File::Temp qw(tempdir);
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

my ($input, $plexType, $plexDir, $vcfPath, $jsonOut, $textOut, $log,
    $logConfig, $verbose, $debug);

GetOptions('debug'       => \$debug,
           'help'        => sub { pod2usage(-verbose => 2,
                                            -exitval => 0) },
           'input=s'     => \$input,
           'json=s'      => \$jsonOut,
           'logconf=s'   => \$logConfig,
           'plex_dir=s'  => \$plexDir,
           'plex_type=s' => \$plexType,
           'text=s'      => \$textOut,
           'vcf=s'       => \$vcfPath,
           'verbose'     => \$verbose,
       );

if ($logConfig) {
    Log::Log4perl::init($logConfig);
    $log = Log::Log4perl->get_logger('npg.vcf.plex');
} else {
    Log::Log4perl::init(\$embedded_conf);
    $log = Log::Log4perl->get_logger('npg.vcf.plex');
    if ($verbose) {
        $log->level($INFO);
    }
    elsif ($debug) {
        $log->level($DEBUG);
    }
}

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

my $plexDirOpt;
if ($plexType eq 'sequenom') {
    $plexDirOpt = 'sequenom_plex_dir';
    $plexDir ||= '/seq/sequenom/multiplexes';
} elsif ($plexType eq 'fluidigm') {
    $plexDirOpt = 'fluidigm_plex_dir';
    $plexDir ||= '/seq/fluidigm/multiplexes';
} else {
    $log->logcroak("Must specify either 'sequenom' or 'fluidigm' as plex type");
}

unless ($vcfPath) {
    my $tmpDir = tempdir('vcf_from_plex_XXXXXX', CLEANUP => 1);
    $vcfPath = $tmpDir."/qc_plex.vcf";
}

my $converter = WTSI::NPG::Genotyping::VCF::VCFConverter->new(inputs => \@inputs, verbose => $verbose, input_type => $plexType, $plexDirOpt => $plexDir);
$converter->convert($vcfPath);
my $gtcheck = WTSI::NPG::Genotyping::VCF::VCFGtcheck->new(input => $vcfPath, verbose => $verbose);
my ($resultRef, $maxDiscord) = $gtcheck->run();
my $msg = sprintf "VCF consistency check complete. Maximum pairwise difference %.4f", $maxDiscord;
$log->info($msg);
if ($jsonOut) {
    $log->info("Writing JSON output to $jsonOut");
    $gtcheck->write_results_json($resultRef, $maxDiscord, $jsonOut);
}
if ($textOut) {
    $log->info("Writing text output to $textOut");
    $gtcheck->write_results_text($resultRef, $maxDiscord, $textOut);
}


__END__

=head1 NAME

vcf_from_plex

=head1 SYNOPSIS

vcf_from_plex (options)

Options:

  --help              Display this help and exit
  --input=PATH        List of iRODS paths to Sequenom or Fluidigm "CSV" data
                      files, one per line. File locations will be read from
                      the given PATH, or from standard input if PATH is
                      omitted or equal to '-'. The two types of file format
                      may not be mixed. Required.
  --plex_type=NAME    Either fluidigm or sequenom. Required.
  --plex_dir=PATH     Directory containing QC plex manifest files.
  --vcf=PATH          Path for VCF file output. Optional; if not given, VCF
                      is not written.
  --json=PATH         Path for JSON output of metrics computed from VCF file.
                      Optional; if not given, JSON is not written.
  --text=PATH         Path for text output of metrics computed from VCF file.
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
