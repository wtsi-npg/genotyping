#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;

use Carp;
use Cwd qw(abs_path);
use DateTime;
use Getopt::Long;
use List::AllUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Net::LDAP;
use Pod::Usage;
use URI;
use UUID;

use WTSI::DNAP::Utilities::IO qw(maybe_stdin);
use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Expression::AnalysisPublisher;
use WTSI::NPG::Expression::ChipLoadingManifestV1;
use WTSI::NPG::Expression::ChipLoadingManifestV2;
use WTSI::NPG::Expression::Publisher;
use WTSI::NPG::Utilities qw(collect_files trim user_session_log);

our $VERSION = '';

my $uid = `whoami`;
chomp($uid);
my $session_log = user_session_log($uid, 'publish_expression_analysis');

my $embedded_conf = "
   log4perl.logger.npg.irods.publish = ERROR, A1, A2

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

our $DEFAULT_INI = $ENV{HOME} . '/.npg/genotyping.ini';

our $EXIT_CLI_ARG = 3;
our $EXIT_CLI_VAL = 4;
our $EXIT_UPLOAD  = 5;

# our $DEFAULT_ANALYSIS_DEST = '/archive/GAPI/exp/analysis';
# our $DEFAULT_SAMPLE_DEST = '/archive/GAPI/exp/infinium';

run() unless caller();

sub run {
  my $analysis_source;
  my $debug;
  my $log4perl_config;
  my $manifest_path;
  my $manifest_version;
  my $publish_analysis_dest;
  my $publish_sample_dest;
  my $sample_source;
  my $uuid;
  my $verbose;

  GetOptions('analysis-dest=s'    => \$publish_analysis_dest,
             'analysis-source=s'  => \$analysis_source,
             'debug'              => \$debug,
             'help'               => sub { pod2usage(-verbose => 2,
                                                     -exitval => 0) },
             'logconf=s'          => \$log4perl_config,
             'manifest=s'         => \$manifest_path,
             'manifest-version=s' => \$manifest_version,
             'sample-dest=s'      => \$publish_sample_dest,
             'sample-source=s'    => \$sample_source,
             'uuid=s'             => \$uuid,
             'verbose'            => \$verbose);

  unless ($analysis_source) {
    pod2usage(-msg     => "An --analysis-source argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }
  unless ($sample_source) {
    pod2usage(-msg     => "A --sample-source argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }

  unless ($publish_analysis_dest) {
    pod2usage(-msg     => "An --analysis-dest argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }
  unless ($publish_sample_dest) {
    pod2usage(-msg     => "A --sample-dest argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }
  unless ($manifest_path) {
    pod2usage(-msg     => "A --manifest argument is required\n",
              -exitval => $EXIT_CLI_ARG);
  }
  unless ($manifest_path) {
    pod2usage(-msg => "A --manifest argument is required\n",
              -exitval => 3);
  }

  unless (-e $analysis_source) {
    pod2usage(-msg     => "No such analysis source as '$analysis_source'\n",
              -exitval => $EXIT_CLI_VAL);
  }
  unless (-d $analysis_source) {
    pod2usage
      (-msg     => "The --analysis-source argument was not a directory\n",
       -exitval => $EXIT_CLI_VAL);
  }

  unless (-e $sample_source) {
    pod2usage(-msg     => "No such sample source as '$sample_source'\n",
              -exitval => $EXIT_CLI_VAL);
  }
  unless (-d $sample_source) {
    pod2usage(-msg     => "The --sample-source argument was not a directory\n",
              -exitval => $EXIT_CLI_VAL);
  }
  unless (-e $manifest_path) {
    pod2usage(-msg     => "No such manifest as '$manifest_path'\n",
              -exitval => $EXIT_CLI_VAL);
  }
  unless (-e $manifest_path) {
    pod2usage(-msg => "No such manifest as '$manifest_path'\n",
              -exitval => 3);
  }

  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.publish');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    $log = Log::Log4perl->get_logger('npg.irods.publish');

    if ($verbose) {
      $log->level($INFO);
    }
    elsif ($debug) {
      $log->level($DEBUG);
    }
  }

  $log->info("Publishing samples from '$sample_source' ",
             "to '$publish_sample_dest'");
  $log->info("Publishing analysis from '$analysis_source' ",
             "to '$publish_analysis_dest'");

  # Hack to persuade the automounter to work
  opendir(my $dir, $sample_source);
  readdir($dir);
  closedir($dir);

  $manifest_version ||= '2';
  my $config ||= $DEFAULT_INI;
  my $in = maybe_stdin($manifest_path);

  my $manifest;
  if ($manifest_version eq '1') {
    pod2usage
      (-msg     => "Invalid --manifest-version, version 1 manifests " .
                   "are no longer supported\n",
       -exitval => $EXIT_CLI_VAL);
  }
  elsif ($manifest_version eq '2') {
    $manifest = WTSI::NPG::Expression::ChipLoadingManifestV2->new
      (file_name => $manifest_path);
  }
  else {
    pod2usage
      (-msg     => "Invalid --manifest-version, expected one of [1, 2]\n",
       -exitval => $EXIT_CLI_VAL);
  }

  my $publication_time = DateTime->now;
  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile => $config,
     logger  => $log)->connect(RaiseError           => 1,
                               mysql_enable_utf8    => 1,
                               mysql_auto_reconnect => 1);

  my @data_files = find_data_files($sample_source, $manifest);
  my $sample_publisher = WTSI::NPG::Expression::Publisher->new
    (data_files       => \@data_files,
     manifest         => $manifest,
     publication_time => $publication_time,
     sequencescape_db => $ssdb,
     logger           => $log);

  # Includes secondary metadata (from warehouse)
  $sample_publisher->publish($publish_sample_dest);

  my $analysis_publisher = WTSI::NPG::Expression::AnalysisPublisher->new
    (analysis_directory => $analysis_source,
     manifest           => $manifest,
     publication_time   => $publication_time,
     sample_archive     => $publish_sample_dest,
     irods              => $sample_publisher->irods,
     logger             => $log);

  # Uses the secondary metadata added above to find the sample data in
  # iRODS for cross-referencing
  my $analysis_uuid =
    $analysis_publisher->publish($publish_analysis_dest, $uuid);

  if (defined $uuid && defined $analysis_uuid)  {
    print "Used analysis UUID: ", $analysis_uuid, "\n";
  }
  elsif (defined $analysis_uuid) {
    print "New analysis UUID: ", $analysis_uuid, "\n";
  }
  else {
    exit $EXIT_UPLOAD;
    $log->error('No analysis UUID; upload aborted because of errors.',
                ' Please raise an RT ticket or email ',
                'new-seq-pipe@sanger.ac.uk');
  }
}

sub find_data_files {
  my ($sample_source, $manifest) = @_;

  my @samples = @{$manifest->samples};

  $log->info("Finding sample data for: [",
             join(", ", map { $_->{sample_id} } @samples), "]");

  my @beadchips = uniq(map { $_->{beadchip} } @samples);
  my @sections = map { $_->{beadchip_section} } @samples;

  my $channel = 'Grn';
  my $beadchips_patt = join('|', @beadchips);
  my $sections_patt = join('|', @sections);
  my $filename_regex =
    qr{($beadchips_patt)_($sections_patt)_$channel.(idat|xml)$}msxi;

  $log->debug("Finding sample data files matching regex '$filename_regex'");

  my $sample_dir = abs_path($sample_source);
  my $file_test = sub { return $_[0] =~ $filename_regex };
  my $relative_depth = 3;

  return collect_files($sample_dir, $file_test, $relative_depth);
}

__END__

=head1 NAME

publish_expression_data - Publish Beadarray expression data to iRODS.

=head1 SYNOPSIS

publish_expression_data --analysis-source <directory> \
                        --analysis-dest <collection> \
                        --sample-source <directory> \
                        --sample-dest <collection>
                        [--manifest <file>] [--verbose]

Options:

  --analysis-dest   The data destination root collection for the analysis
                    data in iRODS. E.g. /archive/GAPI/exp/analysis
  --analysis-source The root directory of the analysis.
  --help            Display help.
  --logconf         A log4perl configuration file. Optional.
  --manifest        Tab-delimited chip loading manifest. Optional,
                    defaults to STDIN.
  --sample-dest     The data destination root collection for the sample
                    data in iRODS. E.g. /archive/GAPI/exp/infinium
  --sample-source   The root directory of all samples.
  --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION


=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015, 2016 Genome Research Limited. All
Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
