#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;

use Carp;
use Cwd qw(abs_path);
use DateTime;
use File::Basename qw(basename);
use Getopt::Long;
use List::MoreUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Net::LDAP;
use Pod::Usage;
use URI;
use UUID;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::iRODS qw(collect_files);
use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri
                              get_publisher_name
                              pair_rg_channel_files);
use WTSI::NPG::Expression::Publication qw(publish_expression_analysis
                                          parse_beadchip_table_v1
                                          parse_beadchip_table_v2);
use WTSI::NPG::Utilities qw(trim user_session_log);
use WTSI::NPG::Utilities::IO qw(maybe_stdin);

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

# our $DEFAULT_ANALYSIS_DEST = '/archive/GAPI/exp/analysis';
# our $DEFAULT_SAMPLE_DEST = '/archive/GAPI/exp/infinium';

run() unless caller();

sub run {
  my $analysis_source;
  my $dbfile;
  my $debug;
  my $log4perl_config;
  my $manifest;
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
             'manifest=s'         => \$manifest,
             'manifest-version=s' => \$manifest_version,
             'sample-dest=s'      => \$publish_sample_dest,
             'sample-source=s'    => \$sample_source,
             'uuid=s'             => \$uuid,
             'verbose'            => \$verbose);

  unless ($analysis_source) {
    pod2usage(-msg => "An --analysis-source argument is required\n",
              -exitval => 3);
  }
  unless ($sample_source) {
    pod2usage(-msg => "A --sample-source argument is required\n",
              -exitval => 3);
  }

  unless ($publish_analysis_dest) {
    pod2usage(-msg => "An --analysis-dest argument is required\n",
              -exitval => 3);
  }
  unless ($publish_sample_dest) {
    pod2usage(-msg => "A --sample-dest argument is required\n",
              -exitval => 3);
  }

  unless (-e $analysis_source) {
    pod2usage(-msg => "No such analysis source as '$analysis_source'\n",
              -exitval => 4);
  }
  unless (-d $analysis_source) {
    pod2usage(-msg => "The --analysis-source argument was not a directory\n",
              -exitval => 4);
  }

  unless (-e $sample_source) {
    pod2usage(-msg => "No such sample source as '$sample_source'\n",
              -exitval => 4);
  }
  unless (-d $sample_source) {
    pod2usage(-msg => "The --sample-source argument was not a directory\n",
              -exitval => 4);
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

  # Hack to persuade the automounter to work
  opendir(my $dir, $sample_source);
  readdir($dir);
  closedir($dir);

  $manifest_version ||= '2';
  my $config ||= $DEFAULT_INI;
  my $in = maybe_stdin($manifest);

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile => $config)->connect(RaiseError => 1,
                                  mysql_enable_utf8 => 1,
                                  mysql_auto_reconnect => 1);
  $ssdb->log($log);

  my @samples;

  if ($manifest_version eq '1') {
    @samples = parse_beadchip_table_v1($in, $ssdb);
  }
  elsif ($manifest_version eq '2') {
    @samples = parse_beadchip_table_v2($in, $ssdb);
  }
  else {
    pod2usage(-msg => "Invalid --manifest-version, expected one of [1, 2]\n",
              -exitval => 4);
  }
  unless (@samples) {
    $log->logcroak("Found no sample rows in input: stopping\n");
  }

  my @beadchips = uniq(map { $_->{beadchip} } @samples);
  my @sections = map { $_->{beadchip_section} } @samples;

  my $channel = 'Grn';
  my $beadchips_patt = join('|', @beadchips);
  my $sections_patt = join('|', @sections);
  my $filename_regex = qr{($beadchips_patt)_($sections_patt)_$channel.(idat|xml)$}msxi;

  my $sample_dir = abs_path($sample_source);
  my $file_test = sub { return $_[0] =~ $filename_regex };
  my $relative_depth = 3;

  my @paths = collect_files($sample_dir, $file_test, $relative_depth);
  my $samples = add_paths(\@samples, \@paths);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $name = get_publisher_name($publisher_uri);
  my $now = DateTime->now();

  $log->info("Publishing samples from '$sample_source' ",
             "to '$publish_sample_dest' as ", $name);
  $log->info("Publishing analysis from '$analysis_source' ",
             "to '$publish_analysis_dest' as ", $name);

  my $analysis_uuid =
    publish_expression_analysis($analysis_source, $creator_uri,
                                $publish_analysis_dest,
                                $publish_sample_dest,
                                $publisher_uri, $samples,
                                $ssdb, $now, $uuid);
  if (defined $uuid && defined $analysis_uuid)  {
    print "Used analysis UUID: ", $analysis_uuid, "\n";
  }
  elsif (defined $analysis_uuid) {
    print "New analysis UUID: ", $analysis_uuid, "\n";
  }
  else {
    $log->error('No analysis UUID; upload aborted because of errors.',
                ' Please raise an RT ticket or email ',
                'new-seq-pipe@sanger.ac.uk');
  }
}

sub add_paths {
  my ($samples, $paths) = @_;

  foreach my $sample (@$samples) {
    add_path($sample, 'idat_file', 'idat_path', $paths);
    add_path($sample, 'xml_file', 'xml_path', $paths);
  }

  return $samples;
}

sub add_path {
  my ($sample, $file_key, $type, $paths) = @_;

  my $id = $sample->{sanger_sample_id};
  my $pattern = $sample->{$file_key}; # 'idat_file' or 'xml_file'
  my @matches = grep { m{$pattern$}msxi } @$paths;

  my $count = scalar @matches;
  if ($count == 0) {
    $log->logcroak("Failed to find the $type file $pattern for sample ",
                   "'$id' under the sample-source directory");
  }
  elsif (scalar @matches == 1) {
    $sample->{$type} = $matches[0];
  }
  else {
    $log->logcroak("Found multiple $type files matching $pattern for sample ",
                   "'$id': [", join(', ', @matches), "]");
  }

  return $sample;
}


__END__

=head1 NAME


=head1 SYNOPSIS

publish_expression_data --analysis-source <directory> --analysis-dest <collection>
                        --sample-source <directory> --sample-dest <collection>
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

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
