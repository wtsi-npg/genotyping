#!/usr/bin/env perl

use utf8;

package main;

use strict;
use warnings;

use Carp;
use Cwd qw(abs_path);
use DateTime;
use File::Basename qw(basename);
use Getopt::Long;
use List::MoreUtils qw(firstidx uniq);
use Log::Log4perl;
use Net::LDAP;
use Pod::Usage;
use URI;
use UUID;

use Data::Dumper;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::iRODS qw(collect_files);
use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri
                              get_publisher_name
                              pair_rg_channel_files);
use WTSI::NPG::Expression::Publication qw(publish_expression_analysis);
use WTSI::NPG::Utilities qw(trim);
use WTSI::NPG::Utilities::IO qw(maybe_stdin);

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = DEBUG, A1
   log4perl.logger.quiet             = DEBUG, A2

   log4perl.appender.A1          = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.stderr   = 0
   log4perl.appender.A1.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n

   log4perl.appender.A2          = Log::Log4perl::Appender::Screen
   log4perl.appender.A2.stderr   = 0
   log4perl.appender.A2.layout   = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A2.layout.ConversionPattern = %d %p %m %n
   log4perl.appender.A2.Filter   = F2

   log4perl.filter.F2               = Log::Log4perl::Filter::LevelRange
   log4perl.filter.F2.LevelMin      = WARN
   log4perl.filter.F2.LevelMax      = FATAL
   log4perl.filter.F2.AcceptOnMatch = true
);

my $log;

our $DEFAULT_INI = $ENV{HOME} . '/.npg/genotyping.ini';

# our $DEFAULT_ANALYSIS_DEST = '/archive/GAPI/exp/analysis';
# our $DEFAULT_SAMPLE_DEST = '/archive/GAPI/exp/infinium';

run() unless caller();

sub run {
  my $dbfile;
  my $log4perl_config;
  my $analysis_source;
  my $manifest;
  my $publish_analysis_dest;
  my $publish_sample_dest;
  my $sample_source;
  my $verbose;

  GetOptions('analysis-dest=s'   => \$publish_analysis_dest,
             'analysis-source=s' => \$analysis_source,
             'help'              => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'         => \$log4perl_config,
             'manifest=s'        => \$manifest,
             'sample-dest=s'     => \$publish_sample_dest,
             'sample-source=s'   => \$sample_source,
             'verbose'           => \$verbose);

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
    if ($verbose) {
      $log = Log::Log4perl->get_logger('npg.irods.publish');
    }
    else {
      $log = Log::Log4perl->get_logger('quiet');
    }
  }

  my $config ||= $DEFAULT_INI;
  my $in = maybe_stdin($manifest);

  my @samples = parse_beadchip_table($in);
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

  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $name = get_publisher_name($publisher_uri);
  my $now = DateTime->now();
  my $make_groups = 0;

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile => $config)->connect(RaiseError => 1,
                                  mysql_enable_utf8 => 1);
  $ssdb->log($log);

  $log->info("Publishing samples from '$sample_source' to '$publish_sample_dest' as ", $name);
  $log->info("Publishing analysis from '$analysis_source' to '$publish_analysis_dest' as ", $name);

  publish_expression_analysis($analysis_source, $creator_uri, $publish_analysis_dest,
                              $publish_sample_dest, $publisher_uri, $samples,
                              $ssdb, $now, $make_groups);
}

# Expects a tab-delimited text file. Useful data start after line
# containing column headers. This line is identified by the parser by
# presence of the the string 'BEADCHIP'.
#
# Column header  Content
# 'SAMPLE ID'    Sanger sample ID
# 'BEADCHIP'     Infinium Beadchip number
# 'ARRAY'        Infinium Beadchip section
#
# Data lines follow the header, the zeroth column of which contain an
# arbitrary string. This string is the same on all data containing
# lines.
#
# Data rows are terminated by a line containing the string
# 'Kit Control' in the zeroth column. This line is ignored by the
# parser.
#
# Any whitespace-only lines are ignored.
sub parse_beadchip_table {
  my ($fh) = @_;

  # For error reporting
  my $line_count = 0;

  # Leftmost column; used only to determine which rows have sample data
  my $sample_key_col = 0;
  my $sample_key;

  # Columns containing useful data
  my $sample_id_col;
  my $beadchip_col;
  my $section_col;

  # True if we are past the header and into a data block
  my $in_sample_block = 0;

  # Collected data
  my @samples;

  while (my $line = <$fh>) {
    ++$line_count;
    chomp($line);
    next if $line =~ m/^\s*$/;

    if ($in_sample_block) {
      my @sample_row = map { trim($_) } split("\t", $line);
      unless ($sample_row[$sample_key_col]) {
        $log->logcroak("Premature end of sample data at line $line_count\n");
      }

      if (!defined $sample_key) {
        $sample_key = $sample_row[$sample_key_col];
      }

      if ($sample_key eq $sample_row[$sample_key_col]) {
        push(@samples,
             {sanger_sample_id => validate_sample_id($sample_row[$sample_id_col],
                                                     $line_count),
              beadchip         => validate_beadchip($sample_row[$beadchip_col],
                                                    $line_count),
              beadchip_section => validate_section($sample_row[$section_col],
                                                   $line_count)});
      }
      elsif ($sample_row[$sample_key_col] eq 'Kit Control') {
        # This token is taken to mean the data block has ended
        last;
      }
      else {
        $log->logcroak("Premature end of sample data at line $line_count " .
                       "(missing 'Kit Control')\n");
      }
    }
    else {
      if ($line =~ m/BEADCHIP/) {
        $in_sample_block = 1;
        my @header = map { trim($_) } split("\t", $line);
        # Expected to be Sanger sample ID
        $sample_id_col = firstidx { /SAMPLE ID/ } @header;
        # Expected to be chip number
        $beadchip_col  = firstidx { /BEADCHIP/ } @header;
        # Expected to be chip section
        $section_col = firstidx { /ARRAY/ } @header;
      }
    }
  }

  my $channel = 'Grn';
  foreach my $sample (@samples) {
    my $basename = sprintf("%s_%s_%s",
                           $sample->{beadchip},
                           $sample->{beadchip_section},
                           $channel);

    $sample->{idat_file} = $basename . '.idat';
    $sample->{xml_file}  = $basename . '.xml' ;
  }

  return @samples;
}

sub validate_sample_id {
  my ($sample_id, $line) = @_;

  unless (defined $sample_id) {
    $log->logcroak("Missing sample ID at line $line\n");
  }

  if ($sample_id !~ /^\S+$/) {
    $log->logcroak("Invalid sample ID '$sample_id' at line $line\n");
  }

  return $sample_id;
}

sub validate_beadchip {
  my ($chip, $line) = @_;

  unless (defined $chip) {
    $log->logcroak("Missing beadchip number at line $line\n");
  }

  if ($chip !~ /^\d{10}$/) {
    $log->logcroak("Invalid beadchip number '$chip' at line $line\n");
  }

  return $chip;
}

sub validate_section {
  my ($section, $line) = @_;

  unless (defined $section) {
    $log->logcroak("Missing beadchip section at line $line\n");
  }

  if ($section !~ /^[A-Z]$/) {
    $log->logcroak("Invalid beadchip section '$section' at line $line\n");
  }

  return $section;
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
    $log->logcroak("Failed to find the $type file $pattern for sample '$id' under the sample-source directory");
  }
  elsif (scalar @matches == 1) {
    $sample->{$type} = $matches[0];
  }
  else {
    $log->logcroak("Found multiple $type files matching $pattern for sample '$id': [",
                   join(', ', @matches), "]");
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

  --analysis-dest   The data destination root collection for the analysis data
                    in iRODS. E.g. /archive/GAPI/exp/analysis
  --analysis-source The root directory of the analysis.
  --help            Display help.
  --logconf         A log4perl configuration file. Optional.
  --manifest        Tab-delimted chip loading manifest. Optional, defaults to
                    STDIN.
  --sample-dest     The data destination root collection for the sample data
                    in iRODS. E.g. /archive/GAPI/exp/infinium
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
