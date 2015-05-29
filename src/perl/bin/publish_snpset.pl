#!/usr/bin/env perl

package main;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use Getopt::Long;
use Log::Log4perl;
use Log::Log4perl::Level;
use Moose;
use Pod::Usage;

use WTSI::NPG::iRODS;
use WTSI::NPG::Genotyping::SNPSetPublisher;

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

run() unless caller();
sub run {
  my $debug;
  my $log4perl_config;
  my $snpset;
  my $platform;
  my $publish_dest;
  my @references;
  my $source;
  my $verbose;

  GetOptions('debug'             => \$debug,
             'dest=s'            => \$publish_dest,
             'help'              => sub { pod2usage(-verbose => 2,
                                                    -exitval => 0) },
             'logconf=s'         => \$log4perl_config,
             'snpset-name=s'     => \$snpset,
             'snpset-platform=s' => \$platform,
             'reference-name=s'  => \@references,
             'source=s'          => \$source,
             'verbose'           => \$verbose);

  unless ($snpset) {
    pod2usage(-msg => "A --snpset-name argument is required\n",
              -exitval => 2);
  }

  unless ($platform) {
    pod2usage(-msg => "A --snpset-platform argument is required\n",
              -exitval => 2);
  }

  unless (@references) {
    pod2usage(-msg => "A --reference-name argument is required\n",
              -exitval => 2);
  }

  unless ($source) {
    pod2usage(-msg => "A --source argument is required\n",
              -exitval => 2);
  }

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

  my $log;

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

  my $irods = WTSI::NPG::iRODS->new;
  $irods->logger($log);

  my $source_file = abs_path($source);
  $log->info("Publishing from '$source_file' to '$publish_dest' $platform ",
             "SNP set file");

  my $publisher = WTSI::NPG::Genotyping::SNPSetPublisher->new
    (file_name        => $source_file,
     publication_time => DateTime->now,
     reference_names  => \@references,
     snpset_name      => $snpset,
     snpset_platform  => $platform,
     logger           => $log);

  my $rods_path = $publisher->publish($publish_dest);

  $log->info("Published SNP set '$snpset' to ", $rods_path);
}

__END__

=head1 NAME

publish_snpset

=head1 SYNOPSIS

publish_snpset --snpset-name <name> --snpset-platform <platform> \
   --reference-name <reference> --source <file path> \
   --dest <irods collection> [--verbose]

Options:

  --dest            The data destination root collection in iRODS.
  --help            Display help.
  --logconf         A log4perl configuration file. Optional.
  --reference-name  The symbolic name of the reference genome to which
                    these SNP coordinates apply. More than one name may
                    be supplied.
  --snpset-name     The symbolic name of the SNP set. E.g. W30467.
  --snpset-platform The genotyping platform on which this set of SNPs has
                    been used. One of [sequenom, fluidigm].
  --source          The SNP set file to publish.
  --verbose         Print messages while processing. Optional.

=head1 DESCRIPTION

Publishes a SNP set manifest to iRODS. This means inserting it and
adding metadata. In addition to the standard metadata (creation
timestamp, MD5 sum etc.) the following are added:

  <snpset platform>_plex = <snpset platform value>
  reference_name         = <reference name value> (may be repeated)

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

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
