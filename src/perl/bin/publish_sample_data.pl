#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use File::Basename;
use File::Find;
use Getopt::Long;
use Log::Log4perl qw(:easy);;
use Net::LDAP;
use Pod::Usage;
use URI;

use WTSI::Genotyping qw(make_warehouse_metadata
                        make_infinium_metadata
                        make_file_metadata
                        make_creation_metadata
                        publish_idat_files
                        publish_gtc_files);

use WTSI::Genotyping::iRODS qw(collect_files
                               collect_dirs
                               modified_between);


use WTSI::Genotyping::Database::Infinium;
use WTSI::Genotyping::Database::Warehouse;


my $embedded_conf = q(
   log4perl.logger.verbose        = DEBUG, A1
   log4perl.logger.quiet          = DEBUG, A2

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


our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 7;

run() unless caller();

sub run {
  my $config;
  my $days;
  my $days_ago;
  my $log4perl_config;
  my $publish_dest;
  my $source;
  my $type;
  my $verbose;

  GetOptions('config=s'   => \$config,
             'days=i'     => \$days,
             'days-ago=i' => \$days_ago,
             'dest=s'     => \$publish_dest,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
             'source=s'   => \$source,
             'type=s'     => \$type,
             'verbose'    => \$verbose,);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }
  unless ($source) {
    pod2usage(-msg => "A --source argument is required\n",
              -exitval => 2);
  }
  unless ($type) {
    pod2usage(-msg => "A --type argument is required\n",
              -exitval => 2);
  }
  unless ($type =~ m{^idat$}msxi or $type =~ m{^gtc$}msxi) {
    pod2usage(-msg => "Invalid --type '$type'; expected one of [gtc, idat]\n",
              -exitval => 2);
  }

  $config ||= $DEFAULT_INI;
  $days ||= $DEFAULT_DAYS;
  $days_ago ||= 0;
  $type = lc($type);

  my $log;

  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.publish');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    if ($verbose) {
      $log = Log::Log4perl->get_logger('verbose');
    }
    else {
      $log = Log::Log4perl->get_logger('quiet');
    }
  }

  my $now = DateTime->now();

  my $end;
  if ($days_ago > 0) {
    $end = DateTime->from_epoch
      (epoch => $now->epoch())->subtract(days => $days_ago);
  }
  else {
    $end = $now;
  }

  my $begin = DateTime->from_epoch
    (epoch => $end->epoch())->subtract(days => $days);

  $log->info("Publishing '$type' from '$source' to '$publish_dest'",
             " last modified between ", $begin->iso8601,
             " and ", $end->iso8601);

  my $file_test = modified_between($begin->epoch(), $end->epoch());
  my $file_regex = qr{.($type)$}msxi;
  my $source_dir = abs_path($source);
  my $relative_depth = 2;

  my $ifdb = WTSI::Genotyping::Database::Infinium->new
    (name   => 'infinium',
     inifile =>  $config)->connect(RaiseError => 1);

  my $ssdb = WTSI::Genotyping::Database::Warehouse->new
    (name   => 'sequencescape_warehouse',
     inifile =>  $config)->connect(RaiseError => 1);

  my $uid = `whoami`;
  chomp($uid);

  my $publisher_uri = URI->new("ldap:");
  $publisher_uri->host('ldap.internal.sanger.ac.uk');
  $publisher_uri->dn('ou=people,dc=sanger,dc=ac,dc=uk');
  $publisher_uri->attributes('title');
  $publisher_uri->scope('sub');
  $publisher_uri->filter("(uid=$uid)");

  my $ldap = Net::LDAP->new($publisher_uri->host) or
    $log->logcroak("LDAP connection failed: ", $@);
  my $name = authorize($publisher_uri, $ldap, $log);

  $log->logcroak("Failed to find $uid in LDAP") unless $name;
  $log->info("Publishing from '$source' to '$publish_dest' as ", $name);

  my @files;
  foreach my $dir (collect_dirs($source_dir, $file_test, $relative_depth)) {
    $log->debug("Checking directory '$dir'");
    my @found = collect_files($dir, $file_test, $relative_depth, $file_regex);
    $log->debug("Found " . scalar @found . " matching items in '$dir'");
    push(@files, @found);
  }

  # The above contains dupes due to the 2-level processing. Remove them.
  my @unique;
  my %seen;
  foreach my $file (@files) {
    if (! $seen{$file}) {
      push(@unique, $file);
      $seen{$file}++;
    }
  }

  if ($type eq 'idat') {
    publish_idat_files(\@unique, $publish_dest, $publisher_uri,
                       $ifdb, $ssdb, $now);
  }
  elsif ($type eq 'gtc') {
    publish_gtc_files(\@unique, $publish_dest, $publisher_uri,
                      $ifdb, $ssdb, $now);
  }
  else {
    $log->logcroak("Unable to publish unknown data type '$type'");
  }

  return 0;
}

sub authorize {
  my ($uri, $ldap, $log) = @_;

  my $msg = $ldap->bind;
  $msg->code && $log->logcroak($msg->error);

  $msg = $ldap->search(base   => "ou=people,dc=sanger,dc=ac,dc=uk",
                       filter => $uri->filter);
  $msg->code && $log->logcroak($msg->error);

  my ($name) = ($msg->entries)[0]->get('cn');

  $ldap->unbind;

  return $name;
}


__END__

=head1 NAME

publish_sample_data

=head1 SYNOPSIS

publish_sample_data [--config <database .ini file>] \
   [--days-ago <n>] [--days <n>] \
   --source <directory> --dest <irods collection> \
   --type <data type>

Options:

  --config    Load database configuration from a user-defined .ini file.
              Optional, defaults to $HOME/.npg/genotyping.ini
  --days-ago  The number of days ago that the publication window ends.
              Optional, defaults to zero (the current day).
  --days      The number of days in the publication window, ending at
              the day given by the --days-ago argument. Any sample data
              modified during this period will be considered
              for publication. Optional, defaults to 7 days.
  --dest      The data destination root collection in iRODS.
  --help      Display help.
  --logconf   A log4perl configuration file. Optional.
  --source    The root directory to search for sample data.
  --type      The data type to publish. One of [idat, gtc].
  --verbose   Print messages while processing. Optional.

=head1 DESCRIPTION

Searches a directory recursively for idat or GTC sample data files
that have been modified within the n days prior to a specific time.
Any files identified are published to iRODS with metadata obtained from
LIMS.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
