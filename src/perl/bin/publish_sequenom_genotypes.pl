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
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Genotyping::Database::Sequenom;
use WTSI::NPG::Genotyping::Publication qw(publish_sequenom_files);
use WTSI::NPG::iRODS;
use WTSI::NPG::Metadata qw(make_sample_metadata
                           make_md5_metadata
                           make_type_metadata
                           make_creation_metadata);
use WTSI::NPG::Publication qw(get_wtsi_uri
                              get_publisher_uri
                              get_publisher_name);

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

our $DEFAULT_INI = $ENV{HOME} . "/.npg/genotyping.ini";
our $DEFAULT_DAYS = 7;

run() unless caller();

sub run {
  my $config;
  my $days;
  my $days_ago;
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $verbose;

  GetOptions('config=s'    => \$config,
             'days=i'      => \$days,
             'days-ago=i'  => \$days_ago,
             'debug'       => \$debug,
             'dest=s'      => \$publish_dest,
             'help'        => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'   => \$log4perl_config,
             'verbose'     => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

  $config ||= $DEFAULT_INI;
  $days ||= $DEFAULT_DAYS;
  $days_ago ||= 0;

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

  $log->info("Publishing Sequenom results to '$publish_dest'",
             " finished between ", $begin->iso8601,
             " and ", $end->iso8601);

   my $sqdb = WTSI::NPG::Genotyping::Database::Sequenom->new
     (name    => 'mspec2',
      inifile => $config)->connect(RaiseError => 1);

  my $uid = `whoami`;
  chomp($uid);

  my $creator_uri = get_wtsi_uri();
  my $publisher_uri = get_publisher_uri($uid);
  my $name = get_publisher_name($publisher_uri);

  $log->info("Publishing to '$publish_dest' as ", $name);

  my $plate_names = $sqdb->find_finished_plate_names($begin, $end);
  $log->debug("Found " . scalar @$plate_names . " finished plates");

  foreach my $plate_name (@$plate_names) {
    my $results = $sqdb->find_plate_results($plate_name);

    publish_sequenom_files($results, $creator_uri, $publish_dest,
                           $publisher_uri, $now);
  }

  return 0;
}

