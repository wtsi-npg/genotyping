#!/software/bin/perl

use utf8;

package main;

use strict;
use warnings;
use Cwd qw(abs_path);
use DateTime;
use File::Basename;
use Getopt::Long;
use List::AllUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::Infinium;
use WTSI::NPG::Genotyping::Infinium::Publisher;

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
  my $debug;
  my $dry_run;
  my $log4perl_config;
  my $output;
  my $publish_dest;
  my $type;
  my $validate,
  my $verbose;

  GetOptions('config=s'   => \$config,
             'debug'      => \$debug,
             'dest=s'     => \$publish_dest,
             'dry-run'    => \$dry_run,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
	     'output=s'   => \$output,
	     'validate'   => \$validate,
             'verbose'    => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

  $config ||= $DEFAULT_INI;
  $type = lc($type);
  if ($output && $output ne '-') { $output = abs_path($output); } 

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

  my $now = DateTime->now;

  my $ifdb = WTSI::NPG::Genotyping::Database::Infinium->new
    (name    => 'infinium',
     inifile => $config,
     logger  => $log)->connect(RaiseError => 1);

  my $ssdb = WTSI::NPG::Database::Warehouse->new
    (name    => 'sequencescape_warehouse',
     inifile => $config,
     logger  => $log)->connect(RaiseError           => 1,
                               mysql_enable_utf8    => 1,
                               mysql_auto_reconnect => 1);
  my @files = <>;
  foreach my $file (@files) {
    chomp($file);
    my ($filename, $directories, $suffix) = fileparse($file, $type);
  }

  @files = uniq(@files);
  $log->debug("Found ", scalar @files, " unique files");

  my $publisher = WTSI::NPG::Genotyping::Infinium::Publisher->new
    (publication_time => $now,
     data_files       => \@files,
     infinium_db      => $ifdb,
     logger           => $log);

  if ($dry_run && $validate) {
      $log->logcroak("Can specify at most one of --dry-run and --validate");
  } elsif ($dry_run) {
      $log->debug("Starting dry run");
      $publisher->dry_run($publish_dest, $output);
  } elsif ($validate) {
      $publisher->validate($publish_dest, $output);
  } else {
      $log->debug("Starting publication");
      $publisher->publish($publish_dest);
  }

  return 0;
}

__END__

=head1 NAME

publish_infinium_file_list

=head1 SYNOPSIS

publish_infinium_file_list [--config <database .ini file>] \
   --dest <irods collection> [--dry-run] [--help] [--logconf <log4perl config>]
   [--output <destination file or -> ] [--validate] [--verbose] < <files>

Options:

  --config      Load database configuration from a user-defined .ini file.
                Optional, defaults to $HOME/.npg/genotyping.ini
  --dest        The data destination root collection in iRODS.
  --dry-run     Attempt to determine if inputs are valid. Does *not* actually
                publish any files. Not compatible with --validate. Output is 
                a list of publishable files, one per line.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --output      In --dry-run or --validate mode, write output to the 
                given file, or '-' for STDOUT. If this option is omitted, 
                output is not written.
  --validate    Validate upload of files which have already been published 
                to iRODS. Not compatible with --dry-run. Output is tab 
                delimited text, giving source, destination, status code, and 
                status description for each input.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Default behaviour is to publishe files named on STDIN to iRODS with metadata 
obtained from LIMS. Can also perform a dry run to check if metadata exists 
in the LIMS, or validate that files have been uploaded successfully.

=head1 METHODS

None

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012-2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
