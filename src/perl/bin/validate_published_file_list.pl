#!/software/bin/perl

use utf8;

use strict;
use warnings;
use File::Basename;
use Getopt::Long;
use List::AllUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;
use WTSI::NPG::iRODS;

my $embedded_conf = q(
   log4perl.logger.npg.irods.validate = WARN, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

unless (caller()) {
    my $status = run();
    exit($status);
}

sub run {
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $silent;
  my $type;
  my $verbose;

  # parse command line options

  GetOptions('debug'      => \$debug,
             'dest=s'     => \$publish_dest,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
             'silent'     => \$silent,
	     'verbose'    => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }

  # set up log

  my $log;
  if ($log4perl_config) {
    Log::Log4perl::init($log4perl_config);
    $log = Log::Log4perl->get_logger('npg.irods.validate');
  }
  else {
    Log::Log4perl::init(\$embedded_conf);
    $log = Log::Log4perl->get_logger('npg.irods.validate');

    if ($verbose) {
      $log->level($INFO);
    }
    elsif ($debug) {
      $log->level($DEBUG);
    }
  }

  # create iRODS object and read paths

  my $irods =  WTSI::NPG::iRODS->new;
  $log->debug("Created irods object");

  my @files = <>;
  @files = uniq(@files);
  $log->debug("Read ".@files." distinct file paths.");
  my %filenames;
  foreach my $file (@files) {
    chomp($file);
    my ($filename, $directories, $suffix) = fileparse($file);
    $filenames{$file} = $filename;
  }

  # process input file paths

  my ($invalid_file, $invalid_object, $invalid_meta, $invalid_md5) = (0,0,0,0);
  my $i = 0;
  my $total = @files;
  foreach my $file (@files) {
      chomp $file;
      $i++;
      my $prefix =  "File $i of $total: $file";
      if (! -e $file) { 
	  $log->logwarn("FAIL: $prefix: Input path '$file' does not exist!");
	  $invalid_file++;
	  next;
      }
      my $md5 = $irods->md5sum($file);
      my ($filename, $directories, $suffix) = fileparse($file);
      unless ($publish_dest =~ /\/$/) { $publish_dest .= '/'; }
      my $iPath = $publish_dest.$irods->hash_path($file).'/'.$filename;
      my $listing = $irods->list_object($iPath);
      if (!$listing) {
	  $log->logwarn("FAIL: $prefix: Cannot read iRODS path $iPath");
	  $invalid_object++;
	  next;
      }
      my $valid_meta = $irods->validate_checksum_metadata($iPath);
      if (! $valid_meta) {
	  $log->logwarn("FAIL: $prefix: Invalid checksum in metadata for $iPath");
	  $invalid_meta++;
	  next;
      }
      my $irods_md5 = $irods->calculate_checksum($iPath);
      if ($md5 ne $irods_md5) {
	  $log->logwarn("FAIL: $prefix: Original and iRODS md5 checksums do not match for $file");
	  $invalid_md5++;
	  next;
      }
      $log->info("OK: $prefix validated");
  }
  $log->info("Total files processed: $total");
  $log->info("Invalid input file path: $invalid_file");
  $log->info("Invalid iRODS object path: $invalid_object");
  $log->info("Invalid iRODS metadata checksum: $invalid_meta");
  $log->info("Mismatched original and iRODS md5: $invalid_md5");
  my $errors = $invalid_file + $invalid_object + $invalid_meta + $invalid_md5;
  if ($errors == 0) {
      my $msg = "$total input files processed, no errors found.";
      $log->info($msg);
      if (!$silent) { print "Finished: $msg\n"; }
      return 0;
  } else {
      my $msg = "Errors found in $errors of $total input files.";
      $log->info($msg);
      if (!$silent) { print "Finished: $msg\n"; }
      return 1;
  }
}


__END__

=head1 NAME

validate_published_file_list

=head1 SYNOPSIS

validate_published_file_list [--config <database .ini file>] \
   --dest <irods collection> < <files>

Options:

  --dest        The data destination root collection in iRODS.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --silent      Do not print status message on completion.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Attempts to find files named on STDIN in iRODS and validates their MD5 checksums. Finds if checksum stored in iRODS metadata, checksum of iRODS data object, and checksum of original file are identical. Exit status is 1 if any discrepancies are found, 0 otherwise.

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
