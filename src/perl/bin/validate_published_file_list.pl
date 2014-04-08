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


# useful irods methods: md5sum hash_path

my $embedded_conf = q(
   log4perl.logger.npg.irods.publish = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
);

run() unless caller();

sub run {
  my $config;
  my $debug;
  my $log4perl_config;
  my $publish_dest;
  my $type;
  my $verbose;

  GetOptions('config=s'   => \$config,
             'debug'      => \$debug,
             'dest=s'     => \$publish_dest,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
             'verbose'    => \$verbose);

  unless ($publish_dest) {
    pod2usage(-msg => "A --dest argument is required\n",
              -exitval => 2);
  }
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

  my $irods =  WTSI::NPG::iRODS->new;

  print "Created irods object\n";

  my @files = <>;
  @files = uniq(@files);
  print "Read ".@files." distinct file paths.\n";
  my %filenames;
  foreach my $file (@files) {
    chomp($file);
    my ($filename, $directories, $suffix) = fileparse($file);
    $filenames{$file} = $filename;
  }

  my ($invalid_file, $invalid_object, $invalid_meta, $invalid_md5) = (0,0,0,0);

  my $i = 0;
  foreach my $file (@files) {
      chomp $file;
      if (! -e $file) { 
	  $log->logwarn("Input path '$file' does not exist!");
	  $invalid_file++;
      }
      my $md5 = $irods->md5sum($file);
      #print "$file\t$md5\n";
      my ($filename, $directories, $suffix) = fileparse($file);
      unless ($publish_dest =~ /\/$/) { $publish_dest .= '/'; }
      my $iPath = $publish_dest.$irods->hash_path($file).'/'.$filename;
      $i++;
      print "$i $iPath\n";
      my $listing = $irods->list_object($iPath);
      if (!$listing) {
	  $log->logwarn("Cannot read iRODS path $iPath\n");
	  $invalid_object++;
	  next;
      }
      my $valid_meta = $irods->validate_checksum_metadata($iPath);
      if (! $valid_meta) {
	  $invalid_meta++;
      }
      my $irods_md5 = $irods->calculate_checksum($iPath);
      if ($md5 ne $irods_md5) {
	  $log->logwarn("Original and iRODS md5 checksums do not match for $file\n");
	  $invalid_md5++;
      }

  }
  print "Total files processed: ".@files."\n";
  print "Invalid input file path: $invalid_file\n";
  print "Invalid iRODS object path: $invalid_object\n";
  print "Invalid iRODS metadata checksum: $invalid_meta\n";
  print "Mismatched original and iRODS md5: $invalid_md5\n";

}


__END__

=head1 NAME

validate_published_file_list

=head1 SYNOPSIS

validate_published_file_list [--config <database .ini file>] \
   --dest <irods collection> < <files>

Options:

  --config      Load database configuration from a user-defined .ini file.
                Optional, defaults to $HOME/.npg/genotyping.ini
  --dest        The data destination root collection in iRODS.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --verbose     Print messages while processing. Optional.

=head1 DESCRIPTION

Attempts to find files named on STDIN in iRODS and validates their MD5 checksums. Finds if checksum stored in iRODS metadata, checksum of iRODS data object, and checksum of original file are identical; reports any discrepancies.

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
