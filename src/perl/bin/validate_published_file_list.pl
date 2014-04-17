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


# descriptions of iRODS upload status
our @DESCRIPTIONS = qw/UPLOAD_OK SOURCE_MISSING DEST_MISSING 
                       METADATA_MD5_ERROR SOURCE_MD5_ERROR/;

unless (caller()) {
    my $status = run();
    exit($status);
}

sub run {
  my $debug;
  my $log4perl_config;
  my $output;
  my $publish_dest;
  my $verbose;

  # parse command line options
  GetOptions('debug'      => \$debug,
             'dest=s'     => \$publish_dest,
             'help'       => sub { pod2usage(-verbose => 2, -exitval => 0) },
             'logconf=s'  => \$log4perl_config,
	     'output=s'   => \$output,
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
    } elsif ($debug) {
      $log->level($DEBUG);
    }
  }

  my @files = <>;
  foreach my $file (@files) { chomp($file); }
  $log->debug("Validating iRODS upload, output $output");
  my $valtotal = validate(\@files, $publish_dest, $output);
  $log->info("Validated ", $valtotal, " of ", scalar @files, " files");
  if ($valtotal == scalar @files) { return 0; }
  else { return 1; }
}

sub validate {
    my ($filesRef, $publish_dest, $out_path) = @_; 
    my $log = Log::Log4perl->get_logger('npg.irods.validate');
    $log->debug("Starting validation for destination $publish_dest");
    my $irods =  WTSI::NPG::iRODS->new;
    $log->debug("Created irods object");
    my $out;
    my $num_valid = 0;
    if (!$out_path) {
	$out = 0; # no output
    } elsif ($out_path eq '-') { 
	$out = *STDOUT;
    } else {   
	$out = open ">", $out_path || 
	    $log->logconfess("Cannot open output '$out_path'");
    }
    my @header = qw/Source Destination Status Description/;
    if ($out) { print $out join("\t", @header)."\n";  }
    foreach my $file (@{$filesRef}) {
	my $result = _validate_file($irods, $file, $publish_dest); 
	my ($file, $irods_file, $status) = @{$result};
	if ($status == 0) { $num_valid++; }
	my @fields = ($file, $irods_file, $status, $DESCRIPTIONS[$status]);
	if ($out) { print $out join("\t", @fields)."\n"; }
    }
    if ($out && $out_path ne '-') {
	close $out || $log->logconfess("Cannot close output '$out_path'");
    }
    return $num_valid;
}

sub _validate_file {
  my ($irods, $file, $publish_dest) = @_;
  my $status = 0;
  my $log = Log::Log4perl->get_logger('npg.irods.validate');
  # gather information on source and destination files, if available
  my ($file_exists, $listing, $valid_meta, $file_md5, $irods_md5);
  unless ($publish_dest =~ /\/$/) { $publish_dest .= '/'; }
  if (-e $file) { 
      $file_exists = 1;
      $file_md5 = $irods->md5sum($file); 
      $publish_dest .= $irods->hash_path($file).'/'.fileparse($file);
      $listing = eval { $irods->list_object($publish_dest) };
      if ($listing) {
	  $valid_meta = $irods->validate_checksum_metadata($publish_dest);
	  $irods_md5 = $irods->calculate_checksum($publish_dest);
      }
  } else {
      $publish_dest = 'UNKNOWN';
  }
  # assign status to file
  if (! $file_exists) { 
    $status = 1;
  } elsif (! $listing) {
    $status = 2;
  } elsif (! $valid_meta) {
    $status = 3;
  } elsif ($file_md5 ne $irods_md5) {
    $status = 4;
  }
  $log->debug("Attempted to validate file ", $file, " status ", $status);
  return [$file, $publish_dest, $status];
}



__END__

=head1 NAME

validate_published_file_list

=head1 SYNOPSIS

validate_published_file_list [--config <database .ini file>] \
   --dest <irods collection> < <files>

Options:

  --dest         The data destination root collection in iRODS. Required.
  --help         Display help.
  --logconf      A log4perl configuration file. Optional.
  --output=PATH  PATH for output, or '-' for STDOUT. Optional.
  --verbose      Print messages while processing. Optional.

=head1 DESCRIPTION

Given a list of 'source' file paths on STDIN, check if there are valid 
'destination' copies of the files in iRODS. Validation fails if the source or 
destination does not exist; if the MD5 checksums of the source and destination 
files are not identical; or if the MD5 checksum of the destination file does 
not match the value in iRODS metadata.

This script is intended for use immediately after an upload to iRODS to verify
success, particularly when many files have been uploaded at once using 
publish_infinium_file_list.pl.

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
