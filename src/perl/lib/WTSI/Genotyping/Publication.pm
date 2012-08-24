use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;


=head2 publish_idat_files

  Arg [1]    : arrayref of IDAT file names
  Arg [2]    : string publication destination in iRODS
  Arg [3]    : URI object of publisher (typically an LDAP URI)
  Arg [4]    : Infinium database handle
  Arg [5]    : SequenceScape Warehouse database handle
  Arg [6]    : DateTime object of publication
  Example    : my $n = publish_idat_files(\@files, '/my/project',
                                          $publisher_uri,
                                          $ifdb, $ssdb, $now, $log);
  Description: Publishes IDAT file pairs to iRODS with attendant metadata.
               Skips any files where consent is absent. Republishes any
               file that is already published, but whose checksum has
               changed.
  Returntype : integer number of files published
  Caller     : general

=cut

sub publish_idat_files {
  my ($files, $publish_dest, $publisher_uri, $ifdb, $ssdb, $time) = @_;

  my $log = Log::Log4perl->get_logger('genotyping');
  my $paired = paired_idat_files($files, $log);
  my $pairs = scalar @$paired;
  my $total = $pairs * 2;
  my $published = 0;

  $log->debug("Publishing $pairs pairs of idat files");

  foreach my $pair (@$paired) {
    my ($red) = grep { m{Red}msxi } @$pair;
    my ($grn) = grep { m{Grn}msxi } @$pair;

    my ($basename, $dir, $suffix) = fileparse($red);

    $log->debug("Finding the sample for '$red' in the Infinium LIMS");
    my $if_sample = $ifdb->find_scanned_sample($basename);

    if ($if_sample) {
      my @meta;
      push(@meta, make_warehouse_metadata($if_sample, $ssdb));
      push(@meta, make_infinium_metadata($if_sample));

      foreach my $file ($red, $grn) {
        eval {
          publish_file($file, \@meta, $publish_dest, $publisher_uri->as_string,
                       $time, $log);
          ++$published;
        };

        if ($@) {
          $log->error("Failed to publish '$red' to '$publish_dest': ", $@);
        }
      }
    }
    else {
     $log->warn("Failed to find the sample for '$red' in the Infinium LIMS");
    }
  }

  $log->info("Published $published/$total idat files to '$publish_dest'");

  return $published;
}


=head2 publish_gtc_files

  Arg [1]    : arrayref of GTC file names
  Arg [2]    : string publication destination in iRODS
  Arg [3]    : URI object of publisher (typically an LDAP URI)
  Arg [4]    : Infinium database handle
  Arg [5]    : SequenceScape Warehouse database handle
  Arg [6]    : DateTime object of publication
  Example    : my $n = publish_idat_files(\@files, '/my/project',
                                          $publisher_uri,
                                          $ifdb, $ssdb, $now, $log);
  Description: Publishes GTC files to iRODS with attendant metadata.
               Skips any files where consent is absent. Republishes any
               file that is already published, but whose checksum has
               changed.
  Returntype : integer number of files published
  Caller     : general

=cut
sub publish_gtc_files {
  my ($files, $publish_dest, $publisher_uri, $ifdb, $ssdb, $time) = @_;

  my $log = Log::Log4perl->get_logger('genotyping');
  my $total = scalar @$files;
  my $published = 0;

  $log->debug("Publishing $total of GTC files");

  foreach my $file (@$files) {
    my ($basename, $dir, $suffix) = fileparse($file);

    $log->debug("Finding the sample for '$file' in the Infinium LIMS");
    my $if_sample = $ifdb->find_called_sample($basename);

    if ($if_sample) {
      my @meta;
      push(@meta, make_warehouse_metadata($if_sample, $ssdb));
      push(@meta, make_infinium_metadata($if_sample));

      eval {
        publish_file($file, \@meta, $publish_dest, $publisher_uri->as_string,
                     $time, $log);
        ++$published;
      };

      if ($@) {
        $log->error("Failed to publish '$file' to '$publish_dest': ", $@);
      }
    }
    else {
      $log->warn("Failed to find the sample for '$file' in the Infinium LIMS");
    }
  }

  $log->info("Published $published/$total GTC files to '$publish_dest'");

  return $published;
}

sub publish_file {
  my ($file, $sample_meta, $publish_dest, $publisher, $time, $log) = @_;

  my $basename = fileparse($file);
  my $target = $publish_dest . '/' . $basename;

  my @meta = @$sample_meta;

  if (has_consent(@meta)) {
    if (list_object($target)) {
      if (checksum_object($target)) {
        $log->info("Skipping publishing $target because checksum is unchanged");
      }
      else {
        $log->info("Republishing $target because checksum is changed");
        $target = add_object($file, $target);
        push(@meta, make_modification_metadata($time));
      }
    }
    else {
      $log->info("Publishing $target");
      push(@meta, make_creation_metadata($time, $publisher));
      $target = add_object($file, $target);
    }

    my %current_meta = get_object_meta($target);

    push(@meta, make_file_metadata($file, '.idat', '.gtc'));

    foreach my $elt (@meta) {
      my ($key, $value, $units) = @$elt;
      unless (meta_exists($key, $value, %current_meta)) {
        add_object_meta($target, $key, $value, $units);
      }
    }
  }
  else {
    $log->info("Skipping publishing $target because no consent was given");
  }

  return $target;
}

sub paired_idat_files {
  my ($files, $log) = @_;

  my %names;

  # Determine unique 
  foreach my $file (@$files) {
    my ($stem, $colour, $suffix) = $file =~ m{^(\S+)_(Red|Grn)(.idat)$}msxi;

    unless ($stem && $colour && $suffix) {
      $log->warn("Found a non-idat file while sorting idat files: '$file'");
    }

    if (exists $names{$stem}) {
      push(@{$names{$stem}}, $file);
    }
    else {
      $names{$stem} = [$file];
    }
  }

  my @paired;
  foreach my $stem (sort keys %names) {
    if (scalar @{$names{$stem}} == 2) {
      push(@paired, $names{$stem});
    }
    else {
      $log->warn("Ignoring an unpaired idat file with name stem '$stem'");
    }
  }

  return \@paired;
}

1;

__END__

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
