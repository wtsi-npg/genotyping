
use utf8;

package WTSI::NPG::Genotyping::Infinium::Publisher;

use List::AllUtils qw(sum);
use File::Basename;
use Moose;
use WTSI::NPG::Genotyping::Infinium::InfiniumDataObject;
use WTSI::NPG::Genotyping::Infinium::ResultSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::Publisher;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable', 'WTSI::NPG::Annotator',
  'WTSI::NPG::Genotyping::Annotator';

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

has 'publication_time' =>
  (is       => 'ro',
   isa      => 'DateTime',
   required => 1);

has 'data_files' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   required => 1);

has 'resultsets' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Genotyping::Infinium::ResultSet]',
   required => 1,
   lazy     => 1,
   builder  => '_build_resultsets');

has 'infinium_db' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::Database::Infinium',
   required => 1);

has 'ss_warehouse_db' =>
  (is       => 'ro',
   # isa      => 'WTSI::NPG::Database::Warehouse',
   isa      => 'Object',
   required => 1);

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);

}

sub dry_run {
  my ($self, $publish_dest, $output) = @_;
  return $self->dry_run_samples($publish_dest, $output);
}

sub dry_run_samples {
  my ($self, $publish_dest, $output) = @_;
  $publish_dest = $self->_validate_publish_dest($publish_dest);
  # find out how many files are ready to be published:
  # ie. source exists, and data exists in Infinium LIMS
  my $num_ready = 0;
  my $total = scalar @{$self->resultsets} * 3;
  $self->info("Starting dry run for $total Infinium files");
  my $out = $self->_open_output_handle($output);
  foreach my $resultset (@{$self->resultsets}) {
    my $idat_ok = $self->_dryrun_idat_files($resultset);
    my $gtc_ok = $self->_dryrun_gtc_file($resultset);
    if ($idat_ok + $gtc_ok == 3) {
      foreach my $file ($resultset->grn_idat_file,
                        $resultset->red_idat_file,
                        $resultset->gtc_file) {
        $num_ready++;
        if ($out) { print $out $file."\n"; }
      }
    }
  }
  if ($out && $output ne '-') {
    close $out || $self->logcroak("Cannot close output '$output'");
  }
  $self->info("$num_ready of $total Infinium files are ready to publish.");
  return $num_ready;
}

sub publish {
  my ($self, $publish_dest) = @_;
  return $self->publish_samples($publish_dest);
}

sub publish_samples {
  my ($self, $publish_dest) = @_;

  $publish_dest = $self->_validate_publish_dest($publish_dest);

  my $num_published = 0;
  my $total = sum map { $_->size } @{$self->resultsets};
  $total ||= 0;

  $self->debug("Starting to publish $total Infinium files");

  foreach my $resultset (@{$self->resultsets}) {
    $num_published += $self->_publish_idat_files($resultset, $publish_dest);

    unless ($resultset->is_methylation) {
      $num_published += $self->_publish_gtc_file($resultset, $publish_dest);
    }
  }

  $self->info("Published $num_published/$total Infinium files ",
              "to '$publish_dest'");

  return $num_published;
}

sub validate {
  my ($self, $publish_dest, $output) = @_;
  return $self->validate_samples($publish_dest, $output);
}

sub validate_samples {
  my ($self, $publish_dest, $output) = @_;
  $publish_dest = $self->_validate_publish_dest($publish_dest);
  my $total = scalar @{$self->resultsets} * 3;
  my $num_valid = 0;
  $self->debug("Starting to validate $total Infinium files");
  my $out = $self->_open_output_handle($output);
  my @descriptions = _descriptions();
  foreach my $resultset (@{$self->resultsets}) {
    foreach my $file ($resultset->grn_idat_file,
                      $resultset->red_idat_file,
                      $resultset->gtc_file) {
      my ($file, $irods_file, $status) =
        @{$self->_validate_file($file, $publish_dest)};
      if ($status == 0) { $num_valid++; }
      my @fields = ($file, $irods_file, $status, $descriptions[$status]);
      if ($out) { print $out join("\t", @fields)."\n"; }
    }
  }
  if ($out && $output ne '-') {
      close $out || $self->logconfess("Cannot close output '$output'");
  }
  return $num_valid;
}

sub _dryrun_gtc_file {
  my ($self, $resultset) = @_;
  my $gtc_file = $resultset->gtc_file;
  my ($vol, $dirs, $gtc_filename) = File::Spec->splitpath($gtc_file);
  my $if_sample;
  my $num_ready = 0;
  if (!(-e $gtc_file)) {
    $self->warn("Input GTC file $gtc_file does not exist");
  } else {
    eval {
      $if_sample = $self->infinium_db->find_called_sample($gtc_filename);
    };
    if (!($if_sample) || $@) {
      $self->warn("Query error for GTC $gtc_filename in Infinium LIMS");
    } else {
      $self->debug("File $gtc_filename OK for publication");
      $num_ready = 1;
    }
  }
  return $num_ready;
}

sub _dryrun_idat_files {
  my ($self, $resultset) = @_;
  my $grn_file = $resultset->grn_idat_file;
  my $red_file = $resultset->red_idat_file;
  my $num_ready = 0;
  my $missing_files = 0;
  foreach my $file ($red_file, $grn_file) {
    if (!(-e $file)) {
      $self->warn("Input IDAT file $file does not exist");
      $missing_files = 1;
    }
  }
  my ($if_sample, $vol, $dirs, $grn_name, $red_name);
  ($vol, $dirs, $red_name) = File::Spec->splitpath($red_file);
  ($vol, $dirs, $grn_name) = File::Spec->splitpath($grn_file);
  eval {
    $if_sample = $self->infinium_db->find_scanned_sample($red_name);
  };
  if (!($if_sample) || $@) {
    $self->warn("Query error for IDAT $red_name in Infinium LIMS");
    $self->warn("Cannot publish IDAT $grn_name due to LIMS error");
  } elsif ($missing_files==0) {
    $self->debug("File $red_name OK for publication");
    $self->debug("File $grn_name OK for publication");
    $num_ready = 2;
  }
  return $num_ready;
}

sub _open_output_handle {
  my ($self, $output) = @_;
  my $out;
  if ($output) {
    if ($output eq '-') {
      $out = *STDOUT;
    } else {
      open $out, ">", $output ||
        $self->logcroak("Cannot open output '$output'");
    }
  }
  return $out;
}

sub _publish_gtc_file {
  my ($self, $resultset, $publish_dest) = @_;

  my $num_published = 0;
  my $gtc_file = $resultset->gtc_file;
  my ($vol, $dirs, $gtc_filename) = File::Spec->splitpath($gtc_file);

  $self->debug("Finding the sample for '$gtc_filename' in the Infinium LIMS");
  my $if_sample = $self->infinium_db->find_called_sample($gtc_filename);

  if ($if_sample) {
    eval {
      $self->_publish_file($gtc_file, $if_sample, $publish_dest);
      ++$num_published;
    };

    if ($@) {
      $self->error("Failed to publish '$gtc_file' to '$publish_dest': ", $@);
    }
    else {
      $self->info("Published '$gtc_file' to '$publish_dest'");
    }
  }
  else {
    $self->warn("Failed to find the sample for '$gtc_filename' ",
                "in the Infinium LIMS");
  }

  return $num_published;
}

sub _publish_idat_files {
  my ($self, $resultset, $publish_dest) = @_;

  my $num_published = 0;
  my $grn_file = $resultset->grn_idat_file;
  my $red_file = $resultset->red_idat_file;
  my ($vol, $dirs, $red_filename) = File::Spec->splitpath($red_file);

  $self->debug("Finding the sample for '$red_filename' in the Infinium LIMS");
  my $if_sample = $self->infinium_db->find_scanned_sample($red_filename);

  if ($if_sample) {
    foreach my $file ($grn_file, $red_file) {
      eval {
        $self->_publish_file($file, $if_sample, $publish_dest);
        ++$num_published;
      };

      if ($@) {
        $self->error("Failed to publish '$file' to '$publish_dest': ", $@);
      }
      else {
        $self->info("Published '$file' to '$publish_dest'");
      }
    }
  }
  else {
    $self->warn("Failed to find the sample for '$red_filename' ",
                "in the Infinium LIMS");
  }

  return $num_published;
}

sub _publish_file {
  my ($self, $filename, $if_sample, $publish_dest) = @_;

  my $publisher =
    WTSI::NPG::Publisher->new(irods         => $self->irods,
                              accountee_uid => $self->accountee_uid,
                              logger        => $self->logger);

  my @meta = $self->make_infinium_metadata($if_sample);
  my @fingerprint = $self->infinium_fingerprint(@meta);
  my $data_path = $publisher->publish_file($filename, \@fingerprint,
                                           $publish_dest,
                                           $self->publication_time);

  # Now that adding the secondary metadata is fast enough, we can
  # run it inline here, so that the data are available
  # immediately.
  my $obj = WTSI::NPG::Genotyping::Infinium::InfiniumDataObject->new
    ($self->irods, $data_path);
  $obj->update_secondary_metadata($self->ss_warehouse_db);

  return $data_path;
}

sub _build_resultsets {
  my ($self) = @_;

  my $filesets = $self->_build_filesets;

  my @resultsets;

  foreach my $beadchip (sort keys %$filesets) {
    $self->trace("Collating beadchip $beadchip");

  SECTION:
    foreach my $section (sort keys $filesets->{$beadchip}) {
      $self->trace("Collating section $section");

      my @fileset = @{$filesets->{$beadchip}{$section}};
      unless (scalar @fileset >= 2) {
        $self->warn("Failed to collate a resultset for beadchip ",
                    "'$beadchip' section '$section' because it did not ",
                    "contain at least 2 files: [",
                    join(", ", sort @fileset), "]");
        next SECTION;
      }

      my ($gtc, $red, $grn) = ('', '', '');

      foreach my $path (@fileset) {
        if ($path =~ m{_Red\.idat$}msi)    { $red = $path }
        elsif ($path =~ m{_Grn\.idat$}msi) { $grn = $path }
        elsif ($path =~ m{\.gtc}msi)       { $gtc = $path }
        else {
          $self->warn("Failed to collate a resultset for beadchip ",
                      "'$beadchip' section '$section' because it ",
                      "contained an expected file '$path'");
          next SECTION;
        }
      }

      if ($red && $grn) {
        $self->debug("Collating a new resultset for $beadchip $section");
        my $collated = 0;

        my @initargs = (beadchip         => $beadchip,
                        beadchip_section => $section,
                        red_idat_file    => $red,
                        grn_idat_file    => $grn);

        # Identify methylation studies; these do not have a GTC file
        my ($vol, $dirs, $red_filename) = File::Spec->splitpath($red);
        my $if_sample = $self->infinium_db->find_scanned_sample($red_filename);
        unless ($if_sample) {
          $self->logconfess("Failed to find a sample in the LIMS for ",
                            "'$red_filename'");
        }

        my $chip_design = $if_sample->{beadchip_design};
        unless ($chip_design) {
          $self->logconfess("Failed to find the chip design of beadchip '",
                            $beadchip, "' section '$section'");
        }

        my $is_methylation = $self->infinium_db->is_methylation_chip_design
          ($chip_design);

        push @initargs, is_methylation => $is_methylation;
        if ($is_methylation) {
          $collated = 1;
        }
        elsif ($gtc) {
          push @initargs, gtc_file => $gtc;
          $collated = 1;
        }

        if ($collated) {
          push @resultsets, WTSI::NPG::Genotyping::Infinium::ResultSet->new
            (@initargs);
        }
        else {
          my $desc = $is_methylation ? 'methylation beadchip ' : 'beadchip ';

          $self->warn("Failed to collate a resultset for ", $desc,
                      "'$beadchip' section '$section' because ",
                      "its file set was incomplete: ",
                      "[GTC: '$gtc', Red: '$red', Green: '$grn']");
        }
      }
    }
  }

  return \@resultsets;
}

sub _build_filesets {
  my ($self) = @_;

  # Each hash chain $filesets{beadchip}{section} points to an array
  # containing the names of the 2 (for methylation) or 3 files in the
  # set
  my %filesets;

  foreach my $path (sort  @{$self->data_files}) {
    my ($volume, $dirs, $filename) = File::Spec->splitpath($path);

    $self->trace("Preparing to collate '$filename' into a resultset");

    my ($beadchip, $section, $channel, $suffix) =
      $filename =~ m{^
                     (\d{10})        # beadchip
                     _(R\d{2}C\d{2}) # beadchip section
                     _?(Red|Grn)?    # channel (idat only)
                     \.(\S+)         # suffix
                     $}msxi;

    unless ($beadchip && $section && $suffix) {
      $self->warn("Failed to parse Infinium results filename '$filename'; ",
                  "ignoring it");
      next;
    }

    unless (exists $filesets{$beadchip}{$section}) {
      $filesets{$beadchip}{$section} = [];
    }

    push @{$filesets{$beadchip}{$section}}, $path;
  }

  return \%filesets;
}

sub _descriptions {
  # return a list of upload status descriptions
  my @descriptions = qw/UPLOAD_OK SOURCE_MISSING DEST_MISSING
                        METADATA_MD5_ERROR SOURCE_MD5_ERROR/;
  return @descriptions;
}

sub _validate_file {
  my ($self, $file, $publish_dest) = @_;
  # gather information on source and destination files, if available
  my ($file_exists, $listing, $valid_meta, $file_md5, $irods_md5);
  unless ($publish_dest =~ /\/$/) { $publish_dest .= '/'; }
  if (-e $file) {
      $file_exists = 1;
      $file_md5 = $self->irods->md5sum($file);
      $publish_dest .= $self->irods->hash_path($file).'/'.fileparse($file);
      $listing = eval { $self->irods->list_object($publish_dest) };
      if ($listing) {
        $valid_meta = eval {
          $self->irods->validate_checksum_metadata($publish_dest);
        };
        $irods_md5 = eval {
          $self->irods->calculate_checksum($publish_dest);
        };
        if (!defined($valid_meta)) {
          $self->warn("Unable to validate metadata: ", $@);
        } elsif (!defined($irods_md5)) {
          $self->warn("Unable to calculate iRODS checksum: ", $@);
        }
      }
  } else {
      $publish_dest = 'UNKNOWN';
  }
  $self->debug("Validating publication of $file to $publish_dest");
  # assign status to file
  my $status = 0;
  if (! $file_exists) {
    $status = 1;
  } elsif (! $listing) {
    $status = 2;
  } elsif (! $valid_meta) {
    $status = 3;
  } elsif ($file_md5 ne $irods_md5) {
    $status = 4;
  }
  if ($status==0) {
      $self->info("Validation OK: file ", $file, " status ", $status);
  } else {
      $self->info("Validation FAIL: file ", $file, " status ", $status);
  }
  return [$file, $publish_dest, $status];
}

sub _validate_publish_dest {
  my ($self, $publish_dest) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  $publish_dest eq '' and
    $self->logconfess('A non-empty publish_dest argument is required');

  $publish_dest = File::Spec->canonpath($publish_dest);

  return $publish_dest;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013-2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
