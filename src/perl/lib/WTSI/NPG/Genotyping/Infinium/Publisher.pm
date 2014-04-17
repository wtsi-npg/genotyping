
use utf8;

package WTSI::NPG::Genotyping::Infinium::Publisher;

use Moose;

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

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
}

sub publish {
  my ($self, $publish_dest) = @_;

  return $self->publish_samples($publish_dest);
}

sub publish_samples {
  my ($self, $publish_dest) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  $publish_dest eq '' and
    $self->logconfess('A non-empty publish_dest argument is required');

  $publish_dest = File::Spec->canonpath($publish_dest);

  my $num_published = 0;
  my $total = scalar @{$self->resultsets} * 3;

  $self->debug("Publishing $total Infinium files from a possible $total");

  foreach my $resultset (@{$self->resultsets}) {
    $num_published += $self->_publish_idat_files($resultset, $publish_dest);
    $num_published += $self->_publish_gtc_file($resultset, $publish_dest);
  }

  $self->info("Published $num_published/$total Infinium files ",
              "to '$publish_dest'");

  return $num_published;
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
  my $data_object = $publisher->publish_file($filename, \@fingerprint,
                                             $publish_dest,
                                             $self->publication_time);
  return $data_object;
}

sub _build_resultsets {
  my ($self) = @_;

  my $filesets = $self->_build_filesets;

  my @resultsets;

  foreach my $beadchip (sort keys %$filesets) {
    $self->debug("Collating beadchip $beadchip");

  SECTION:
    foreach my $section (sort keys $filesets->{$beadchip}) {
      $self->debug("Collating section $section");

      my @fileset = @{$filesets->{$beadchip}{$section}};
      unless (scalar @fileset == 3) {
        $self->warn("Failed to collate a resultset for beadchip ",
                    "'$beadchip' section '$section' because it did not ",
                    "contain exactly 3 files: [",
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

      if ($gtc && $red && $grn) {
        $self->debug("Collating a new resultset for $beadchip $section");

        push @resultsets, WTSI::NPG::Genotyping::Infinium::ResultSet->new
          (beadchip         => $beadchip,
           beadchip_section => $section,
           gtc_file         => $gtc,
           red_idat_file    => $red,
           grn_idat_file    => $grn);
      }
      else {
        $self->warn("Failed to collate a resultset for beadchip ",
                    "'$beadchip' section '$section' because its file set was ",
                    "incomplete: [GTC: '$gtc', Red: '$red', Green: '$grn']");
      }
    }
  }

  return \@resultsets;
}

sub _build_filesets {
  my ($self) = @_;

  # Each hash chain $filesets{beadchip}{section} points to an array
  # containing the names of the 3 files in the set
  my %filesets;

  foreach my $path (sort  @{$self->data_files}) {
    my ($volume, $dirs, $filename) = File::Spec->splitpath($path);

    $self->debug("Preparing to collate '$filename' into a resultset");

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

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

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
