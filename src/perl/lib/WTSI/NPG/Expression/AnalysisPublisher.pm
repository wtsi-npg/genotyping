use utf8;

package WTSI::NPG::Expression::AnalysisPublisher;

use File::Spec;
use Moose;

use WTSI::NPG::Expression::ResultSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::Publisher;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable', 'WTSI::NPG::Annotator',
  'WTSI::NPG::Expression::Annotator';

our $DEFAULT_SAMPLE_ARCHIVE = '/archive/GAPI/exp/infinium';

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

has 'analysis_directory' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'sample_archive' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 0,
   default  => sub {
     return $DEFAULT_SAMPLE_ARCHIVE;
   });

has 'manifest' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Expression::ChipLoadingManifest',
   required => 1);

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
}

sub publish {
  my ($self, $publish_dest, $uuid) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  $publish_dest eq '' and
    $self->logconfess('A non-empty publish_dest argument is required');

  $publish_dest = File::Spec->canonpath($publish_dest);
  $self->info("Publishing to '$publish_dest' using the sample archive in '",
              $self->sample_archive, "'");

  # Make a based on the md5sum of the manifest
  my $irods = $self->irods;
  my $hash_path = $irods->hash_path($self->manifest->file_name);
  my $target = File::Spec->catdir($publish_dest, $hash_path);

  my @dirs = grep { $_ ne '' } File::Spec->splitdir($self->analysis_directory);
  my $leaf_dir = pop @dirs;
  my $leaf_collection = File::Spec->catdir($target, $leaf_dir);

  unless ($uuid) {
    if ($irods->list_collection($leaf_collection)) {
      $self->logcroak("An iRODS collection already exists at ",
                      "'$leaf_collection'. ",
                      "Please move or delete it before proceeding.");
    }
  }

  my $analysis_coll;
  my $analysis_uuid;
  my $num_samples = 0;
  my $num_objects = 0;

  eval {
    # Analysis directory
    my @analysis_meta;
    push(@analysis_meta, $self->make_analysis_metadata($uuid));

    if ($irods->list_collection($leaf_collection)) {
      $self->info("Collection '$leaf_collection' exists; ",
                  "updating metadata only");
      $analysis_coll = WTSI::NPG::iRODS::Collection->new($irods,
                                                         $leaf_collection);
    }
    else {
      $irods->add_collection($target);
      push(@analysis_meta,
           $self->make_creation_metadata($self->affiliation_uri,
                                         $self->publication_time,
                                         $self->accountee_uri));
      my $coll_path = $irods->put_collection($self->analysis_directory,
                                             $target);
      $analysis_coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
      $self->info("Created new collection '", $analysis_coll->str, "'");
    }

    my @uuid_meta = grep { $_->[0] eq $self->analysis_uuid_attr }
      @analysis_meta;
    $analysis_uuid = $uuid_meta[0]->[1];

    foreach my $sample (@{$self->manifest->samples}) {
      my @sample_objects = $irods->find_objects_by_meta
        ($self->sample_archive,
         [$self->dcterms_identifier_attr          => $sample->{sample_id}],
         [$self->expression_beadchip_attr         => $sample->{beadchip}],
         [$self->expression_beadchip_section_attr => $sample->{beadchip_section}]);
      unless (@sample_objects) {
        $self->logconfess("Failed to find data in iRODS in sample archive '",
                          $self->sample_archive, "' for sample '",
                          $sample->{sample_id}, "'");
      }

      my %studies_seen;

      # Should be pairs of 1x idat plus 1x XML files for each sample
      foreach my $sample_object (@sample_objects) {
        my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $sample_object);

        # Xref analysis to sample studies
        my @studies = map { $_->{value} }
          $obj->find_in_metadata($self->study_id_attr);

        if (@studies) {
          $self->debug("Sample '", $sample->{sample_id}, "' has metadata for ",
                       "studies [", join(", ", @studies), "]");

          foreach my $study (@studies) {
            unless (exists $studies_seen{$study}) {
              push(@analysis_meta, [$self->study_id_attr => $study]);
              $studies_seen{$study}++;
            }
          }
        }
        else {
          $self->logconfess("Failed to find a study_id in iRODS for sample '",
                            $sample->{sample_id}, "', data object '",
                            $obj->str, "'");
        }

        # Xref samples to analysis UUID
        $obj->add_avu($self->analysis_uuid_attr, $analysis_uuid);
        ++$num_objects;
      }

      ++$num_samples;
    }

    foreach my $m (@analysis_meta) {
      my ($attribute, $value, $units) = @$m;
      $analysis_coll->add_avu($attribute, $value, $units);
    }

    my @groups = $analysis_coll->expected_irods_groups;
    $analysis_coll->set_content_permissions('read', @groups);
  };

  if ($@) {
    $self->error("Failed to publish: ", $@);
    undef $analysis_uuid;
  }
  else {
    $self->info("Published '", $self->analysis_directory, "' to '",
                $analysis_coll->str, "' and cross-referenced $num_objects ",
                "data objects for $num_samples samples");
  }

  return $analysis_uuid;
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
