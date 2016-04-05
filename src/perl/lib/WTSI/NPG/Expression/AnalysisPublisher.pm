use utf8;

package WTSI::NPG::Expression::AnalysisPublisher;

use Data::Dump qw(dump);
use File::Spec;
use Moose;
use Try::Tiny;

use WTSI::NPG::Expression::ProfileAnnotation;
use WTSI::NPG::Expression::ResultSet;
use WTSI::NPG::Expression::SampleProbeProfile;
use WTSI::NPG::Publisher;
use WTSI::NPG::SimplePublisher;
use WTSI::NPG::Utilities qw(collect_files);
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata; # has attribute name constants

our $VERSION = '';

our $FILE_TESTER = 'file';
our $DEFAULT_SAMPLE_ARCHIVE = '/archive/GAPI/exp/infinium';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Accountable',
  'WTSI::NPG::Annotator', 'WTSI::NPG::Expression::Annotator';

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
  my ($self, $publish_dest, $input_uuid) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  $publish_dest eq '' and
    $self->logconfess('A non-empty publish_dest argument is required');

  $publish_dest = File::Spec->canonpath($publish_dest);
  $self->info("Publishing to '$publish_dest' using the sample archive in '",
              $self->sample_archive, "'");

  # Make a path based on the md5sum of the manifest
  my $irods = $self->irods;

  my $analysis_coll;
  my $uuid;
  my $num_samples = 0;
  my $num_objects = 0;

  try {
    # Analysis directory
    my @analysis_meta;

    push(@analysis_meta, $self->make_analysis_metadata($input_uuid));
    unless ($input_uuid) {
      push(@analysis_meta,
           $self->make_creation_metadata($self->affiliation_uri,
                                         $self->publication_time,
                                         $self->accountee_uri));
    }

    my @uuid_meta = grep { $_->[0] eq $ANALYSIS_UUID }
      @analysis_meta;
    $uuid = $uuid_meta[0]->[1];
    if ($uuid) {
      $self->debug("Found analysis_uuid '$uuid' in metadata: ",
                   dump(\@analysis_meta));
    }
    else {
      $self->logconfess("Failed to find an analysis UUID in metadata: ",
                        dump(\@analysis_meta));
    }
    $analysis_coll = $self->ensure_analysis_collection($publish_dest, $uuid);
    my @analysis_files = $self->find_analysis_files;

    foreach my $file (@analysis_files) {
      $self->publish_analysis_file($analysis_coll, $file, $uuid);
    }

    foreach my $sample (@{$self->manifest->samples}) {
      my @sample_objects = $irods->find_objects_by_meta
        ($self->sample_archive,
         [$DCTERMS_IDENTIFIER          => $sample->{sample_id}],
         [$EXPRESSION_BEADCHIP         => $sample->{beadchip}],
         [$EXPRESSION_BEADCHIP_SECTION => $sample->{beadchip_section}]);

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
          $obj->find_in_metadata($STUDY_ID);

        if (@studies) {
          $self->debug("Sample '", $sample->{sample_id}, "' has metadata for ",
                       "studies [", join(", ", @studies), "]");

          foreach my $study (@studies) {
            unless (exists $studies_seen{$study}) {
              push(@analysis_meta, [$STUDY_ID => $study]);
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
        $obj->add_avu($ANALYSIS_UUID, $uuid);
        ++$num_objects;
      }

      ++$num_samples;
    }

    foreach my $m (@analysis_meta) {
      my ($attribute, $value, $units) = @$m;
      $analysis_coll->add_avu($attribute, $value, $units);
    }

    my @groups = $analysis_coll->expected_groups;
    $analysis_coll->set_content_permissions('read', @groups);

    $self->info("Published '", $self->analysis_directory, "' to '",
                $analysis_coll->str, "' and cross-referenced $num_objects ",
                "data objects for $num_samples samples");
  } catch {
    $self->error("Failed to publish: ", $_);
    undef $uuid;
  };

  return $uuid;
}

sub ensure_analysis_collection {
  my ($self, $publish_dest, $uuid) = @_;

  my $irods = $self->irods;

  # Make a path based on the md5sum of the manifest
  my $hash_path = $irods->hash_path($self->manifest->file_name);
  my $target = File::Spec->catdir($publish_dest, $hash_path);

  my @dirs = grep { $_ ne '' } File::Spec->splitdir($self->analysis_directory);
  my $leaf_dir = pop @dirs;
  my $leaf_coll = File::Spec->catdir($target, $leaf_dir);

  unless ($uuid) {
    if ($irods->list_collection($leaf_coll)) {
      $self->logcroak("An iRODS collection already exists at '$leaf_coll'. ",
                      "Please move or delete it before proceeding.");
    }
  }

  my $analysis_coll;

  if ($irods->list_collection($leaf_coll)) {
    $self->info("Collection '$leaf_coll' exists; updating metadata only");
    $analysis_coll = WTSI::NPG::iRODS::Collection->new($irods, $leaf_coll);
  }
  else {
    my $coll_path = $irods->add_collection($leaf_coll);
    $analysis_coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
    $self->info("Created new collection '", $analysis_coll->str, "'");
  }

  return $analysis_coll;
}

sub find_analysis_files {
  my ($self) = @_;

  return collect_files($self->analysis_directory, sub { return 1;});
}

sub publish_analysis_file {
  my ($self, $analysis_coll, $filename, $uuid) = @_;

  $self->debug("Publishing expression analysis content file '$filename' to '",
               $analysis_coll->str, "'");

  my @fingerprint = $self->make_analysis_metadata($uuid);

  if ($self->is_text_file($filename)) {
    $self->debug("Testing contents of text file '$filename'");

    # Test file to see whether it a type we recognise
    my $profile    = WTSI::NPG::Expression::SampleProbeProfile->new($filename);
    my $annotation = WTSI::NPG::Expression::ProfileAnnotation->new($filename);

    if ($profile->is_valid) {
      push @fingerprint,
        $self->make_profile_metadata($profile->normalisation_method,
                                     'sample', 'probe');
    }
    elsif ($annotation->is_valid) {
      push @fingerprint, $self->make_profile_annotation_metadata('annotation');
    }
  }
  else {
    $self->debug("Skipping testing contents of non-text file '$filename'");
  }

  my $publisher;

  if (@fingerprint) {
    $publisher = WTSI::NPG::Publisher->new
      (disperse      => 0,
       irods         => $self->irods,
       accountee_uid => $self->accountee_uid,
       logger        => $self->logger);
  }
  else {
    $publisher = WTSI::NPG::SimplePublisher->new
      (irods         => $self->irods,
       accountee_uid => $self->accountee_uid,
       logger        => $self->logger);
  }

  my $data_object = $publisher->publish_file($filename, \@fingerprint,
                                             $analysis_coll->str,
                                             $self->publication_time);
  return $data_object;
}

sub is_text_file {
  my ($self, $filename) = @_;

  my $test_run = WTSI::DNAP::Utilities::Runnable->new
    (executable  => $FILE_TESTER,
     arguments   => ['-b', '-i', $filename])->run;

  my @result = $test_run->split_stdout;

  $self->debug("Detected file '$filename' to be ", join(' ', @result));

  return (@result && $result[0] =~ m{^text\/plain}msx);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
