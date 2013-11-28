
use utf8;

package WTSI::NPG::Expression::AnalysisPublisher;

use Digest::MD5 qw(md5_hex);
use File::Spec;
use List::AllUtils qw(firstidx uniq);
use Moose;

use WTSI::NPG::Expression::Metadata qw($EXPRESSION_ANALYSIS_UUID_META_KEY
                                       $EXPRESSION_BEADCHIP_META_KEY
                                       $EXPRESSION_BEADCHIP_SECTION_META_KEY
                                       make_analysis_metadata);
use WTSI::NPG::Expression::ResultSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY
                           make_creation_metadata
                           make_modification_metadata
                           make_sample_metadata);
use WTSI::NPG::Publisher;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable';

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
    push(@analysis_meta, make_analysis_metadata($uuid));

    if ($irods->list_collection($leaf_collection)) {
      $self->info("Collection '$leaf_collection' exists; ",
                  "updating metadata only");
      $analysis_coll = WTSI::NPG::iRODS::Collection->new($irods,
                                                         $leaf_collection);
    }
    else {
      $irods->add_collection($target);
      push(@analysis_meta, make_creation_metadata($self->affiliation_uri,
                                                  $self->publication_time,
                                                  $self->accountee_uri));
      my $coll_path = $irods->put_collection($self->analysis_directory,
                                             $target);
      $analysis_coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
      $self->info("Created new collection '", $analysis_coll->str, "'");
    }

    my @uuid_meta = grep { $_->[0] =~ /uuid/ } @analysis_meta;
    $analysis_uuid = $uuid_meta[0]->[1];

    foreach my $sample (@{$self->manifest->samples}) {
      my @sample_objects = $irods->find_objects_by_meta
        ($self->sample_archive,
         ['dcterms:identifier'                  => $sample->{sample_id}],
         [$EXPRESSION_BEADCHIP_META_KEY         => $sample->{beadchip}],
         [$EXPRESSION_BEADCHIP_SECTION_META_KEY => $sample->{beadchip_section}]);
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
          $obj->find_in_metadata($STUDY_ID_META_KEY);

        if (@studies) {
          $self->debug("Sample '", $sample->{sample_id}, "' has metadata for ",
                       "studies [", join(", ", @studies), "]");

          foreach my $study (@studies) {
            unless (exists $studies_seen{$study}) {
              push(@analysis_meta, [$STUDY_ID_META_KEY => $study]);
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
        $obj->add_avu($EXPRESSION_ANALYSIS_UUID_META_KEY, $analysis_uuid);
        ++$num_objects;
      }

      ++$num_samples;
    }

    foreach my $m (@analysis_meta) {
      my ($attribute, $value, $units) = @$m;
      $analysis_coll->add_avu($attribute, $value, $units);
    }

    my @groups = $analysis_coll->expected_irods_groups;
    $analysis_coll->grant_group_access('read', @groups);
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
