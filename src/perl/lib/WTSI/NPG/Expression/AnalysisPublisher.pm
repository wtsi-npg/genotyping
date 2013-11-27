
use utf8;

package WTSI::NPG::Expression::AnalysisPublisher;

use Digest::MD5 qw(md5_hex);
use File::Spec;
use List::AllUtils qw(firstidx uniq);
use Moose;

use WTSI::NPG::Expression::Metadata qw(infinium_fingerprint
                                       make_infinium_metadata
                                       make_analysis_metadata);
use WTSI::NPG::Expression::ResultSet;
use WTSI::NPG::iRODS;
use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY
                           make_creation_metadata
                           make_modification_metadata
                           make_sample_metadata);
use WTSI::NPG::Publisher;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable';

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

has 'manifest' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Expression::ChipLoadingManifest',
   required => 1);

has 'resultsets' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Expression::ResultSet]',
   required => 1,
   lazy     => 1,
   builder  => '_build_resultsets');

has 'sequencescape_db' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Database::Warehouse',
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




  return $analysis_uuid;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
