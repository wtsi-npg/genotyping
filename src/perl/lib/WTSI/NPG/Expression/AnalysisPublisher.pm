
use utf8;

package WTSI::NPG::Expression::AnalysisPublisher;

use Digest::MD5 qw(md5_hex);
use File::Spec;
use List::AllUtils qw(firstidx uniq);
use Moose;

# use WTSI::NPG::Expression::Metadata qw();
use WTSI::NPG::Expression::ResultSet;
use WTSI::NPG::iRODS;
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

  my @beadchips = uniq(map { $_->beadchip } @{$self->samples});
  my @section  = map { $_->beadchip_section } @{$self->samples};

}

sub _build_resultsets {
  my ($self) = @_;

  my @resultsets;

  foreach my $sample (@{$self->manifest->samples}) {
    push @resultsets, WTSI::NPG::Expression::ResultSet->new($sample);
  }

  return \@resultsets;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
