
use utf8;

package WTSI::NPG::Genotyping::SNPSetPublisher;

use File::Spec;
use Moose;
use Moose::Util::TypeConstraints;

use WTSI::NPG::Genotyping::SNPSet;
use WTSI::NPG::Genotyping::Metadata qw();
use WTSI::NPG::iRODS;
use WTSI::NPG::SimplePublisher;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Accountable';

enum 'GenotypingPlatform', [qw(fluidigm sequenom)];

has 'file_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'snpset_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'snpset_platform' =>
  (is       => 'ro',
   isa      => 'GenotypingPlatform',
   required => 1);

has 'reference_names' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   required => 1);

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

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
}

sub publish {
  my ($self, $publish_dest) = @_;

  defined $publish_dest or
    $self->logconfess('A defined publish_dest argument is required');

  my $publisher = WTSI::NPG::SimplePublisher->new
    (irods         => $self->irods,
     accountee_uid => $self->accountee_uid,
     logger        => $self->logger);

  $self->debug("Publishing SNPSet CSV data file '", $self->file_name, "'");

  my $snpset_name = $self->snpset_platform . '_plex';

  my @meta = ([$snpset_name   => $self->snpset_name]);
  foreach my $reference_name (@{$self->reference_names}) {
    push @meta, [reference_name => $reference_name]
  }

  my $rods_path = $publisher->publish_file($self->file_name, \@meta,
                                           $publish_dest,
                                           $self->publication_time);

  my $data_object = WTSI::NPG::iRODS::DataObject->new($self->irods, $rods_path);
  my $snpset = WTSI::NPG::Genotyping::SNPSet->new($data_object);

  $self->info("Published SNPSet CSV data file '", $self->file_name, "' of ",
              scalar @{$snpset->snps}, " SNPs to ", $data_object->str);

  return $data_object->str;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
