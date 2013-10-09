
package WTSI::NPG::Genotyping::FluidigmPublisher;

use File::Temp qw(tempdir);

use Moose;

use WTSI::NPG::Publication qw(publish_file);

with 'WTSI::NPG::Loggable';

has 'creator_uri' => (is => 'ro', isa => 'URI', required => 1);

has 'publisher_uri' => (is => 'ro', isa => 'URI', required => 1);

has 'publication_time' => (is => 'ro', isa => 'DateTime', required => 1);

has 'fluidigm_export' => (is => 'ro',
                          isa => 'WTSI::NPG::Genotyping::FluidigmExportFile',
                          required => 1);

sub publish {
  my ($self, $publish_dest, @addresses) = @_;

  my $published = 0;
  my $tmpdir = tempdir(CLEANUP => 1);
  my $current_file;

  my $export = $self->fluidigm_export;
  unless (@addresses) {
    @addresses = @{$export->addresses};
  }

  my $total = scalar @addresses;
  my $possible = $export->size;
  $self->debug("Publishing $total Fluidigm CSV data files ",
               "from a possible $possible");

  foreach my $address (@addresses) {
    eval {
      my $file = sprintf("%s/%s_%s.csv", $tmpdir, $address,
                         $export->fluidigm_barcode);
      $current_file = $file;

      my $record_count = $export->write_sample_assays($address, $file);
      $self->debug("Wrote $record_count records into '$file'");

      my @fingerprint = $export->fluidigm_fingerprint($address);
      my $data_object = publish_file($file, \@fingerprint,
                                     $self->creator_uri,
                                     $publish_dest,
                                     $self->publisher_uri,
                                     $self->publication_time);
      ++$published;
    };

    if ($@) {
      $self->error("Failed to publish '$current_file' to ",
                   "'$publish_dest': ", $@);
    }
    else {
      $self->debug("Published '$current_file': $published of $total ",
                   "from a possible $possible");
    }
  }

  return $published;
}

no Moose;

1;
