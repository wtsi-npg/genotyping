
package WTSI::NPG::Database::MLWarehouse;

use Moose;

use WTSI::DNAP::Warehouse::Schema;

our $VERSION = '';

extends 'WTSI::NPG::Database';

with 'WTSI::NPG::Database::DBIx';

sub connect {
  my ($self, %args) = @_;

  $self->schema(WTSI::DNAP::Warehouse::Schema->connect($self->data_source,
                                                       $self->username,
                                                       $self->password,
                                                       \%args));
  return $self;
}

sub find_fluidigm_plate {
  my ($self, $fluidigm_barcode) = @_;

  defined $fluidigm_barcode or
    $self->logconfess("The fluidigm_barcode argument was undefined");
  $fluidigm_barcode or
    $self->logconfess("The fluidigm_barcode argument was empty");

  my $plate = $self->schema->resultset('FlgenPlate')->search
    ({plate_barcode => $fluidigm_barcode},
     {prefetch      => 'sample'});

  my %plate;
  while (my $row = $plate->next) {
    $plate{$row->well_label} = _make_well_result($row);
  }

  if (%plate) {
    $self->debug("Found Fluidigm plate '$fluidigm_barcode'");
  }
  else {
    $self->logconfess("Failed to find Fluidigm plate '$fluidigm_barcode'");
  }

  return \%plate;
}

sub find_fluidigm_sample_by_plate {
  my ($self, $fluidigm_barcode, $well_address) = @_;

  defined $fluidigm_barcode or
    $self->logconfess("The fluidigm_barcode argument was undefined");
  $fluidigm_barcode or
    $self->logconfess("The fluidigm_barcode argument was empty");

  defined $well_address or
    $self->logconfess("The well_address argument was undefined");
  $well_address or $self->logconfess("The map argument was empty");

  my $plate = $self->schema->resultset('FlgenPlate')->search
    ({plate_barcode => $fluidigm_barcode,
      well_label    => $well_address},
     {prefetch      => ['sample', 'study']});

  # (plate_barcode . well_label) cardinality is not constrained in the
  # warehouse, so check that we get one result only.
  my @results;
  while (my $row = $plate->next) {
    push @results, _make_well_result($row);
  }
  my $n = scalar @results;
  if ($n > 1) {
    $self->logconfess("$n samples were returned where 1 sample was expected");
  }

  $self->debug("Found 1 Fluidigm sample in '$fluidigm_barcode' ",
               "'$well_address'");

  return shift @results;
}

sub _make_well_result {
  my ($plate_row) = @_;

  return {id_lims            => $plate_row->sample->id_lims,
          id_sample_lims     => $plate_row->sample->id_sample_lims,
          sanger_sample_id   => $plate_row->sample->sanger_sample_id,
          consent_withdrawn  => $plate_row->sample->consent_withdrawn,
          donor_id           => $plate_row->sample->donor_id,
          uuid               => $plate_row->sample->uuid_sample_lims,
          name               => $plate_row->sample->name,
          common_name        => $plate_row->sample->common_name,
          supplier_name      => $plate_row->sample->supplier_name,
          accession_number   => $plate_row->sample->accession_number,
          gender             => $plate_row->sample->gender,
          cohort             => $plate_row->sample->cohort,
          control            => $plate_row->sample->control,
          study_id           => $plate_row->study->id_study_lims,
          barcode            => $plate_row->plate_barcode_lims,
          plate_purpose_name => 'Fluidigm',
          map                => $plate_row->well_label}
}

no Moose;

__PACKAGE__->meta->make_immutable;

1;

__END__

