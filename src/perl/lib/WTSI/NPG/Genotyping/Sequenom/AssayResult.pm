use utf8;

package WTSI::NPG::Genotyping::Sequenom::AssayResult;

use Moose;
use WTSI::NPG::Genotyping::Types qw(:all);

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

has 'allele'        => (is => 'ro', isa => 'Str', required => 1);
has 'assay_id'      => (is => 'ro', isa => 'Str', required => 1);
has 'chip'          => (is => 'ro', isa => 'Str', required => 1);
has 'customer'      => (is => 'ro', isa => 'Str', required => 1);
has 'experiment'    => (is => 'ro', isa => 'Str', required => 1);
has 'genotype_id'   => (is => 'ro', isa => 'Str', required => 1);
has 'height'        => (is => 'ro', isa => 'Num', required => 1);
has 'mass'          => (is => 'ro', isa => 'Num', required => 1);
has 'plate'         => (is => 'ro', isa => 'Str', required => 1);
has 'project'       => (is => 'ro', isa => 'Str', required => 1);
has 'sample_id'     => (is => 'ro', isa => 'Str', required => 1);
has 'status'        => (is => 'ro', isa => 'Str', required => 1);
has 'well_position' => (is => 'ro', isa => 'Str', required => 1);
has 'str'           => (is => 'ro', isa => 'Str', required => 1);

=head2 assay_address

  Arg [1]    : None

  Example    : my $pos = $self->assay_address
  Description: Alias for the well_position accessor. Provided for
               consistency with the function of the same name in
               Genotyping::Fluidigm::AssayResult
  Returntype : Str

=cut

sub assay_address {
  my ($self) = @_;

  return $self->well_position;
}

=head2 canonical_call

  Arg [1]    : None

  Example    : $call = $result->canonical_call
  Description: Method to return the genotype call, in a string representation
               of the form AA, AC, CC, or NN.  Name and behaviour of method,
               and format of output string, are intended to be consistent
               across all 'AssayResultSet' classes (for Sequenom, Fluidigm,
               etc) in the WTSI::NPG genotyping pipeline.
  Returntype : Str

=cut

sub canonical_call {
  my ($self) = @_;

  my $call = $self->genotype_id;
  if (!$call) {
    $call = ''; # FIXME -- check that this empty string should be permitted
  } elsif (length($call) == 1) {
    $call = $call . $call; # homozygote or no call
  }

  if (!is_SNPGenotype($call)) {
    $self->logcroak("Illegal genotype call '$call' for sample ",
                    $self->canonical_sample_id, ", SNP ", $self->assay_id);
  }

  return $call;
}

=head2 canonical_sample_id

  Arg [1]    : None

  Example    : $sample_identifier = $result->canonical_sample_id
  Description: Method to return the sample ID. Name and behaviour of method,
               and format of output string, are intended to be consistent
               across all 'AssayResultSet' classes (for Sequenom, Fluidigm,
               etc) in the WTSI::NPG genotyping pipeline.
  Returntype : Str

=cut

sub canonical_sample_id {
    my ($self) = @_;
    return $self->sample_id;
}

=head2 is_control

  Arg [1]    : None

  Example    : $result->is_control == 0
  Description: Placeholder. In the Fluidigm::AssayResult class, the function
               of this name checks for a 'control' result.
  Returntype : Str

=cut

sub is_control {
    return 0;
}

sub snpset_name {
  my ($self) = @_;

  return $self->_split_assay_id->[0];
}

=head2 snp_assayed

  Arg [1]    : None

  Example    : $sample_identifier = $result->snp_assayed
  Description: Return the name of the SNP being assayed. Name and
               behaviour of method are intended to be consistent
               across all 'AssayResultSet' classes (for Sequenom, Fluidigm,
               etc) in the WTSI::NPG genotyping pipeline.
  Returntype : Str

=cut

sub snp_assayed {
  my ($self) = @_;

  return $self->_split_assay_id->[1];
}

sub _split_assay_id {
  my ($self) = @_;

  my ($snpset_name, $snp_name) = split /-/, $self->assay_id;
  $snpset_name ||= '';
  $snp_name    ||= '';

  return [$snpset_name, $snp_name];
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Sequenom::AssayResult

=head1 DESCRIPTION

A class which represents a result of a Sequenom assay of one SNP for
one sample.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
