
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayResult;

use Moose;

with 'WTSI::NPG::Loggable';

has 'assay'          => (is => 'ro', isa => 'Str', required => 1);
has 'snp_assayed'    => (is => 'ro', isa => 'Str', required => 1);
has 'x_allele'       => (is => 'ro', isa => 'Str', required => 1);
has 'y_allele'       => (is => 'ro', isa => 'Str', required => 1);
has 'sample_name'    => (is => 'ro', isa => 'Str', required => 1);
has 'type'           => (is => 'ro', isa => 'Str', required => 1);
has 'auto'           => (is => 'ro', isa => 'Str', required => 1);
has 'confidence'     => (is => 'ro', isa => 'Num', required => 1);
has 'final'          => (is => 'ro', isa => 'Str', required => 1);
has 'converted_call' => (is => 'ro', isa => 'Str', required => 1);
has 'x_intensity'    => (is => 'ro', isa => 'Num', required => 1);
has 'y_intensity'    => (is => 'ro', isa => 'Num', required => 1);
has 'str'            => (is => 'ro', isa => 'Str', required => 1);

our $EMPTY_NAME          = '[ Empty ]';
our $NO_TEMPLATE_CONTROL = 'NTC';
our $NO_CALL             = 'No Call';

=head2 is_control

  Arg [1]    : None

  Example    : $result->is_control
  Description: Return whether the result is for a control assay.
  Returntype : Bool

=cut

sub is_control {
  my ($self) = @_;

  # It is not clear how no-template controls are being
  # represented. There seem to be several ways in play, currently:
  #
  # a) The snp_assayed column is empty
  # b) The sample_name column contains the token '[ Empty ]'
  # c) The type, auto, call and converted_call columns contain the token 'NTC'
  #
  # or some combination of the above.

  return ($self->snp_assayed eq '' or
          $self->sample_name eq $EMPTY_NAME or
          $self->type        eq $NO_TEMPLATE_CONTROL);
}

=head2 is_call

  Arg [1]    : None

  Example    : $result->is_call
  Description: Return whether the result has called a genotype.
  Returntype : Bool

=cut

sub is_call {
  my ($self) = @_;

  $self->final ne $NO_CALL;
}

=head2 compact_call

  Arg [1]    : None

  Example    : $result->compact_call
  Description: Return the Fluidigm converted call attribute, having removed
               the colon separator.
  Returntype : Str

=cut

sub compact_call {
  my ($self) = @_;

  my $compact = $self->converted_call;
  $compact =~ s/://;

  return $compact;
}

=head2 npg_call

  Arg [1]    : None

  Example    : $call = $result->npg_call()
  Description: Method to return the genotype call, in a string representation
               of the form AA, AC, CC, or NN. Name and behaviour of method are
               intended to be consistent across all 'AssayResultSet' classes
               (for Sequenom, Fluidigm, etc) in the WTSI::NPG genotyping
               pipeline.
  Returntype : Str

=cut

sub npg_call {
    my ($self) = @_;
    my $call = $self->compact_call(); # removes the : from raw input call
    if ($call eq $NO_CALL) { 
        $call = 'NN'; 
    } elsif ($call !~ /[ACGTN][ACGTN]/ ) {
        my $msg = "Illegal genotype call '$call' for sample ".
            $self->npg_sample_id().", SNP ".self->npg_snp_id();
        $self->logcroak($msg);
    }
    return $call;
}

=head2 npg_sample_id

  Arg [1]    : None

  Example    : $sample_identifier = $result->npg_sample_id()
  Description: Method to return the sample ID. Name and behaviour of method,
               and format of output string, are intended to be consistent
               across all 'AssayResultSet' classes (for Sequenom, Fluidigm,
               etc) in the WTSI::NPG genotyping pipeline.
  Returntype : Str

=cut

sub npg_sample_id {
    my ($self) = @_;
    return $self->sample_name();
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Fluidigm::AssayResult

=head1 DESCRIPTION

A class which represents the result of a Fluidigm assay of one SNP for
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
