use utf8;

package WTSI::NPG::Genotyping::Schema::Result::Piperun;

use strict;
use warnings;

use base 'DBIx::Class::Core';


__PACKAGE__->table('piperun');
__PACKAGE__->add_columns
  ('id_piperun', { data_type => 'integer',
                   is_auto_increment => 1 },
  'name',        { data_type => 'text',
                   is_nullable => 0 },
  'start_time',  { data_type => 'integer',
                   is_nullable => 1 },
  'finish_time', { data_type => 'integer',
                   is_nullable => 1 });

__PACKAGE__->set_primary_key('id_piperun');
__PACKAGE__->add_unique_constraint(['name']);

__PACKAGE__->has_many('datasets',
                      'WTSI::NPG::Genotyping::Schema::Result::Dataset',
                      { 'foreign.id_piperun' => 'self.id_piperun' });

=head2 validate_snpset

  Arg [1]    : string Snpset name
  Example    : $piperun->validate_snpset('HumanOmni25-8v1')
  Description: Returns true if data for the requested Snpset may be analysed
               in this Piperun. It is not possible to mix Snpsets in a single
               Piperun. Validity in this context means that this Snpset
               is the same as any which may be present in the Piperun already.
  Returntype : boolean

=cut

sub validate_snpset {
  my ($self, $snpset) = @_;

  my @snpsets = map { $_->snpset } $self->datasets;
  my @infinium_snpsets = grep { $_->name !~ m{sequenom|fluidigm}i } @snpsets;

  my $valid = 1;
  if (@infinium_snpsets) {
    my $name = $snpset->name;
    unless (grep { $_->name eq $name } @infinium_snpsets) {
      $valid = 0;
    }
  }

  return $valid;
}

=head2 validate_datasets

  Arg [1]    : None
  Example    : $piperun->validate_datasets
  Description: Returns true if the datasets in this Piperun all use the same
               Snpset.
  Returntype : boolean

=cut

sub validate_datasets {
  my ($self) = @_;

  my @snpsets = map { $_->snpset } $self->datasets;

  my @infinium_snpsets = grep { $_->name !~ m{sequenom|fluidigm}i } @snpsets;
  my @snpset_names = map { $_->name } @infinium_snpsets;

  my $valid = 1;
  if (scalar @snpset_names > 1) {
    my ($first, @rest) = @snpset_names;

    my @mismatched;
    foreach my $elt (@rest) {
      if ($elt eq $first) {
        next;
      }
      else {
        push(@mismatched, $elt);
      }
    }

    if (@mismatched) {
      $valid = 0;
      warn "Invalid piperun; datasets have mixed SNP sets: [",
        join(", ", @mismatched), "]";
    }
  }

  return $valid;
}

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
