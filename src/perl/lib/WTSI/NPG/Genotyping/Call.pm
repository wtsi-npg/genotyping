
use utf8;

package WTSI::NPG::Genotyping::Call;

use Moose;

with 'WTSI::DNAP::Utilities::Loggable';

has 'snp' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::SNP',
   required => 1);

has 'genotype' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'is_call' =>
  (is       => 'ro',
   isa      => 'Bool',
   default  => 1); # used to represent 'no calls'

=head2 merge

  Arg [1]    : WTSI::NPG::Genotyping::Call

  Example    : $new_call = $call->merge($other_call)
  Description: Merge results of this call with another on the same SNP:
               - If the genotypes are identical, return $self unchanged.
               - If exactly one of the two calls is a 'no call', return the
               non-null call.
               - If two non-null genotypes are in conflict, die with error
  Returntype : WTSI::NPG::Genotyping::Call

=cut

sub merge {
    my ($self, $other) = @_;
    unless ($self->snp->equals($other->snp)) {
        $self->logconfess("Attempted to merge calls for non-identical SNPs");
    }
    my $merged;
    if ($self->is_call && !($other->is_call)) {
        $merged = $self;
    } elsif (!($self->is_call) && $other->is_call) {
        $merged = $other;
    } elsif ($self->genotype eq $other->genotype) {
        $merged = $self;
    } else {
        $self->logdie("Unable to merge differing non-null genotype calls ",
                      "for SNP '", $self->snp->name, "': '",
                      $self->genotype, "', '", $other->genotype, "'");
    }
    return $merged;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

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
