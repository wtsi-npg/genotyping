
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayResult;

use Moose;

has 'assay'          => (is => 'ro', isa => 'Str', required => 1);
has 'snp_assayed'    => (is => 'ro', isa => 'Str', required => 1);
has 'x_allele'       => (is => 'ro', isa => 'Str', required => 1);
has 'y_allele'       => (is => 'ro', isa => 'Str', required => 1);
has 'sample_name'    => (is => 'ro', isa => 'Str', required => 1);
has 'type'           => (is => 'ro', isa => 'Str', required => 1);
has 'auto'           => (is => 'ro', isa => 'Str', required => 1);
has 'confidence'     => (is => 'ro', isa => 'Num', required => 1);
has 'call'           => (is => 'ro', isa => 'Str', required => 1);
has 'converted_call' => (is => 'ro', isa => 'Str', required => 1);
has 'x_intensity'    => (is => 'ro', isa => 'Num', required => 1);
has 'y_intensity'    => (is => 'ro', isa => 'Num', required => 1);
has 'str'            => (is => 'ro', isa => 'Str', required => 1);

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
