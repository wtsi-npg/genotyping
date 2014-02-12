
use utf8;

package WTSI::NPG::Genotyping::SNP;

use Moose;

has 'name'       => (is => 'ro', isa => 'Str', required => 1);
has 'ref_allele' => (is => 'ro', isa => 'Str', required => 0);
has 'alt_allele' => (is => 'ro', isa => 'Str', required => 0);
has 'chromosome' => (is => 'ro', isa => 'Str', required => 1);
has 'position'   => (is => 'ro', isa => 'Int', required => 1);
has 'strand'     => (is => 'ro', isa => 'Str', required => 0);
has 'str'        => (is => 'ro', isa => 'Str', required => 0);

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
