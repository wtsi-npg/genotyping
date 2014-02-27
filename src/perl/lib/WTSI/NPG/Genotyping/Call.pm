
use utf8;

package WTSI::NPG::Genotyping::Call;

use Moose;

has 'snp' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::SNP',
   required => 1);

has 'genotype' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

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
