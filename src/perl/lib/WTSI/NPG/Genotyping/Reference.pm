
use utf8;

package WTSI::NPG::Genotyping::Reference;

use Moose;

our $VERSION = '';

has 'canonical_name' => (is => 'ro', isa => 'Str', required => 0);
has 'name'           => (is => 'ro', isa => 'Str', required => 1);

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Reference - A genome reference sequence.

=head1 SYNOPSIS

   my $ref = WTSI::NPG::Genotyping::Reference
     (canonical_name => 'GRCh38',
      name           => 'Homo_sapiens (GRCh38_15)');

=head1 DESCRIPTION

A instance of Reference represents a specific genome reference
sequence.

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
