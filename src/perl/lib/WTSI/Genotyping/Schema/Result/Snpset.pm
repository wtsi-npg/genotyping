
package WTSI::Genotyping::Schema::Result::Snpset;

use strict;
use warnings;

use base 'DBIx::Class::Core';


__PACKAGE__->table('snpset');
__PACKAGE__->add_columns
  ('id_snpset', { data_type => 'integer',
                       is_auto_increment => 1,
                       is_nullable => 0 },
   'name',           { data_type => 'text',
                       is_nullable => 0 },
   'version',        { data_type => 'text',
                       is_nullable => 1 });

__PACKAGE__->set_primary_key('id_snpset');
__PACKAGE__->add_unique_constraint(['name']);

__PACKAGE__->has_many('datasets',
                      'WTSI::Genotyping::Schema::Result::Dataset',
                      { 'foreign.id_snpset' => 'self.id_snpset' });

__PACKAGE__->has_many('snps',
                      'WTSI::Genotyping::Schema::Result::Snp',
                      { 'foreign.id_snpset' => 'self.id_snpset' });

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
