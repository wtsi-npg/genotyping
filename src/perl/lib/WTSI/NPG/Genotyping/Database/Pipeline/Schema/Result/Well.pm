
package WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Well;

use strict;
use warnings;

use base 'DBIx::Class::Core';

__PACKAGE__->table('well');
__PACKAGE__->add_columns
  ('id_well',    { data_type => 'integer',
                   is_auto_increment => 1,
                   is_nullable => 0 },
   'id_address', { data_type => 'integer',
                   is_foreign_key => 1,
                   is_nullable => 0 },
   'id_plate',   { data_type => 'integer',
                   is_foreign_key => 1,
                   is_nullable => 0 },
   'id_sample',  { data_type => 'integer',
                   is_foreign_key => 1,
                   is_nullable => 1 });

__PACKAGE__->set_primary_key('id_well');
__PACKAGE__->add_unique_constraint(['id_address', 'id_plate']);

__PACKAGE__->belongs_to
  ('sample',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Sample',
   { 'foreign.id_sample' => 'self.id_sample' },
   { join_type => 'LEFT' });

__PACKAGE__->belongs_to
  ('plate',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Plate',
   { 'foreign.id_plate' => 'self.id_plate' });

__PACKAGE__->belongs_to
  ('address',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Address',
   { 'foreign.id_address' => 'self.id_address' });

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
