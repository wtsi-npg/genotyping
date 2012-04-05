use utf8;

package WTSI::Genotyping::Schema::Result::State;

use strict;
use warnings;

use base 'DBIx::Class::Core';


__PACKAGE__->table('state');
__PACKAGE__->add_columns
  ('id_state',  { data_type => 'integer',
                  is_auto_increment => 1,
                  is_nullable => 0 },
   'name',      { data_type => 'text',
                  is_nullable => 0 },
  'definition', { data_type => 'text',
                  is_nullable => 0 });

__PACKAGE__->set_primary_key('id_state');
__PACKAGE__->add_unique_constraint(['name']);

__PACKAGE__->has_many('samples',
                      'WTSI::Genotyping::Schema::Result::Sample',
                      { 'foreign.id_state' => 'self.id_state' });

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
