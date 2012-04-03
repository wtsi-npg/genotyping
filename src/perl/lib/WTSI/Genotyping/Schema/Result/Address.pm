
package WTSI::Genotyping::Schema::Result::Address;

use strict;
use warnings;

use base 'DBIx::Class::Core';


__PACKAGE__->table('address');
__PACKAGE__->add_columns
  ('id_address', { data_type => 'integer',
                   is_auto_increment => 1,
                   is_nullable => 0 },
   'label1',     { data_type => 'text',
                   is_nullable => 0 },
   'label2',     { data_type => 'text',
                   is_nullable => 0 });

__PACKAGE__->set_primary_key('id_address');
__PACKAGE__->add_unique_constraint(['label2']);
__PACKAGE__->add_unique_constraint(['label1']);

__PACKAGE__->has_many('wells',
                      'WTSI::Genotyping::Schema::Result::Well',
                      { 'foreign.id_address' => 'self.id_address' });

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
