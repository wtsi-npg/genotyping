
package WTSI::Genotyping::Schema::Result::SampleGender;

use strict;
use warnings;

use base 'DBIx::Class::Core';


__PACKAGE__->table('sample_gender');
__PACKAGE__->add_columns
  ('id_sample', { data_type => 'integer',
                  is_foreign_key => 1,
                  is_nullable => 0 },
   'id_gender', { data_type => 'integer',
                  is_foreign_key => 1,
                  is_nullable => 0 },
   'id_method', { data_type => 'integer',
                  is_foreign_key => 1,
                  is_nullable => 0 });

__PACKAGE__->set_primary_key('id_sample', 'id_gender', 'id_method');

__PACKAGE__->belongs_to('sample',
                        'WTSI::Genotyping::Schema::Result::Sample',
                        { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->belongs_to('gender',
                        'WTSI::Genotyping::Schema::Result::Gender',
                        { 'foreign.id_gender' => 'self.id_gender' });

__PACKAGE__->belongs_to('method',
                        'WTSI::Genotyping::Schema::Result::Method',
                        { 'foreign.id_method' => 'self.id_method' });

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
