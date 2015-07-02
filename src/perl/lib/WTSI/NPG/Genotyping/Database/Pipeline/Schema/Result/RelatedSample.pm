
package WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::RelatedSample;

use strict;
use warnings;

use base 'DBIx::Class::Core';

__PACKAGE__->table('related_sample');
__PACKAGE__->add_columns
  ('id_sample_a', { data_type => 'integer',
                    is_foreign_key => 1,
                    is_nullable => 0 },
   'id_sample_b', { data_type => 'integer',
                    is_foreign_key => 1,
                    is_nullable => 0 },
   'id_relation', { data_type => 'integer',
                    is_foreign_key => 1,
                    is_nullable => 0 });

__PACKAGE__->set_primary_key('id_sample_a', 'id_sample_b', 'id_relation');

__PACKAGE__->belongs_to
  ('relation',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Relation',
   { 'foreign.id_relation' => 'self.id_relation' });

__PACKAGE__->belongs_to
  ('sample_a',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Sample',
   { 'foreign.id_sample' => 'self.id_sample_a' });

__PACKAGE__->belongs_to
  ('sample_b',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Sample',
   { 'foreign.id_sample' => 'self.id_sample_b' });

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
