
package WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Result;

use strict;
use warnings;

use base 'DBIx::Class::Core';

__PACKAGE__->table('result');
__PACKAGE__->add_columns
  ('id_result', { data_type => 'integer',
                  is_auto_increment => 1,
                  is_nullable => 0 },
   'id_sample', { data_type => 'integer',
                  is_foreign_key => 1,
                  is_nullable => 0 },
   'id_method', { data_type => 'integer',
                  is_foreign_key => 1,
                  is_nullable => 0 },
   'value',     { data_type => 'text',
                  is_nullable => 1 });

__PACKAGE__->set_primary_key('id_result');

__PACKAGE__->belongs_to
  ('method',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Method',
   { 'foreign.id_method' => 'self.id_method' });

__PACKAGE__->belongs_to
  ('sample',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::Sample',
   { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->has_many
  ('snp_results',
   'WTSI::NPG::Genotyping::Database::Pipeline::Schema::Result::SnpResult',
   { 'foreign.id_result' => 'self.id_result' });

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
