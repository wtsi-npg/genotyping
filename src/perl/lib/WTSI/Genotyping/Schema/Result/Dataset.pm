use utf8;

package WTSI::Genotyping::Schema::Result::Dataset;

use strict;
use warnings;

use base 'DBIx::Class::Core';


__PACKAGE__->table('dataset');
__PACKAGE__->add_columns
  ('id_dataset',      { data_type => 'integer',
                        is_auto_increment => 1,
                        is_nullable => 0 },
   'if_project',      { data_type => 'text',
                        is_nullable => 1 },
   'id_datasupplier', { data_type => 'integer',
                        is_foreign_key => 1,
                        is_nullable => 0 },
   'id_snpset',       { data_type => 'integer',
                        is_foreign_key => 1,
                        is_nullable => 0 },
   'id_piperun',      { data_type => 'integer',
                        is_foreign_key => 1,
                        is_nullable => 0 });

__PACKAGE__->set_primary_key('id_dataset');
__PACKAGE__->add_unique_constraint(['if_project']);

__PACKAGE__->belongs_to('piperun',
                        'WTSI::Genotyping::Schema::Result::Piperun',
                        { 'foreign.id_piperun' => 'self.id_piperun' });

__PACKAGE__->belongs_to('snpset',
                        'WTSI::Genotyping::Schema::Result::Snpset',
                        { 'foreign.id_snpset' => 'self.id_snpset' });

__PACKAGE__->belongs_to('datasupplier',
                        'WTSI::Genotyping::Schema::Result::Datasupplier',
                        { 'foreign.id_datasupplier' => 'self.id_datasupplier' });

__PACKAGE__->has_many('samples',
                      'WTSI::Genotyping::Schema::Result::Sample',
                      { 'foreign.id_dataset' => 'self.id_dataset' });

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
