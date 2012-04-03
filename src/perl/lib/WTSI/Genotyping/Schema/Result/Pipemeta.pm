
package WTSI::Genotyping::Schema::Result::Pipemeta;

use strict;
use warnings;

use base 'DBIx::Class::Core';


__PACKAGE__->table('pipemeta');
__PACKAGE__->add_columns
  ( 'schema_version',  { data_type => 'text',
                         is_nullable => 0 },
    'pipeline_version',{ data_type => 'text',
                         is_nullable => 0 });

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
