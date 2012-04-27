use utf8;

package WTSI::Genotyping;

use strict;
use vars qw($VERSION @ISA @EXPORT_OK);

use Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(
                common_stem
                filter_columns
                filter_gt_columns
                find_column_indices
                maybe_stdin
                maybe_stdout
                read_sample_json
                read_column_names
                read_fon
                read_gt_column_names
                read_it_column_names
                update_it_columns
                write_gt_calls
              );

use WTSI::Genotyping::DelimitedFiles;
use WTSI::Genotyping::IO;
use WTSI::Genotyping::Illuminus;
use WTSI::Genotyping::Utilities;

$VERSION = '0.1.0';

1;

__END__

=head1 NAME

WTSI::Genotyping

=head1 DESCRIPTION

General purpose utilities that may be used by genotyping projects.
See individual POD for details.

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

=head1 VERSION

  0.1.0

=head1 CHANGELOG

Fri Feb  3 13:35:00 GMT 2012 -- Initial version 0.1.0

=cut
