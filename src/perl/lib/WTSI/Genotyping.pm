use utf8;

package WTSI::Genotyping;

use warnings;
use strict;

use vars qw(@ISA @EXPORT_OK);

use Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(
                make_warehouse_metadata
                make_infinium_metadata
                make_file_metadata
                make_creation_metadata
                make_analysis_metadata
                has_consent

                get_wtsi_uri
                get_publisher_uri
                get_publisher_name
                publish_idat_files
                publish_gtc_files
                publish_analysis_directory

                filter_columns
                filter_gt_columns
                find_column_indices
                maybe_stdin
                maybe_stdout
                read_snp_json
                read_sample_json
                read_column_names
                read_fon
                read_gt_column_names
                read_it_column_names
                update_it_columns
                write_gt_calls
                write_gs_snps

                update_snp_locations
                update_sample_genders

                common_stem
              );

use WTSI::Genotyping::DelimitedFiles;
use WTSI::Genotyping::GenoSNP;
use WTSI::Genotyping::IO;
use WTSI::Genotyping::Illuminus;
use WTSI::Genotyping::Metadata;
use WTSI::Genotyping::Plink;
use WTSI::Genotyping::Publication;
use WTSI::Genotyping::Utilities;

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

  0.2.0

=cut
