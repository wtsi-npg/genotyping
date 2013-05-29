use utf8;

package WTSI::NPG::Genotyping::Metadata;

use strict;
use warnings;
use Carp;
use File::Basename;
use UUID;

use WTSI::NPG::iRODS qw(md5sum);

use base 'Exporter';
our @EXPORT_OK = qw($GENOTYPING_PROJECT_TITLE_META_KEY
                    $GENOTYPING_ANALYSIS_UUID_META_KEY
                    $GENOTYPING_BEADCHIP_META_KEY
                    $GENOTYPING_BEADCHIP_DESIGN_META_KEY
                    $GENOTYPING_BEADCHIP_SECTION_META_KEY
                    make_infinium_metadata
                    make_analysis_metadata);

our $GENOTYPING_PROJECT_TITLE_META_KEY = 'dcterms:title';
our $GENOTYPING_ANALYSIS_UUID_META_KEY = 'analysis_uuid';
our $GENOTYPING_BEADCHIP_META_KEY         = 'beadchip';
our $GENOTYPING_BEADCHIP_DESIGN_META_KEY  = 'beadchip_design';
our $GENOTYPING_BEADCHIP_SECTION_META_KEY = 'beadchip_section';

our $log = Log::Log4perl->get_logger('npg.irods.publish');

=head2 make_infinium_metadata

  Arg [1]    : sample hashref from WTSI::NPG::Genotyping::Database::Infinium
  Example    : my @meta = make_infinium_metadata($sample)
  Description: Return a list of metadata key/value pairs describing the
               sample in the Infinium LIMS. Includes the beadchip identifier.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_infinium_metadata {
  my ($if_sample) = @_;

  return ([$GENOTYPING_PROJECT_TITLE_META_KEY    => $if_sample->{project}],
          ['dcterms:identifier'                  => $if_sample->{sample}],
          [$GENOTYPING_BEADCHIP_META_KEY         => $if_sample->{beadchip}],
          [$GENOTYPING_BEADCHIP_SECTION_META_KEY => $if_sample->{beadchip_section}],
          [$GENOTYPING_BEADCHIP_DESIGN_META_KEY  => $if_sample->{beadchip_design}]);
}

=head2 make_analysis_metadata

  Arg [1]    : Arrayref of genotyping project titles
  Example    : my @meta = make_analysis_metadata(\@titles)
  Description: Return a list of metadata key/value pairs describing an analysis
               including the genotyping project names involved.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_analysis_metadata {
  my ($genotyping_project_titles) = @_;

  my $uuid_bin;
  my $uuid_str;
  UUID::generate($uuid_bin);
  UUID::unparse($uuid_bin, $uuid_str);

  my @meta = ([$GENOTYPING_ANALYSIS_UUID_META_KEY => $uuid_str]);

  foreach my $title (@$genotyping_project_titles) {
    push(@meta, [$GENOTYPING_PROJECT_TITLE_META_KEY => $title]);
  }

  return @meta;
}

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
