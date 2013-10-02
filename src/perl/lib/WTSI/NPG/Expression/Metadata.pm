use utf8;

package WTSI::NPG::Expression::Metadata;

use strict;
use warnings;
use Carp;
use File::Basename;
use UUID;

use WTSI::NPG::iRODS qw(md5sum);

use WTSI::NPG::Metadata qw(make_fingerprint);

use base 'Exporter';
our @EXPORT_OK = qw(
                    $EXPRESSION_ANALYSIS_UUID_META_KEY
                    $EXPRESSION_BEADCHIP_DESIGN_META_KEY
                    $EXPRESSION_BEADCHIP_META_KEY
                    $EXPRESSION_BEADCHIP_SECTION_META_KEY
                    $EXPRESSION_PROJECT_TITLE_META_KEY

                    infinium_fingerprint
                    make_analysis_metadata
                    make_infinium_metadata
);

our $EXPRESSION_PROJECT_TITLE_META_KEY = 'dcterms:title';
our $EXPRESSION_ANALYSIS_UUID_META_KEY = 'analysis_uuid';
our $EXPRESSION_BEADCHIP_META_KEY         = 'beadchip';
our $EXPRESSION_BEADCHIP_DESIGN_META_KEY  = 'beadchip_design';
our $EXPRESSION_BEADCHIP_SECTION_META_KEY = 'beadchip_section';

our $log = Log::Log4perl->get_logger('npg.irods.publish');


=head2 make_infinium_metadata

  Arg [1]    : sample hashref
  Example    : my @meta = make_infinium_metadata($sample)
  Description: Return a list of metadata key/value pairs describing the
               sample. Includes the beadchip identifier.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_infinium_metadata {
  my ($sample) = @_;

  return (['dcterms:identifier'                  => $sample->{sanger_sample_id}],
          [$EXPRESSION_BEADCHIP_META_KEY         => $sample->{beadchip}],
          [$EXPRESSION_BEADCHIP_SECTION_META_KEY => $sample->{beadchip_section}]);
}

=head2 make_analysis_metadata

  Arg [1]    : UUID to use instead of gereating a new one. Optional.
  Example    : my @meta = make_analysis_metadata()
  Description: Return a list of metadata key/value pairs describing an analysis.
  Returntype : array of arrayrefs
  Caller     : general

=cut

sub make_analysis_metadata {
  my ($supplied_uuid) = @_;

  my $uuid_str;
  if (defined $supplied_uuid) {
    $uuid_str = $supplied_uuid;
  }
  else {
    my $uuid_bin;
    UUID::generate($uuid_bin);
    UUID::unparse($uuid_bin, $uuid_str);
  }

  my @meta = ([$EXPRESSION_ANALYSIS_UUID_META_KEY => $uuid_str]);

  return @meta;
}

sub infinium_fingerprint {
  my @meta = @_;

  return make_fingerprint([$EXPRESSION_BEADCHIP_META_KEY,
                           $EXPRESSION_BEADCHIP_SECTION_META_KEY],
                          \@meta);
}

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
