use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;
use File::Basename;

sub make_creation_metadata {
  my ($creation_time, $publisher) = @_;

  return (['dcterms:created' => $creation_time->iso8601()],
          ['dcterms:publisher' => $publisher]);
}

sub make_modification_metadata {
  my ($modification_time) = @_;

  return (['dcterms:modified' => $modification_time]);
}

sub make_warehouse_metadata {
  my ($if_sample, $ssdb) = @_;

  my $if_barcode = $if_sample->{'plate'};
  my $if_well = $if_sample->{'well'};

  my $ss_sample = $ssdb->find_infinium_sample($if_barcode, $if_well);
  my @ss_studies = @{$ssdb->find_infinium_studies($if_barcode, $if_well)};

  my @meta = ([sample => $ss_sample->{name}],
              [sample_id => $ss_sample->{internal_id}],
              ['dcterms:identifier' => $ss_sample->{sanger_sample_id}]);

  if (defined $ss_sample->{accession_number}) {
    push(@meta, [sample_accession_number => $ss_sample->{accession_number}]);
  }
  if (defined $ss_sample->{common_name}) {
    push(@meta, [sample_common_name => $ss_sample->{common_name}]);
  }

  foreach my $ss_study (@ss_studies) {
    push(@meta, [study_id => $ss_study->{internal_id}]);

    if (defined $ss_study->{study_title}) {
      push(@meta, [study_title => $ss_study->{study_title}]);
    }
  }

  return @meta;
}

sub make_infinium_metadata {
  my ($if_sample) = @_;

  return (['dcterms:identifier' => $if_sample->{'sample'}]);
}

sub make_file_metadata {
  my ($file, @suffixes) = @_;

  my ($basename, $dir, $suffix) = fileparse($file, @suffixes);

  my @result = run_command("md5sum $file");
  my $md5 = shift @result;
  $md5 =~ s/^(\S+)\s+\S+$/$1/;

  $suffix =~ s/^\.?//;

  my @meta = ([md5 => $md5],
              ['type' => $suffix]);

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
