use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;

use English;
use List::AllUtils qw(uniq);
use Moose;
use Text::CSV;

use WTSI::NPG::Genotyping::Fluidigm::AssayResult;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::iRODS::Storable';

has '+data_object' =>
  (isa      => 'WTSI::NPG::Genotyping::Fluidigm::AssayDataObject');

has 'assay_results' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Genotyping::Fluidigm::AssayResult]',
   required => 1,
   builder  => '_build_assay_results',
   lazy     => 1);

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # Permit an AssayDataObject as an anonymous argument mapping to data_object
  # Permit a Str as an anonymous argument mapping to file_name
  if (@args == 1 and
      ref $args[0] eq 'WTSI::NPG::Genotyping::Fluidigm::AssayDataObject') {
    return $class->$orig(data_object => $args[0]);
  }
  elsif (@args == 1 and !ref $args[0]) {
    return $class->$orig(file_name => $args[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

=head2 snpset_name

  Arg [1]    : None

  Example    : $result->snpset_name
  Description: Return the name of the SNP set annotated in the iRODS
               metadata. Fails if the data backing the result set is not
               an iRODS data object.
  Returntype : Str

=cut

sub snpset_name {
  my ($self) = @_;

  defined $self->data_object or
    $self->logconfess("Failed to determine SNP set name: '", $self->str,
                      "' is not in iRODS");

  my @snpset_names = $self->data_object->find_in_metadata('fluidigm_plex');
  my $num_names = scalar @snpset_names;

  $num_names > 0 or
    $self->logconfess("No SNP sets defined in metadata of '", $self->str, "'");
  $num_names == 1 or
    $self->logconfess("$num_names SNP sets defined in metadata of '",
                      $self->str, "': [", join(', ', @snpset_names), "]");

  my $avu = shift @snpset_names;

  return $avu->{value};
}

=head2 snp_names

  Arg [1]    : None

  Example    : $result->snp_names
  Description: Return a sorted array of the names of the SNPs assayed in
               this result set.
  Returntype : Array

=cut

sub snp_names {
  my ($self) = @_;

  my @snp_names;
  foreach my $result (@{$self->assay_results}) {
    # Some wells may have no template and therefore no SNP being assayed.
    if ($result->snp_assayed) {
      push @snp_names, $result->snp_assayed;
    }
  }

  return sort { $a cmp $b } uniq @snp_names;
}

=head2 filter_on_confidence

  Arg [1]    : Num confidence threshold to compare using >=

  Example    : @confident = $result->filter_on_confidence(0.9)
  Description: Return assay results with confidence >= the specified value.
  Returntype : ArrayRef[WTSI::NPG::Genotyping::Fluidigm::AssayResult]

=cut

sub filter_on_confidence {
  my ($self, $confidence_threshold) = @_;

  defined $confidence_threshold or
    $self->logconfess('The confidence_threshold argument was not defined');

  my @filtered_results;
  foreach my $result (sort { $a->snp_assayed cmp
                             $b->snp_assayed } @{$self->assay_results}) {
    if ($result->confidence >= $confidence_threshold and !$result->is_control) {
      push @filtered_results, $result;
    }
  }

  return \@filtered_results;
}

sub _build_assay_results {
  my ($self) = @_;

  my $fh;

  if ($self->data_object) {
    my $content = $self->data_object->slurp;
    open $fh, '<', \$content
      or $self->logconfess("Failed to open content string for reading: $!");
  }
  elsif ($self->file_name) {
    open $fh, '<:encoding(utf8)', $self->file_name or
      $self->$self->logconfess("Failed to open file '", $self->file_name,
                               "' for reading: $!");
  }

  my $records = $self->_parse_assay_results($fh);

  close $fh or $self->logwarn("Failed to close a string handle");

  return $records;
}

sub _parse_assay_results {
  my ($self, $fh) = @_;

  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});

  my @records;
  while (my $record = $csv->getline($fh)) {
    $csv->combine(@$record);

    my $str = $csv->string;
    chomp $str;
    $self->debug("Building a new result from '$str'");

    my $num_fields = scalar @$record;
    unless ($num_fields == 12) {
      $self->logconfess("Invalid Fluidigm record '$str': ",
                        "expected 12 fields but found $num_fields");
    }

    push @records, WTSI::NPG::Genotyping::Fluidigm::AssayResult->new
      (assay          => $record->[0],
       snp_assayed    => $record->[1],
       x_allele       => $record->[2],
       y_allele       => $record->[3],
       sample_name    => $record->[4],
       type           => $record->[5],
       auto           => $record->[6],
       confidence     => $record->[7],
       final          => $record->[8],
       converted_call => $record->[9],
       x_intensity    => $record->[10],
       y_intensity    => $record->[11],
       str            => $csv->string);
  }
  $csv->eof or
    $self->logconfess("Parse error within '", $self->str, "': ",
                      $csv->error_diag);

  return \@records;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::Fluidigm::AssayResultSet

=head1 DESCRIPTION

A class which represents the result of a Fluidigm assay on one sample
for a number of SNPs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
