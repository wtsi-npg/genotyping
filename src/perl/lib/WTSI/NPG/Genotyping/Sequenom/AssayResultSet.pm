use utf8;

package WTSI::NPG::Genotyping::Sequenom::AssayResultSet;

use List::AllUtils qw(uniq);
use Moose;
use Text::CSV;

use WTSI::NPG::Genotyping::Sequenom::AssayResult;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::iRODS::Storable',
  'WTSI::NPG::Genotyping::Annotation';

our $VERSION = '';

our $HEADER = "ALLELE\tASSAY_ID\tCHIP\tCUSTOMER\tEXPERIMENT\tGENOTYPE_ID\tHEIGHT\tMASS\tPLATE\tPROJECT\tSAMPLE_ID\tSTATUS\tWELL_POSITION";

has '+data_object' =>
  (isa      => 'WTSI::NPG::Genotyping::Sequenom::AssayDataObject');

has 'assay_results' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Genotyping::Sequenom::AssayResult]',
   required => 1,
   builder  => '_build_assay_results',
   lazy     => 1);

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # Permit an AssayDataObject as an anonymous argument mapping to data_object
  # Permit a Str as an anonymous argument mapping to file_name
  if (@args == 1 and
      ref $args[0] eq 'WTSI::NPG::Genotyping::Sequenom::AssayDataObject') {
    return $class->$orig(data_object => $args[0]);
  }
  elsif (@args == 1 and !ref $args[0]) {
    return $class->$orig(file_name => $args[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub size {
  my ($self) = @_;

  return scalar @{$self->assay_results};
}


=head2 canonical_sample_id

  Arg [1]    : None

  Example    : $result->canonical_sample_id
  Description: Return the canonical identifier of the sample analysed.
               This method raises an error if it encounters multiple
               sample identifiers.
  Returntype : Str

=cut

sub canonical_sample_id {
  my ($self) = @_;

  my @names = uniq map { $_->canonical_sample_id }
    grep { ! $_->is_empty } @{$self->assay_results};

  if (scalar @names > 1) {
    $self->logconfess("Assay result set '", $self->str, "' contains data for ",
                      ">1 sample: [", join(', ', @names), "]");
  }

  return shift @names;
}


sub snpset_name {
  my ($self) = @_;

  defined $self->data_object or
    $self->logconfess("Failed to determine SNP set name: '", $self->str,
                      "' is not in iRODS");

  my @snpset_names = $self->data_object->find_in_metadata
    ($self->sequenom_plex_name_attr);
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
  Returntype : ArrayRef[Str]

=cut

sub snp_names {
  my ($self) = @_;

  my @snp_names;
  foreach my $result (@{$self->assay_results}) {
    push @snp_names, $result->snp_assayed;
  }

  @snp_names = sort { $a cmp $b } uniq @snp_names;

  return \@snp_names;
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

    if ($str eq $HEADER) {
      $self->debug("Found file header");
      next;
    }

    # Ignore empty lines
    if ($str =~ m{^\s*$}msx) {
      next;
    }

    $self->debug("Building a new result from '$str'");

    my $num_fields = scalar @$record;
    unless ($num_fields == 13) {
      $self->logconfess("Invalid Sequenom record '$str': ",
                        "expected 13 fields but found $num_fields");
    }

    push @records, WTSI::NPG::Genotyping::Sequenom::AssayResult->new
      (allele        => $record->[0],
       assay_id      => $record->[1],
       chip          => $record->[2],
       customer      => $record->[3],
       experiment    => $record->[4],
       genotype_id   => $record->[5],
       height        => $record->[6],
       mass          => $record->[7],
       plate         => $record->[8],
       project       => $record->[9],
       sample_id     => $record->[10],
       status        => $record->[11],
       well_position => $record->[12],
       str           => $csv->string);
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

WTSI::NPG::Genotyping::Sequenom::AssayResultSet

=head1 DESCRIPTION


=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
