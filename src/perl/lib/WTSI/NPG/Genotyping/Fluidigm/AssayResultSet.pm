
use utf8;

package WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;

use English;
use List::AllUtils qw(uniq);
use Moose;
use Text::CSV;

use WTSI::NPG::Genotyping::Fluidigm::AssayResult;

with 'WTSI::NPG::Loggable';

has 'file_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 0);

has 'data_object' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::Fluidigm::AssayDataObject',
   required => 0);

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

sub BUILD {
  my ($self) = @_;

  unless ($self->data_object or $self->file_name) {
     $self->logconfess("Neither data_object nor file_name ",
                       "arguments were supplied to the constructor");
  }

  if ($self->data_object and $self->file_name) {
    $self->logconfess("Both data_object '", $self->data_object,
                      "' and file_name '", $self->file_name,
                      "' arguments were supplied to the constructor");
  }

  if ($self->data_object) {
    $self->data_object->is_present or
      $self->logconfess("Assay data file ", $self->data_object->absolute,
                        " is not present");
  }

  if ($self->file_name) {
    unless (-e $self->file_name) {
      $self->logconfess("Assay data file ", $self->file_name,
                        " is not present");
    }
  }
}

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

sub str {
  my ($self) = @_;

  if ($self->data_object) {
    return $self->data_object->str;
  }
  else {
    return $self->file_name
  }
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
       call           => $record->[8],
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
