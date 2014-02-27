
use utf8;

package WTSI::NPG::Genotyping::SNPSet;

use List::AllUtils qw(uniq);
use Moose;
use Text::CSV;

use WTSI::NPG::Genotyping::SNP;

with 'WTSI::NPG::Loggable';

our @HEADER = qw(SNP_NAME REF_ALLELE ALT_ALLELE CHR POS STRAND);

has 'file_name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 0);

has 'data_object' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::DataObject',
   required => 0);

has 'column_names' =>
  (is      => 'ro',
   isa     => 'ArrayRef[Str]',
   writer  => '_write_column_names',
   default => sub { return [@HEADER] });

has 'snps' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Genotyping::SNP]',
   required => 1,
   builder  => '_build_snps',
   lazy     => 1);

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # Permit a DataObject as an anonymous argument mapping to data_object
  # Permit a Str as an anonymous argument mapping to file_name
  if (@args == 1 &&
      ref $args[0] eq 'WTSI::NPG::iRODS::DataObject') {
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
      $self->logconfess("SNP set data file ", $self->data_object->absolute,
                        " is not present");
  }

  if ($self->file_name) {
    unless (-e $self->file_name) {
      $self->logconfess("SNP set data file ", $self->file_name,
                        " is not present");
    }
  }

  $self->_build_snps;
}

sub snp_names {
  my ($self) = @_;

  my @snp_names;
  foreach my $snp (@{$self->snps}) {
    push @snp_names, $snp->name;
  }

  return sort { $a cmp $b } uniq @snp_names;
}

sub named_snp {
  my ($self, $snp_name) = @_;

  defined $snp_name or
    $self->logconfess("A defined snp_name argument is required");
  $snp_name or
    $self->logconfess("A non-empty snp_name argument is required");

  return grep { $snp_name eq $_->name } @{$self->snps};
}

sub write_snpset_data {
  my ($self, $file_name) = @_;

  defined $file_name or
    $self->logconfess("A defined file_name argument is required");

  my $records_written = 0;
  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});
  $csv->column_names($self->column_names);

  open my $out, '>:encoding(utf8)', $file_name
    or $self->logcroak("Failed to open SNP set file '$file_name' ",
                       "for writing: $!");

  $csv->print($out, $self->column_names);

  foreach my $snp (@{$self->snps}) {
    $csv->print($out, [$snp->name, $snp->ref_allele, $snp->alt_allele,
                       $snp->chromosome, $snp->position, $snp->strand])
      or $self->logcroak("Failed to write record [", $snp->str,
                         "] to '$file_name': ", $csv->error_diag);
    ++$records_written;
  }

  close $out;

  return $records_written;
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

sub _build_snps {
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

  my $records = $self->_parse_snps($fh);

  close $fh or $self->logwarn("Failed to close a string handle");

  return $records;
}

sub _parse_snps {
  my ($self, $fh) = @_;

  my $csv = Text::CSV->new({eol              => "\n",
                            sep_char         => "\t",
                            allow_whitespace => undef,
                            quote_char       => undef});

  my $header = $csv->getline($fh);
  unless ($header and @$header) {
    $self->logconfess("SNPSet data file '", $self->str, "' is empty");
  }

  unless ($header->[0] =~ m{SNP_NAME}) {
    $self->logconfess("SNPSet data file '", $self->str,
                      "' is missing its header");
  }

  $self->_write_column_names($header);

  my @records;
  while (my $record = $csv->getline($fh)) {
    $csv->combine(@$record);

    my $str = $csv->string;
    chomp $str;
    $self->debug("Building a new SNP set record from '$str'");

    my $num_fields = scalar @$record;
    unless ($num_fields == 6) {
      $self->logconfess("Invalid SNP set record '$str': ",
                        "expected 6 fields but found $num_fields");
    }

    push @records, WTSI::NPG::Genotyping::SNP->new
      (name       => $record->[0],
       ref_allele => $record->[1],
       alt_allele => $record->[2],
       chromosome => $record->[3],
       position   => $record->[4],
       strand     => $record->[5],
       str        => $csv->string);
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

WTSI::NPG::Genotyping::SNPSet - Information on a set of SNPs used
in genotyping analyses

=head1 SYNOPSIS

  my $irods = WTSI::NPG::iRODS->new;
  my $data_object = WTSI::NPG::iRODS::DataObject->new
    ($irods, "/seq/fluidigm/multiplex/qc.csv");

  my $plex = WTSI::NPG::Genotyping::SNPSet->new
    (data_object => $data_object);

=head1 DESCRIPTION

A wrapper for the CSV data files used to contain sets of SNP
information. It performs an eager parse of the file and provides
methods to access and manipulate the data.

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
