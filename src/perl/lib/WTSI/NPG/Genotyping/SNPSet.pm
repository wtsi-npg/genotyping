
use utf8;

package WTSI::NPG::Genotyping::SNPSet;

use List::AllUtils qw(uniq);
use Log::Log4perl::Level;
use Moose;
use Text::CSV;

use WTSI::NPG::Genotyping::SNP;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::iRODS::Storable';

our @HEADER = qw(SNP_NAME REF_ALLELE ALT_ALLELE CHR POS STRAND);

has 'name' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   default  => sub { return ''} );

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

has 'quiet' => # reduced level of log output
  (is       => 'ro',
   isa      => 'Bool',
   default  => 0);

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

# BUILD is defined in the Storable Role
sub BUILD {
  my ($self) = @_;

  if ($self->quiet) { $self->logger->level($ERROR); }

  $self->_build_snps;
}

=head2 snp_names

  Arg [1]    : None

  Example    : $set->snp_names
  Description: Return a sorted array of the names of the SNPs in the set.
  Returntype : Array

=cut

sub snp_names {
  my ($self) = @_;

  my @snp_names;
  foreach my $snp (@{$self->snps}) {
    push @snp_names, $snp->name;
  }

  return sort { $a cmp $b } uniq @snp_names;
}

=head2 snp_names

  Arg [1]    : Str SNP name e.g. rs######

  Example    : $snp = $set->named_snp('rs0123456')
  Description: Return specific, named SNP from the set.
  Returntype : WTSI::NPG::Genotyping::SNP

=cut

sub named_snp {
  my ($self, $snp_name) = @_;

  defined $snp_name or
    $self->logconfess("A defined snp_name argument is required");
  $snp_name or
    $self->logconfess("A non-empty snp_name argument is required");

  return grep { $snp_name eq $_->name } @{$self->snps};
}

sub contains_snp {
  my ($self, $snp_name) = @_;

  return defined $self->named_snp($snp_name);
}

=head2 write_snpset_data

  Arg [1]    : Str file name

  Example    : $set->write_snpset_data('snpset.txt')
  Description: Write the content of the set to a file in the TSV format
               used by NPG.
  Returntype : Int number of records written (may be > number of unique
               SNP names for cases such as gender markers that have
               multiple locations on the reference genome).

=cut

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
