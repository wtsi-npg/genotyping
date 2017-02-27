
package WTSI::NPG::Genotyping::Fluidigm::QC;

use Moose;

use Set::Scalar;
use Text::CSV;

use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;
use WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;

our $VERSION = '';

our $PLATE_INDEX = 9;
our $WELL_INDEX = 10;
our $MD5_INDEX = 11;
our $EXPECTED_FIELDS_TOTAL = 12;

with 'WTSI::DNAP::Utilities::Loggable';

has 'data_objects' =>
  (is       => 'ro',
   isa      => 'ArrayRef[WTSI::NPG::Genotyping::Fluidigm::AssayDataObject]',
   required => 1,
   documentation => 'AssayDataObjects for results which may be added to QC',
);

has 'data_objects_indexed' =>
  (is       => 'ro',
   isa      => 'HashRef',
   lazy     => 1,
   builder  => '_build_data_objects_indexed',
   init_arg => undef,
   documentation => 'Input AssayDataObjects, indexed by plate and well.',
);

has 'csv_path' =>
  (is       => 'ro',
   isa      => 'Maybe[Str]',
   documentation => 'Path for input of existing QC results. Optional; '.
       'if not defined, omit CSV input.',
);

=head2 csv_fields

  Arg [1]    : WTSI::NPG::Genotyping::Fluidigm::AssayDataObject

  Example    : my $fields = $qc->csv_update_fields($assay_data_object);

  Description: Find QC data for the given AssayDataObject, for CSV output.

               CSV format consists of the fields returned by the
               summary_string() method of
               WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
               and three additional fields, denoting the Fluidigm
               plate, Fluidigm well, and md5 checksum.

  Returntype : ArrayRef: CSV fields for update

=cut

sub csv_fields {
    my ($self, $obj) = @_;
    my @fields = @{$obj->assay_resultset->summary_fields};
    # Find Fluidigm plate/well from object metadata
    my ($plate, $well);
    my $plate_avu = $obj->get_avu($FLUIDIGM_PLATE_NAME);
    my $well_avu = $obj->get_avu($FLUIDIGM_PLATE_WELL);
    if ($plate_avu) {
        $plate = $plate_avu->{'value'};
    } else {
        $self->logcroak("$FLUIDIGM_PLATE_NAME AVU not found for data ",
                        "object '", $obj->str, "'");
    }
    if ($well_avu) {
        $well = $well_avu->{'value'};
    } else {
        $self->logcroak("$FLUIDIGM_PLATE_WELL AVU not found for data ",
                        "object '", $obj->str, "'");
    }
    # Append plate, well, and md5 checksum
    push @fields, $plate, $well, $obj->checksum;
    return \@fields;
}

=head2 csv_string

  Arg [1]    : WTSI::NPG::Genotyping::Fluidigm::AssayDataObject

  Example    : my $str = $qc->csv_string($assay_data_object);
  Description: Find updated QC data for the given AssayDataObject.
               Return string for CSV output.
  Returntype : Str

=cut

sub csv_string {
    my ($self, $assay_data_object) = @_;
    my $fields = $self->csv_fields($assay_data_object);
    my $csv = Text::CSV->new ( { binary => 1 } );
    my $status = $csv->combine(@{$fields});
    if (! defined $status) {
        $self->logcroak("Error combining CSV inputs: '",
                        $csv->error_input, "'");
    }
    return $csv->string();
}

=head2 rewrite_existing_csv

  Arg [1]    : Filehandle

  Example    : my $checksums = $qc->rewrite_existing_csv($fh);

  Description: Read the existing CSV file, and write an updated version to the
               given filehandle. Records will be updated if there is a
               matching data object with the same plate and well, and a
               different checksum; otherwise the original record is output
               unchanged.

               Returns the set of md5 sums for data objects which match
               the plate and well of an existing CSV record -- regardless
               of whether the md5 sum differs.

  Returntype : Set::Scalar

=cut

sub rewrite_existing_csv {
    my ($self, $out) = @_;
    my $existing_checksums = Set::Scalar->new();
    my $csv = Text::CSV->new ( { binary => 1 } );
    my $matched = 0;
    my $updated = 0;
    my $total = 0;
    open my $in, "<", $self->csv_path ||
        $self->logcroak("Cannot open CSV path '", $self->csv_path, "'");
    while (<$in>) {
        my $original_csv_line = $_;
        chomp;
        $csv->parse($_);
        my @fields = $csv->fields();
        if (! @fields) {
            $self->logcroak("Unable to parse CSV line: '",
                            $csv->error_input, "'");
        }
        if (scalar @fields != $EXPECTED_FIELDS_TOTAL) {
            $self->logcroak("Expected ", $EXPECTED_FIELDS_TOTAL,
                            " fields, found ", scalar @fields,
                            " from input: ", $_);
        }
        my $plate = $fields[$PLATE_INDEX];
        my $well = $fields[$WELL_INDEX];
        my $update_obj = $self->data_objects_indexed->{$plate}{$well};
        if (defined $update_obj) {
            $existing_checksums->insert($update_obj->checksum);
            $matched++;
            my $md5 = $fields[$MD5_INDEX];
            if ($md5 eq $update_obj->checksum) {
                $self->debug('No update for plate ', $plate, ', well ',
                             $well, '; md5 checksum is unchanged');
                print $out $original_csv_line;
            } else {
                $self->debug('Updating plate ', $plate, ', well ',
                             $well, ' from data object ',
                             $update_obj->str);
                $updated++;
                print $out $self->csv_string($update_obj)."\n";
            }
        } else {
            $self->debug('No update for plate ', $plate, ', well ',
                         $well, '; no corresponding data object was found');
            print $out $original_csv_line;
        }
        $total++;
    }
    close $in ||
        $self->logcroak("Cannot close CSV path '", $self->csv_path, "'");
    $self->info('Rewrote ', $total, ' existing CSV records for Fluidigm ',
                'QC; matched ', $matched, ' data objects; updated ',
                $updated, ' records');
    return $existing_checksums;
}


=head2 write_csv

  Arg [1]    : Filehandle for output

  Example    : $qc->write_csv($fh);

  Description: Write an updated CSV to the given filehandle. Output
               consists of records in the existing CSV file,
               updated as appropriate; and records for any new data
               objects which do not appear in the existing file. (If the
               existing CSV file is not defined, this method simply writes
               CSV records for all data objects.)

  Returntype : None

=cut

sub write_csv {
    my ($self, $out) = @_;
    my $checksums;
    if (defined $self->csv_path) {
        $checksums = $self->rewrite_existing_csv($out);
    }
    my $total = 0;
    foreach my $obj (@{$self->data_objects}) {
        if (defined $checksums && $checksums->has($obj->checksum)) {
            $self->debug('Object ', $obj->str, 'already exists in CSV');
        } else {
            $self->debug('Writing new CSV output for object ', $obj->str);
            print $out $self->csv_string($obj)."\n";
            $total++;
        }
    }
    $self->info('Wrote ', $total, ' new CSV records for Fluidigm QC');
    return 1;
}

sub _build_data_objects_indexed {
    my ($self,) = @_;
    my %indexed;
    foreach my $obj (@{$self->data_objects}) {
        my $plate = $obj->get_avu($FLUIDIGM_PLATE_NAME)->{'value'};
        my $well = $obj->get_avu($FLUIDIGM_PLATE_WELL)->{'value'};
        if ($indexed{$plate}{$well}) {
            $self->logcroak("Duplicate (plate, well) = (",
                            $plate, ", ", $well, ") for data object '",
                            $obj->str, "'");
        }
        $indexed{$plate}{$well} = $obj;
    }
    return \%indexed;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;


__END__

=head1 NAME

WTSI::NPG::Genotyping::Fluidigm::QC

=head1 DESCRIPTION

A class to process quality control metrics for Fluidigm results.

Find QC metric values for CSV output. Ensure QC values for the
same data object are not written more than once, by comparing md5 checksums.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
