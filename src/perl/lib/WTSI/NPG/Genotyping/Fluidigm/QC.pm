
package WTSI::NPG::Genotyping::Fluidigm::QC;

use Moose;

use Log::Log4perl;
use Set::Scalar;
use Text::CSV;
use Try::Tiny;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::Genotyping::Fluidigm::AssayDataObject;

our $VERSION = '';

our $PLATE_INDEX = 9;
our $WELL_INDEX = 10;
our $MD5_INDEX = 11;
our $EXPECTED_FIELDS_TOTAL = 12;

our $REPORTING_BLOCK_SIZE = 1000;

with 'WTSI::DNAP::Utilities::Loggable';

has 'checksums_by_path' =>
  (is       => 'ro',
   isa      => 'HashRef[Str]',
   documentation => 'The md5 checksum for each input iRODS path. '.
       'Automatically populated by the BUILDARGS method. Do not supply '.
       'this attribute as an argument; any value input will be '.
       'overwritten by BUILDARGS.',
);

has 'csv' =>
  (is       => 'ro',
   isa      => 'Text::CSV',
   init_arg => undef,
   lazy     => 1,
   default  => sub { return Text::CSV->new ({ binary => 1, }); },
   documentation => 'Object for processing data in CSV format',
);

has 'csv_path' =>
  (is       => 'ro',
   isa      => 'Maybe[Str]',
   documentation => 'Path for input of existing QC results. Optional; '.
       'if not defined, omit CSV input.',
);

has 'data_object_paths' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   required => 1,
   documentation => 'iRODS paths for results which may be added to QC',
);

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
 });

has 'paths_by_plate_well' =>
  (is       => 'ro',
   isa      => 'HashRef[HashRef[Str]]',
   documentation => 'Input iRODS paths, indexed by plate and well. '.
       'Automatically populated by the BUILDARGS method. Do not supply '.
       'this attribute as an argument; any value input will be '.
       'overwritten by BUILDARGS.',
);

around BUILDARGS => sub {
    # populate paths_indexed and path_checksums attributes
    # do so on a single pass, for greater efficiency on iRODS calls
    my ($orig, $class, @args) = @_;
    my %args;
    if ( @args == 1 && ref $args[0] ) { %args = %{$args[0]}; }
    else { %args = @args; }
    my %checksums;
    my %indexed;
    my $irods = $args{'irods'} || WTSI::NPG::iRODS->new;
    my $log =
        Log::Log4perl->get_logger("WTSI::NPG::Genotyping::Fluidigm::QC");
    my @data_object_paths = @{$args{'data_object_paths'}};
    my $total = scalar @data_object_paths;
    $log->info('Finding (plate, well) index and checksum for ', $total,
               ' data object paths');
    my $count = 0;
    foreach my $obj_path (@data_object_paths) {
        # can't use _get_fluidigm_data_obj, as it is an instance method
        my $data_obj;
        try {
            $data_obj =  WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
                ($irods, $obj_path);
        } catch {
            $log->logcroak("Unable to create Fluidigm DataObject from ",
                           "iRODS path '", $obj_path, "'");
        };
        my $checksum = $data_obj->checksum;
        my $plate = $data_obj->get_avu($FLUIDIGM_PLATE_NAME)->{'value'};
        my $well = $data_obj->get_avu($FLUIDIGM_PLATE_WELL)->{'value'};
        if (defined $checksums{$obj_path}) {
            $log->logcroak('iRODS data object path ', $obj_path,
                           ' appears more than once in inputs');
        } elsif (defined $indexed{$plate}{$well}) {
            $log->logcroak('Duplicate plate ', $plate, ' and well ',
                           $well, ' for data objects: ', $obj_path, ', ',
                           $indexed{$plate}{$well}
                       );
        }
        $checksums{$obj_path} = $checksum;
        $indexed{$plate}{$well} = $obj_path;
        $count++;
        if ($count % $REPORTING_BLOCK_SIZE == 0) {
            $log->debug('Found (plate, well) index and checksum for ',
                        $count, ' of ', $total, ' data object paths');
        }
    }
    $log->info('Finished processing ', $total, ' data object paths');
    $args{'checksums_by_path'} = \%checksums;
    $args{'paths_by_plate_well'} = \%indexed;

    return $class->$orig(%args);
};


=head2 csv_fields

  Arg [1]    : WTSI::NPG::Genotyping::Fluidigm::AssayDataObject

  Example    : my $fields = $qc->csv_update_fields($assay_data_object);

  Description: Find QC data for the given AssayDataObject, for CSV output.

               CSV format consists of the fields returned by the
               summary_string() method of
               WTSI::NPG::Genotyping::Fluidigm::AssayResultSet;
               and three additional fields, denoting the Fluidigm
               plate, Fluidigm well, and md5 checksum.

  Returntype : [ArrayRef] CSV fields for update

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
    my $status = $self->csv->combine(@{$fields});
    if (! defined $status) {
        $self->logcroak("Error combining CSV inputs: '",
                        $self->csv->error_input, "'");
    }
    return $self->csv->string();
}

=head2 rewrite_existing_csv

  Arg [1]    : Filehandle

  Example    : my $data_object_paths = $qc->rewrite_existing_csv($fh);

  Description: Read the existing CSV file, and write an updated version to the
               given filehandle. Records will be updated if there is a
               matching data object with the same plate and well, and a
               different checksum; otherwise the original record is output
               unchanged.

               Returns the set of data object paths which match
               the plate and well of an existing CSV record.

  Returntype : Set::Scalar

=cut

sub rewrite_existing_csv {
    my ($self, $out) = @_;
    my $existing_paths = Set::Scalar->new();
    if (! defined $self->csv_path) {
        $self->logwarn('Existing CSV path is not defined; cannot rewrite ',
                       'previous results');
        return $existing_paths;
    }
    my $matched = 0;
    my $updated = 0;
    my $total = 0;
    open my $in, "<", $self->csv_path ||
        $self->logcroak("Cannot open CSV path '", $self->csv_path, "'");
    while (<$in>) {
        my $original_csv_line = $_;
        chomp;
        my @fields = $self->_parse_csv_fields($_);
        my $plate = $fields[$PLATE_INDEX];
        my $well = $fields[$WELL_INDEX];
        my $update_path = $self->paths_by_plate_well->{$plate}{$well};
        if (defined $update_path) {
            $existing_paths->insert($update_path);
            $matched++;
            my $md5 = $fields[$MD5_INDEX];
            if ($md5 eq $self->checksums_by_path->{$update_path}) {
                $self->debug('No update for plate ', $plate, ', well ',
                             $well, '; md5 checksum is unchanged');
                print $out $original_csv_line;
            } else {
                $self->debug('Updating plate ', $plate, ', well ',
                             $well, ' from data object path',
                             $update_path);
                my $update_obj = $self->_get_fluidigm_data_obj($update_path);
                print $out $self->csv_string($update_obj)."\n";
                $updated++;
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
    return $existing_paths;
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

               Output for new data objects is sorted in (plate, well) order.

  Returntype : Returns True on completion

=cut

sub write_csv {
    my ($self, $out) = @_;
    my $existing_paths; # data object paths which match existing CSV records
    if (defined $self->csv_path) {
        $existing_paths = $self->rewrite_existing_csv($out);
    }
    my $total = 0;
    my @update_lines;
    foreach my $obj_path (@{$self->data_object_paths}) {
        if (defined $existing_paths && $existing_paths->has($obj_path)) {
            $self->debug('Object ', $obj_path, ' already exists in CSV');
        } else {
            $self->debug('Finding new CSV output for object ', $obj_path);
            my $data_obj = $self->_get_fluidigm_data_obj($obj_path);
            push @update_lines, $self->csv_string($data_obj)."\n";
            $total++;
        }
    }
    $self->info('Found ', $total, ' new CSV records for Fluidigm QC');
    my $sort_ref = $self->_by_plate_well();
    my @sorted_lines = sort $sort_ref @update_lines;
    $self->debug('Sorted ', $total, ' new records in (plate, well) order');
    foreach my $line (@sorted_lines) { print $out $line; }
    $self->debug('Wrote ', $total, ' new records to output');
    return 1;
}

sub _by_plate_well {
    # return a coderef used to sort CSV lines in (plate, well) order
    my ($self,) = @_;

    return sub {
        my @fields_a = $self->_parse_csv_fields($a);
	my @fields_b = $self->_parse_csv_fields($b);
	my $plate_a = $fields_a[$PLATE_INDEX];
	my $plate_b = $fields_b[$PLATE_INDEX];
	my $well_a = $fields_a[$WELL_INDEX];
	my $well_b = $fields_b[$WELL_INDEX];
	my @well_fields_a = split(/S[0]*/msx, $well_a);
	my $well_num_a = pop @well_fields_a;
	my @well_fields_b = split(/S[0]*/msx, $well_b);
	my $well_num_b = pop @well_fields_b;

	return $plate_a <=> $plate_b || $well_num_a <=> $well_num_b;
    };
}

sub _get_fluidigm_data_obj {
    # safely create a Fluidigm AssayDataObject from path
    my ($self, $obj_path) = @_;
    my $data_obj;
    try {
        $data_obj = WTSI::NPG::Genotyping::Fluidigm::AssayDataObject->new
            ($self->irods, $obj_path);
    } catch {
        $self->logcroak("Unable to create Fluidigm DataObject from ",
                        "iRODS path '", $obj_path, "'");
    };
    return $data_obj;
}

sub _parse_csv_fields {
    my ($self, $input) = @_;
    # parse input string and check it is a valid Fluidigm QC record
    $self->csv->parse($input);
    my @fields = $self->csv->fields();
    if (! @fields) {
        $self->logcroak("Unable to parse CSV input: '",
                        $self->csv->error_input(), "'");
    } elsif (scalar @fields != $EXPECTED_FIELDS_TOTAL) {
        $self->logcroak("Expected ", $EXPECTED_FIELDS_TOTAL,
                        " fields, found ", scalar @fields,
                        " from input: ", $input);
    }
    return @fields;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;


__END__

=head1 NAME

WTSI::NPG::Genotyping::Fluidigm::QC

=head1 DESCRIPTION

A class to process quality control metrics for Fluidigm results.

Find QC metric values from iRODS for CSV output. Optionally, can supply a
CSV file with existing QC records, which will be updated if the checksum of
the corresponding iRODS data object has changed.

Output consists of any existing records in their original order, followed
by new records in (plate, well) order. Each (plate, well) pair will have
exactly one record in the output.

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
