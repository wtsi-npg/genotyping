
package WTSI::NPG::Genotyping::Fluidigm::ResultSet;

use Moose;

with 'WTSI::NPG::Loggable';

our $DATA_DIRECTORY_NAME = 'Data';
our $EXPECTED_TIF_TOTAL = 3;

has 'directory' => (is  => 'ro', isa => 'Str', required => 1,
                    writer => '_directory');
has 'data_directory' => (is  => 'ro', isa => 'Str',
			 writer => '_data_directory');
has 'export_file' => (is  => 'ro', isa => 'Str',
		      writer => '_export_file');
has 'tif_files' => (is => 'ro', isa => 'ArrayRef[Str]',
		    writer => '_tif_files');
has 'fluidigm_barcode' =>(is  => 'ro', isa => 'Str',
			  writer => '_fluidigm_barcode');

sub BUILD {
    my ($self) = @_;
    # validate main directory
    if (!(-e $self->directory)) {
      $self->logconfess("Fluidigm directory path '", $self->directory,
                        "' does not exist");
    } elsif (!(-d $self->directory)) {
      $self->logconfess("Fluidigm directory path '", $self->directory,
                        "' is not a directory");
    }
    # find barcode (identical to directory name, by definition)
    my @terms = split(/\//, $self->directory);
    if ($terms[-1] eq '') { pop @terms; } # in case of trailing / in path
    $self->_fluidigm_barcode(pop(@terms));
    # validate data subdirectory
    $self->_data_directory($self->directory .'/'. $DATA_DIRECTORY_NAME);
    if (!(-e $self->data_directory)) {
      $self->logconfess("Fluidigm data path '", $self->data_directory,
                        "' does not exist");
    } elsif (!(-d $self->data_directory)) {
      $self->logconfess("Fluidigm data path '", $self->data_directory,
                        "' is not a directory");
    }
    # find .tif files
    my @tif = glob($self->data_directory.'/*\.{tif,tiff}');
    if (@tif!=$EXPECTED_TIF_TOTAL) {
      $self->logconfess("Should have exactly $EXPECTED_TIF_TOTAL .tif ",
                        "files in ".$self->data_directory);
    } else {
      $self->_tif_files(\@tif);
    }
    # look for export .csv file
    $self->_export_file($self->directory.'/'.$self->fluidigm_barcode.'.csv');
    if (!(-e $self->export_file)) {
      $self->logconfess("Fluidigm export .csv '", $self->export_file,
                        "' does not exist");
    }
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;


=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

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

=head1 DESCRIPTION

Class to represent a Fluidigm result set. The result set is a directory 
which must contain an export .csv file, and a data subdirectory with .tif 
files. The directory may also contain other files and subdirectories.

=cut
