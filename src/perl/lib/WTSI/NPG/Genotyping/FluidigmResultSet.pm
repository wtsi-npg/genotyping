
package WTSI::NPG::Genotyping::FluidigmResultSet;

use Moose;
use namespace::autoclean;

with 'WTSI::NPG::Loggable';

# class to represent Fluidigm result set
# a result set consists of a directory and set of files
# files include: 
## .csv export file
## .tif files (3 different files in Data subdirectory)
## ChipRun files (various) 



# attributes for paths/filenames
# sanity checking that required files are present


our $DATA_DIRECTORY_NAME = 'Data';
our $EXPECTED_TIF_TOTAL = 3;

has 'directory' => (is  => 'ro', isa => 'Str', required => 1,
                    writer => '_directory');
has 'data_directory' => (is  => 'ro', isa => 'Str',
			 writer => '_data_directory');
has 'export_path' => (is  => 'ro', isa => 'Str',
		      writer => '_export_path');
has 'tif_paths' => (is => 'ro', isa => 'ArrayRef[Str]',
			writer => '_tif_paths');
#has 'misc_filenames' => (is => 'ro', isa => 'ArrayRef[Str]',
#			 writer => '_misc_filenames');
has 'fluidigm_barcode' =>(is  => 'ro', isa => 'Str',
			  writer => '_fluidigm_barcode');

# constructor: give a directory path, check for appropriate files

sub BUILD {
    my ($self) = @_;

    if (!(-e $self->directory)) {
	 $self->logdie("Fluidigm directory path '", $self->directory,
		       "' does not exist");
    } elsif (!(-d $self->directory)) {
	$self->logdie("Fluidigm directory path '", $self->directory,
		      "' is not a directory");
    }
    $self->_data_directory($self->directory .'/'. $DATA_DIRECTORY_NAME);
    if (!(-e $self->data_directory)) {
	$self->logdie("Fluidigm data subdirectory '", $self->data_directory,
		       "' does not exist");
    } elsif (!(-d $self->data_directory)) {
	$self->logdie("Fluidigm data subdirectory '", $self->data_directory,
		      "' is not a directory");
    }
    my @tif = glob($self->data_directory.'/*\.{tif,tiff}');
    if (@tif!=$EXPECTED_TIF_TOTAL) {
	$self->logdie("Should have exactly $EXPECTED_TIF_TOTAL .tif files in ".$self->data_directory);
    } else {
	$self->_tif_paths(\@tif);
    }
    # TODO Validate headers of .tif files ?
    my @export = glob($self->directory.'/*\.csv');
    if (@export!=1) {
	$self->logdie("Should have exactly 1 .csv export file in ".$self->directory);
    } else {
	$self->_export_path((shift(@export)));
    }
    # TODO if multiple .csv files are present, look for one with a valid export header !


}



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

=cut
