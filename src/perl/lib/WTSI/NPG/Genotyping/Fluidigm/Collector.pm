use utf8;

package WTSI::NPG::Genotyping::Fluidigm::Collector;

use DateTime;
use File::Temp;
use File::Spec;
use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw/md5sum/;

use Moose;

extends 'WTSI::NPG::Utilities::Collector';

our $VERSION = '';

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });

has 'irods_root' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   documentation => 'Root collection path in iRODS, eg. /seq'
  );


=head2 collect_archivable_dirs

  Arg [1]    : latest modification time to archive, in seconds since the epoch

  Example    : @dirs = $collector->collect_archivable_dirs();
  Description: Return an array of Fluidigm directory names which are ready
               for archiving.
  Returntype : array of strings (dir names)

=cut

sub collect_archivable_dirs {
    my ($self, $threshold) = @_;
    return $self->collect_dirs($self->test_archivable($threshold));
}


=head2 test_archivable

     Arg [1]    : latest modification time to archive, in seconds since
                  the epoch

     Example    : my $ok = $collector->test_archivable($threshold);

     Description: The return value will be used as a test, to determine if
                  a directory will be archived. So, the return value is a
                  reference to a function which itself returns a Boolean
                  value.

     Returntype : Coderef

=cut

sub test_archivable {
    my ($self, $threshold) = @_;
    my $time = DateTime->from_epoch(epoch => $threshold);
    my $time_str = $time->strftime("%Y-%m-%d_%H:%M:%S");

    return sub {
        my ($path) = @_;
        if ($self->last_modified_before($threshold)->($path)) {
            $self->debug("Last modification time of $path was earlier than ",
                         $time_str,
                         " , checking iRODS publication status");
            return $self->irods_publication_ok($path);
        } else {
            $self->debug("Last modification time of $path was later than ",
                         $time_str,
                         ", directory will not be archived");
            return 0;
        }
    }
}


=head2 irods_publication_ok

  Arg [1]    : (Str) Path of Fluidigm results directory to evaluate
  Example    : my $pub_ok = irods_publication_ok($params);
  Description: Check if the given Fluidigm results directory has been
               published to iRODS. Want to verify that local and iRODS
               CSV files are congruent: All files are present, with the
               same plate, well (if any) and md5 checksum.
  Returntype : Bool

=cut

sub irods_publication_ok {
    my ($self, $dir) = @_;
    my $resultset = WTSI::NPG::Genotyping::Fluidigm::ResultSet->new
        (directory => $dir);
    my $export_file = WTSI::NPG::Genotyping::Fluidigm::ExportFile->new
        (file_name => $resultset->export_file);
    my $publication_ok = 1;
    # find CSV results for given Fluidigm plate in iRODS
    my @results = $self->irods->find_objects_by_meta(
        $self->irods_root,
        [ fluidigm_plate => $export_file->fluidigm_barcode ],
        [ type           => 'csv' ],
    );
    my $result_total = scalar(@results);
    if ($result_total == 0) {
        $self->warn("No results found in iRODS for directory '", $dir, "'");
        $publication_ok = 0;
    } else {
        # expect one file for each well address, plus the original export file
        my $expected = scalar(@{$export_file->addresses}) + 1;
        $self->debug("Checking publication status for ", $result_total,
                     " results from iRODS");
        if ($result_total != $expected) {
            $self->error("File totals for plate '",
                         $export_file->fluidigm_barcode,
                         "' do not match; expected ", $expected,
                         " results in iRODS, found ", $result_total);
            $publication_ok = 0;
        }
        # check well (if any) and md5 for each iRODS result found
        my %md5_by_address = $self->_get_md5_by_address($export_file);
        foreach my $result (@results) {
            # look at address and md5 in iRODS metadata
            my ($address, $irods_md5);
            foreach my $avu ($self->irods->get_object_meta($result)) {
                my $attribute = $avu->{'attribute'};
                if ($attribute eq 'fluidigm_well') {
                    $address = $avu->{'value'};
                } elsif ($attribute eq 'md5') {
                    $irods_md5 = $avu->{'value'};
                }
            }
            if (defined($address)) {
                if ($irods_md5 ne $md5_by_address{$address}) {
                    $self->error
                        ("Local and iRODS checksums do not match for plate '",
                         $export_file->fluidigm_barcode, "' well '",
                         $address, "'");
                    $publication_ok = 0;
                }
            } elsif (md5sum($resultset->export_file) ne $irods_md5) {
                $self->error
                    ("Local and iRODS checksums do not match for plate '",
                     $export_file->fluidigm_barcode, "export file '",
                     $export_file->file_name, "'");
                $publication_ok = 0;
            }
        }
    }
    $self->debug("iRODS publication status for '", $dir, "': ",
                 $publication_ok);
    return $publication_ok;
}

sub _get_md5_by_address {
    my ($self, $export_file) = @_;
    my $tmpdir = File::Temp->newdir("fluidigm_samples_XXXXXX",
                                    dir => File::Spec->tmpdir() );
    my %md5_by_address;
    foreach my $address (@{$export_file->addresses}) {
        my $filename = $export_file->fluidigm_filename($address);
        my $file = File::Spec->catfile($tmpdir, $filename);
        my $records = $export_file->write_assay_result_data($address, $file);
        my $md5 = md5sum($file);
        $md5_by_address{$address} = $md5;
        $self->debug("Wrote ", $records, " records for address ",
                     $address, " into temp file ", $file,
                     ", with md5 ", $md5);
    }
    return %md5_by_address;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG:::Genotyping::Fluidigm::Collector

=head1 DESCRIPTION

Subclass of WTSI::NPG::Utilities::Collector, to collect Fluidigm data
for archiving. Includes method to verify iRODS publication.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
