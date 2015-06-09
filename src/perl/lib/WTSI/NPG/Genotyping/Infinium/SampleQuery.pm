use utf8;

package WTSI::NPG::Genotyping::Infinium::SampleQuery;

use Moose;

use warnings;
use strict;
use Log::Log4perl;
use Log::Log4perl::Level;
use Pod::Usage;

use WTSI::NPG::Database::Warehouse;
use WTSI::NPG::Genotyping::Database::Infinium;
use WTSI::NPG::iRODS;
use WTSI::NPG::Utilities qw(user_session_log);

our $VERSION = '';

our $EXPECTED_IRODS_FILES = 3;
our @DATA_SOURCE_NAMES = qw(LIMS_ IRODS SS_WH);
our @HEADER_FIELDS = qw(data_source plate well sample infinium_beadchip
                        infinium_beadchip_section sequencescape_barcode);

with 'WTSI::DNAP::Utilities::Loggable';

has 'infinium_db'  =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Genotyping::Database::Infinium',
   required => 1);

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1,
   default  => sub {
     return WTSI::NPG::iRODS->new;
   });


has 'sequencescape_db'  =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::Database::Warehouse',
   required => 1);

sub BUILD {
  my ($self) = @_;

  # Make our irods handle use our logger by default
  $self->irods->logger($self->logger);
}

=head2 run

  Arg [1]    : string project name
  Arg [2]    : string iRODS root path
  Arg [3]    : string output path (or - for STDOUT) (optional)
  Arg [4]    : boolean, write header to output (if any) (optional)
  Arg [5]    : integer, maximum samples to query (optional)
  Example    : $sq->run('my_project', '/archive', '-', 1, 5)
  Description: Run a query against the Infinium LIMS, iRODS, and
               SequenceScape database for a given project. Retrieve
               information for each sample in the project. Optionally,
               can write results to file or STDOUT, and/or log any
               discrepancies.
  Returntype : null
  Caller     : query_project_samples.pl

=cut

sub run {
    my ($self, $project, $root, $outpath, $header, $limit) = @_;
    unless ($project && $root) {
        # sanity check; main validation of arguments occurs in calling script
        $self->logcroak("Project and iRODS root arguments are required");
    }

    # query data sources: Infinium LIMS, iRODS, SequenceScape warehouse
    my @infinium_data = $self->_find_infinium_data($project);
    my $if_total = scalar @infinium_data;
    $self->info("Found ", $if_total, " Infinium samples.");
    if (defined($limit)) {
        if ($limit < 0) {
            $self->logcroak("limit argument must be >= 0");
        } elsif ($limit >= $if_total) {
            $self->info("Sample limit of ", $limit, " is not less than ",
                        "number found; continuing with all samples.");
        } else {
            @infinium_data = splice(@infinium_data, 0, $limit);
            $self->info("Reduced list of samples to match limit of ", $limit);
            $if_total = $limit;
        }
    }
    my @irods_data = $self->_find_irods_metadata(\@infinium_data, $root);
    my $irods_total = scalar @irods_data;
    $self->info("Found ", $irods_total, " iRODS samples.");
    my @warehouse_data = $self->_find_warehouse_data(\@infinium_data);
    my $wh_total = scalar @warehouse_data;
    $self->info("Found ", $wh_total, " warehouse samples.");

    # compare sample totals and output
    if ($if_total == $irods_total && $irods_total == $wh_total) {
        $self->info("Sample counts in Infinium, iRODS and ",
                    "Sequencescape are identical: ", $if_total,
                    " samples found.");
        if ($outpath) {
            if ($outpath eq '-') { $self->info("Writing output to STDOUT"); }
            else { $self->info("Writing output to $outpath"); }
            my @all_results = ( [@infinium_data], [@irods_data],
                                [@warehouse_data] );
            $self->_write_output(\@all_results, $outpath, $header);
        }
    } else {
        $self->logcroak("Sample counts do not match: Found ", $if_total,
                        " in Infinium, ", $irods_total, " in iRODS, ",
                        $wh_total, " in Sequencescape.");
    }

    return;
}

sub _find_infinium_data {
    # query Infinium LIMS DB to get samples
    # extract relevant fields and store in array of arrays
    my ($self, $project) = @_;
    my @if_samples = @{$self->infinium_db->find_project_samples
                           ($project)};
    my @data;
    foreach my $if_sample (@if_samples) {
        push @data, [$if_sample->{'plate'},
                     $if_sample->{'well'},
                     $if_sample->{'sample'},
                     $if_sample->{'beadchip'},
                     $if_sample->{'beadchip_section'},
                     '' # placeholder for WH barcode
                  ];
    }
    return @data;
}

sub _find_irods_metadata {
    # given a list of metadata values, cross-check with irods
    # root is the path to an irods zone, eg. /archive
    # for each input, require:
    # - 3 files in iRODS: red IDAT, green IDAT, GTC
    # - Consistent values for each field between 3 files
    # Required fields (all correspond to Infinium LIMS values):
    # - infinium_plate
    # - infinium_well
    # - infinium_sample
    # - beadchip
    # - beadchip_section
    # if metadata is incorrect or missing for a given sample,
    #  return empty values
    my ($self, $inputs_ref, $root) = @_;
    my @metadata;
    foreach my $input (@{$inputs_ref}) {
        my ($plate, $well, $sample, $beadchip, $beadchip_section) = @{$input};
        my @irods_query = (['infinium_plate', $plate ],
                           ['infinium_well', $well],
                           ['infinium_sample', $sample],
                           ['beadchip', $beadchip],
                           ['beadchip_section', $beadchip_section]
                       );
        my @results = $self->irods->find_objects_by_meta($root, @irods_query);
        my $total = scalar @results;
        $self->debug("Found ", $total, " iRODS results for plate ",
                     $plate, ", well ", $well, ", sample ", $sample);
        if ($total != $EXPECTED_IRODS_FILES) {
            $self->logwarn("Expected ", $EXPECTED_IRODS_FILES,
                           " files in iRODS, found ", $total,
                           " for plate '", $plate,
                           "', well '", $well, "', sample '", $sample,
                           "', beadchip '", $beadchip, "', section '",
                           $beadchip_section, "'");
            push @metadata, ['', '', '', '', '', '' ];
        } else {
            push @metadata, [$plate, $well, $sample,
                             $beadchip, $beadchip_section,
                             '']; # placeholder for WH barcode
        }
    }
    return @metadata;
}

sub _find_warehouse_data {
    my ($self, $inputs_ref) = @_;
    my @data;
    foreach my $input (@{$inputs_ref}) {
        my ($plate, $well, $sample) = @{$input};
        my $wh_result =
            $self->sequencescape_db->find_infinium_sample_by_plate
                ($plate, $well);
        my @result;
        if ($wh_result) {
            @result = ($plate, $well, $wh_result->{'name'},
                       '', '', # placeholders for Infinium beadchip & section
                       $wh_result->{'barcode'});
        } else {
            $self->logwarn("No SequenceScape Warehouse result for plate '",
                           $plate, "', well '", $well,
                           "', Infinium sample name '", $sample, "'");
            @result = ($plate, $well, '', '', '', '');
        }
        push (@data, [@result]);
    }
    return @data;
}

sub _write_output {
    my ($self, $results_ref, $outpath, $header) = @_;
    # check the number of samples and data sources
    my @all_results = @{$results_ref};
    if (scalar(@all_results) != scalar(@DATA_SOURCE_NAMES)) {
        $self->logcroak("Incorrect number of result arrays input: ",
                        "Expected ", scalar(@DATA_SOURCE_NAMES), " got ",
                        scalar(@all_results));
    }
    my $total_samples = -1;
    foreach my $data (@all_results) {
        if ($total_samples == -1) {
            $total_samples = scalar(@{$data})
        } elsif (scalar(@{$data}) != $total_samples) {
            $self->logcroak("Inconsistent numbers of samples ",
                            "between data sources");
        }
    }
    # now write the output
    my $out;
    if ($outpath eq '-') {
        $out = *STDOUT;
    } else {
        open $out, ">", $outpath ||
            $self->logcroak("Cannot open output path '$outpath': $!");
    }
    if ($header) {
        print $out join(',', @HEADER_FIELDS)."\n";
    }
    for (my $i=0;$i<$total_samples;$i++) {
        for (my $j=0;$j<@all_results;$j++) {
            my @result = @{$all_results[$j][$i]};
            unshift(@result, $DATA_SOURCE_NAMES[$j]);
            if (scalar(@result) != scalar(@HEADER_FIELDS)) {
                $self->logcroak("Incorrect number of fields in result: ",
                                "Expected ", scalar(@HEADER_FIELDS),
                                " got ", scalar(@result));
            }
            print $out join(',', @result)."\n";
        }
    }
    if ($outpath ne '-') {
        close $out ||
            $self->logcroak("Cannot close output path '$outpath': $!");
    }
    return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
