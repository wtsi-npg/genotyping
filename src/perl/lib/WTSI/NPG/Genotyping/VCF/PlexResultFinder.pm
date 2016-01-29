use utf8;

package WTSI::NPG::Genotyping::VCF::PlexResultFinder;

use Moose;

use WTSI::NPG::Genotyping::Fluidigm::Subscriber;
use WTSI::NPG::Genotyping::Sequenom::Subscriber;
use WTSI::NPG::Genotyping::VCF::AssayResultParser;

with 'WTSI::DNAP::Utilities::Loggable';

has 'irods'      =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   default       => sub { return WTSI::NPG::iRODS->new },
   documentation => 'An iRODS handle');

has 'sample_ids' =>
  (is            => 'ro',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   documentation => 'Sample identifiers for query');

our $VERSION = '';

our $SEQUENOM = 'sequenom';
our $FLUIDIGM = 'fluidigm';
our $DEFAULT_DATA_PATH = '/seq/fluidigm';
our $CALLSET_NAME_KEY = 'callset_name';

# keys for config hash
our $IRODS_DATA_PATH_KEY      = 'irods_data_path';
our $PLATFORM_KEY             = 'platform';
our $REFERENCE_NAME_KEY       = 'reference_name';
our $REFERENCE_PATH_KEY       = 'reference_path';
our $SNPSET_NAME_KEY          = 'snpset_name';
our $READ_VERSION_KEY         = 'read_snpset_version';
our $VCF_NAME_KEY             = 'vcf_file_name';
our $WRITE_VERSION_KEY        = 'write_snpset_version';
our @REQUIRED_CONFIG_KEYS = ($IRODS_DATA_PATH_KEY,
                             $PLATFORM_KEY,
                             $REFERENCE_NAME_KEY,
                             $REFERENCE_PATH_KEY,
                             $SNPSET_NAME_KEY);

=head2 read_write_single

  Arg [1]    : HashRef of query params
  Arg [2]    : Path for VCF output
  Arg [3]    : Callset name for VCF metadata

  Example    : my $total = read_write_single($params, "foo.vcf", "data_foo");
  Description: Run a single query on iRODS and write the results (if any)
               as VCF. Returns the number of AssayResultSet objects found.

  Returntype : Int

=cut

sub read_write_single {
    my ($self, $params, $output_path, $callset_name) = @_;
    # query iRODS and write results as VCF
    my @irods_data = $self->_query_irods($params);
    my ($resultsets, $chromosome_lengths, $vcf_meta, $assay_snpset,
        $vcf_snpset) = @irods_data;
    if (scalar @{$resultsets} == 0) {
        $self->info("No assay result sets found for QC plex '",
                    $params->{$SNPSET_NAME_KEY}, "', platform '",
                    $params->{$PLATFORM_KEY}, "'");
    } else {
        $vcf_meta->{$CALLSET_NAME_KEY} = [$callset_name, ];
        my $vcfData = WTSI::NPG::Genotyping::VCF::AssayResultParser->new(
            resultsets     => $resultsets,
            contig_lengths => $chromosome_lengths,
            assay_snpset   => $assay_snpset,
            vcf_snpset     => $vcf_snpset,
            metadata       => $vcf_meta,
            )->get_vcf_dataset();
        open my $out, ">", $output_path ||
            $self->logcroak("Cannot open VCF output: '",
                            $output_path, "'");
        print $out $vcfData->str()."\n";
        close $out ||
            $self->logcroak("Cannot close VCF output: '",
                            $output_path, "'");
    }
    return scalar @{$resultsets};
}


=head2 read_write_all

  Arg [1]    : ArrayRef[HashRef] of query params
  Arg [2]    : Directory for VCF output

  Example    : my $paths = read_write_all($params_array, $out_dir);
  Description: Run the read_write_single method to write separate VCF files
               for each set of query parameters in the input. Callset name
               may be specified in the config hashref; otherwise it defaults
               to the name of the genotyping platform. Output filename
               similarly may be specified in config; default is
               $PLATFORM_$SNPSET.vcf. Returns the VCF paths written.
               Output will only be written for non-empty query results.
  Returntype : ArrayRef[Str]

=cut

sub read_write_all {
    # loop over input arguments (config hashes) and write results
    my ($self, $params_array, $output_dir) = @_;
    my %outputs;
    my %callsets;
    my @vcf;
    foreach my $params (@{$params_array}) {
        # check uniqueness of callset and output file names
        $self->_validate_query_params($params);
        my $callset = $params->{$CALLSET_NAME_KEY} ||
            $params->{$PLATFORM_KEY};
        if ($callsets{$callset}) {
            $self->logcroak("Non-unique callset name '", $callset, "'");
        } else {
            $callsets{$callset} = 1;
        }
        my $platform = $params->{$PLATFORM_KEY};
        my $snpset_name = $params->{$SNPSET_NAME_KEY};
        my $vcf_name_default = $platform."_".$snpset_name.".vcf";
        my $output = $params->{$VCF_NAME_KEY} || $vcf_name_default;
        if ($outputs{$output}) {
            $self->logcroak("Non-unique output file name '", $output, "'");
        } else {
            $outputs{$output} = 1;
        }
        my $out_path = $output_dir."/".$output;
        my $written = $self->read_write_single($params, $out_path, $callset);
        if ($written > 0) { push(@vcf, $out_path); }
    }
    my $vcf_total = scalar @vcf;
    $self->info($vcf_total, " VCF files written.");
    if ($vcf_total==0) {
        $self->logwarn("No plex results found for given inputs");
    }
    return \@vcf;
}

sub _query_irods {
    # get AssayResultSets, SNPSets, and contig lengths from iRODS
    # works for Fluidigm or Sequenom
    my ($self, $params) = @_;
    my $subscriber;
    my %query_params = (irods          => $self->irods,
                        data_path      => $params->{$IRODS_DATA_PATH_KEY},
                        reference_path => $params->{$REFERENCE_PATH_KEY},
                        reference_name => $params->{$REFERENCE_NAME_KEY},
                        snpset_name    => $params->{$SNPSET_NAME_KEY});
    if ($params->{$PLATFORM_KEY} eq $FLUIDIGM) {
        $subscriber = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
            (%query_params);
    } elsif ($params->{$PLATFORM_KEY} eq $SEQUENOM) {
        if ($params->{$READ_VERSION_KEY}) {
            $query_params{'snpset_version'} = $params->{$READ_VERSION_KEY};
        }
        $subscriber = WTSI::NPG::Genotyping::Sequenom::Subscriber->new
            (%query_params);
    } else {
        $self->logcroak("Unknown plex type: '", $params->{$PLATFORM_KEY}, "'");
    }
    my ($resultset_hashref, $vcf_metadata) =
        $subscriber->get_assay_resultsets_and_vcf_metadata($self->sample_ids);
    # unpack hashref from Subscriber.pm into an array of resultsets
    my @resultsets;
    foreach my $sample (keys %{$resultset_hashref}) {
        my @sample_resultsets = @{$resultset_hashref->{$sample}};
        push @resultsets, @sample_resultsets;
    }
    my $total = scalar @resultsets;
    $self->info("Found $total assay resultsets.");
    my $assay_snpset = $subscriber->snpset;
    my $vcf_snpset;
    if ($params->{$PLATFORM_KEY} eq $SEQUENOM) {
        my @args = (
            $params->{$REFERENCE_PATH_KEY},
            $params->{$REFERENCE_NAME_KEY},
            $params->{$SNPSET_NAME_KEY},
        );
        if ($params->{$WRITE_VERSION_KEY}) {
            push @args, $params->{$WRITE_VERSION_KEY};
        }
        $vcf_snpset = $subscriber->find_irods_snpset(@args);
    } else {
        $vcf_snpset = $assay_snpset;
    }
    return (\@resultsets,
            $subscriber->get_chromosome_lengths(),
            $vcf_metadata,
            $assay_snpset,
            $vcf_snpset,
        );
}

sub _validate_query_params {
    # raise an error if plex query params are not valid
    my ($self, $params) = @_;
    foreach my $key (@REQUIRED_CONFIG_KEYS) {
        unless ($params->{$key}) {
            $self->logcroak("Required parameter '", $key,
                            "' missing from query configuration");
        }
    }
    return 1;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::PlexResultFinder

=head1 DESCRIPTION

Find QC plex results (eg. Sequenom, Fluidigm) in iRODS and write as VCF.

=head2 Method

=over 1

=item * Input parameters for one or more iRODS queries

=item * Query iRODS with appropriate Subscriber object

=item * Call AssayResultParser on data returned by query

=item * Write as one or more VCF files

=back

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
