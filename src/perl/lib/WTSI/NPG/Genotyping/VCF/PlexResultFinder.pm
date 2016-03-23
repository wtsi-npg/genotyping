use utf8;

package WTSI::NPG::Genotyping::VCF::PlexResultFinder;

use Moose;

use File::Slurp qw(read_file);
use File::Spec::Functions qw/catfile/;
use JSON;
use Try::Tiny;

use WTSI::NPG::Genotyping::Fluidigm::Subscriber;
use WTSI::NPG::Genotyping::Sequenom::Subscriber;
use WTSI::NPG::Genotyping::VCF::AssayResultParser;
use WTSI::NPG::iRODS::DataObject;

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

has 'subscriber_config' =>
  (is            => 'ro',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   documentation => 'Paths to JSON files of parameters for Subscribers');

# non-input parameters

has 'subscribers' =>
  (is             => 'ro',
   isa            => 'ArrayRef',
   lazy           => 1,
   init_arg       => undef,
   builder        => '_build_subscribers',
   documentation  => 'ArrayRef of Sequenom::Subscriber and/or '.
                     'Fluidigm::Subscriber objects to query iRODS'
                 );

our $VERSION = '';

our $SEQUENOM = 'sequenom';
our $FLUIDIGM = 'fluidigm';
our $PLATFORM_KEY = 'platform';


=head2 write_manifests

  Arg [1]    : [Str] Directory for TSV output

  Example    : write_manifests($out_dir);
  Description: Write the TSV plex manifest from each Subscriber object to
               the given directory, for use in later pipeline workflows.
               Filename is created from the callset name with the .tsv suffix.
               Manifest written is the same one used for VCF output (which
               may or may not be the same as for the original assay).
  Returntype : ArrayRef[Str] Paths for TSV output

=cut


sub write_manifests {
    # get the VCF SNPSet DataObject from each Subscriber
    # Slurp into a string and write to given directory
    # construct filename from the callset name (will be unique)
    # return number of manifests written
    # uses the VCF (output) snpset, if it differs from the input snpset
    my ($self, $outdir) = @_;
    if (! -e $outdir) {
        $self->logcroak("Output directory '", $outdir, "' does not exist");
    } elsif (! -d $outdir) {
        $self->logcroak("Output argument '", $outdir, "' is not a directory");
    }
    my @output_paths;
    if (scalar @{$self->subscribers} == 0) {
        $self->logwarn("No valid Subscriber objects available; QC plex ",
                       "manifests cannot be found");
    }
    foreach my $subscriber (@{$self->subscribers}) {
        my $filename = $subscriber->callset.".tsv";
        my $output_path = catfile($outdir, $filename);
        open my $out, ">", $output_path || $self->logcroak("Cannot open '",
                                                       $output_path, "'");
        print $out $subscriber->write_snpset_data_object->slurp();
        close $out || $self->logcroak("Cannot close '", $output_path, "'");
        push @output_paths, $output_path;
    }
    return \@output_paths;
}

=head2 write_vcf

  Arg [1]    : [Str] Directory for VCF output

  Example    : write_vcf($out_dir);
  Description: Write VCF with QC plex results from each Subscriber object to
               the given directory. Filename is created from the
               callset name with the .vcf suffix.
  Returntype : ArrayRef[Str] Paths for VCF output

=cut

sub write_vcf {
    # query each Subscriber object and write QC plex results as VCF
    my ($self, $outdir) = @_;
    if (! -e $outdir) {
        $self->logcroak("Output directory '", $outdir, "' does not exist");
    } elsif (! -d $outdir) {
        $self->logcroak("Output argument '", $outdir, "' is not a directory");
    }
    my @vcf_paths;
    foreach my $subscriber (@{$self->subscribers}) {
        my $filename = $subscriber->callset.".vcf";
        my $output_path = catfile($outdir, $filename);
        my $total = $self->_write_vcf_single($subscriber, $output_path);
        if ($total > 0) {
            push @vcf_paths, $output_path;
            $self->info("Wrote $total resultsets to VCF ", $output_path);
        } else {
            $self->info("No resultsets found, omitting VCF output ",
                        "for callset '", $subscriber->callset, "'");
        }
    }
    if (scalar @vcf_paths == 0) {
        $self->logwarn("No QC plex data found for VCF output");
    }
    return \@vcf_paths;
}

sub _build_subscribers {
    my ($self) = @_;
    my @subscribers;
    my %callsets;
    foreach my $config (@{$self->_read_subscriber_config()}) {
        my %args = %{$config};
        my $platform = delete $args{$PLATFORM_KEY};
        my $subscriber;
        # Subscriber creation may fail, eg. if plex manifest cannot be located
        if ($platform eq $FLUIDIGM) {
            try {
                $subscriber = WTSI::NPG::Genotyping::Fluidigm::Subscriber->new
                    (%args);
            } catch {
                $self->logwarn("Unable to create Fluidigm subscriber: ", $_);
            }
        } elsif ($platform eq $SEQUENOM) {
            try {
                $subscriber = WTSI::NPG::Genotyping::Sequenom::Subscriber->new
                    (%args);
            } catch {
                $self->logwarn("Unable to create Sequenom subscriber: ", $_);
            }
        } else {
            $self->logcroak("Unknown plex type: '", $platform, "'");
        }
        my $callset = $subscriber->callset();
        if ($callsets{$callset}) {
            $self->logcroak("Non-unique callset name '", $callset, "'");
        } else {
            $callsets{$callset} = 1;
        }
        push @subscribers, $subscriber;
    }
    my $total = scalar @subscribers;
    if ($total == 0) {
        $self->logwarn("No valid iRODS subscribers could be created ",
                       "from given config files; no QC plex data will ",
                       "be retrieved");
    } else {
        $self->info("Successfully created ", $total, " iRODS subscribers to ",
                    "query for QC plex data");
    }
    return \@subscribers;
}


sub _read_subscriber_config {
    # read query params from JSON
    my ($self) = @_;
    my @config;
    foreach my $config_path (@{$self->subscriber_config}) {
        if (-e $config_path) {
            push @config, decode_json(read_file($config_path));
        } else {
            $self->logcroak("Subscriber configuration path '", $config_path,
                            "' does not exist");
        }
    }
    return \@config;
}

sub _write_vcf_single {
    # write a single VCF file, from a single Subscriber
    my ($self, $subscriber, $output_path) = @_;
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
    if (scalar @resultsets == 0) {
        $self->info("No assay result sets found for QC callset '",
                    $subscriber->callset, "'");
    } else {
        my $vcfData = WTSI::NPG::Genotyping::VCF::AssayResultParser->new(
            resultsets     => \@resultsets,
            contig_lengths => $subscriber->get_chromosome_lengths(),
            assay_snpset   => $subscriber->read_snpset,
            vcf_snpset     => $subscriber->write_snpset,
            metadata       => $vcf_metadata,
            )->get_vcf_dataset();
        open my $out, ">", $output_path ||
            $self->logcroak("Cannot open VCF output: '",
                            $output_path, "'");
        print $out $vcfData->str()."\n";
        close $out ||
            $self->logcroak("Cannot close VCF output: '",
                            $output_path, "'");
    }
    return $total;
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

=item *

Input configuration for one or more iRODS queries

=item *

Query iRODS with appropriate Subscriber object

=item *

Call AssayResultParser on data returned by query

=item *

Write as one or more VCF files

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
