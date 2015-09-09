use utf8;

package WTSI::NPG::Genotyping::VCF::ReferenceFinder;

use Moose;

use File::Spec::Functions qw(catfile);
use URI::file;

with qw/WTSI::DNAP::Utilities::Loggable
        npg_tracking::data::reference::find/;

our $VERSION = '';

## attributes inherited from 'find' role

has '+aligner'  => # overrides attribute in 'find' role
    (isa             => 'Str',
     is              => 'ro',
     required        => 0,
     default         => 'fasta',
     documentation   => 'Set the default "aligner" to find the fasta '.
         'reference, instead of BWA',
 );

has '+reference_genome' => # overrides attribute in 'find' role
    (isa             => 'Str',
     is              => 'ro',
     required        => 1,
     documentation   => "Reference genome string in the format ".
         "'organism (strain)' as set in LIMS and iRODS metadata.",
 );

has '+repository' =>  # overrides attribute in 'find' role
    (isa             => 'NPG_TRACKING_REFERENCE_REPOSITORY',
     is              => 'ro',
     required        => 1,
     documentation   => "Make repository a required argument. This is the ".
                        "root directory for reference files.",
);

has 'id_run' =>
    (isa             => 'Maybe[Str]',
     is              => 'ro',
     required        => 0,
     documentation   => "Placeholder for the 'id_run' attribute, referenced in the 'find' role but not required in this class.",
 );

has 'position' =>
    (isa             => 'Maybe[Int]',
     is              => 'ro',
     required        => 0,
     documentation   => "Placeholder for the 'position' attribute, referenced in the 'find' role but not required in this class.",
 );

## new attributes

has 'reference_path' =>
    (isa             => 'Str',
     is              => 'ro',
     builder         => '_build_reference_path',
     lazy            => 1,
     documentation   => 'Path found for the given genome reference',
);


# call the 'refs' method of the 'find' role to get reference path
# returns an array of reference paths
# if exactly one reference is found, return it; otherwise throw an error
#
# npg_tracking::data::reference::list->repository appears to default to
# $ENV{'NPG_REPOSITORY_ROOT'}, but the default doesn't work in tests,
# so make 'repository' a required attribute in this class
#
sub _build_reference_path {
    my ($self,) = @_;
    my @refs = @{$self->refs()};
    my $total_refs = scalar @refs;
    if ($total_refs == 0) {
	    $self->logcroak("No path found for reference genome '",
			    $self->reference_genome, "'");
    } elsif ($total_refs > 1) {
	    $self->logcroak("More than one path found for reference ",
			    "genome '", $self->reference_genome, "'");
    }
    return $refs[0];
}


=head2 get_reference_uri

  Arg [1]    : Bool
  Example    : my $uri = $rf->get_reference_uri($absolute);
  Description: Convert the reference_path attribute to a file uri. If true,
               the argument forces an absolute path to be used.
  Returntype : Str

=cut

# making a 'uri' attribute dependent on reference_path causes a crash with
# "Can't locate object method". Possible bug in Moose, related to dependency
# on lazy attribute?

sub get_reference_uri {
    my ($self, $absolute) = @_;
    my $uri;
    $absolute ||= 1;
    if ($absolute) {
        $uri = URI::file->new_abs($self->reference_path);
    } else {
        $uri = URI::file->new($self->reference_path);
    }
    return $uri->as_string();
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping::VCF::ReferenceFinder

=head1 DESCRIPTION

Class using npg_tracking to find the local human genome reference path from
iRODS metadata, using the LIMS database. The reference path is used to
populate the ##reference field in the VCF header.

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
