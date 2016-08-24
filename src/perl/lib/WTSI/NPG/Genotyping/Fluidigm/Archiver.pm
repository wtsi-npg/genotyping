use utf8;

package WTSI::NPG::Genotyping::Fluidigm::Archiver;

use Cwd qw/abs_path/;
use File::Spec;
use WTSI::NPG::Genotyping::Fluidigm::Collector;
use WTSI::NPG::Genotyping::Fluidigm::ExportFile;
use WTSI::NPG::Genotyping::Fluidigm::ResultSet;

use Moose;

with qw/WTSI::NPG::Utilities::Archivable/;

has 'dir_regex' =>
    (is       => 'ro',
     isa      => 'RegexpRef',
     default  => sub { return qr/^\d{10}$/msx; }
 );

has 'irods_root' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   documentation => 'Root collection path in iRODS, eg. /seq'
  );

has 'output_prefix' => (
    is  => 'ro',
    isa => 'Str',
    documentation => 'Prefix for .tgz archive filenames. Overrides '.
        'attribute in Archivable Role.',
    default => 'fluidigm',
    );

has 'collector' => (
    is       => 'ro',
    isa      => 'WTSI::NPG::Genotyping::Fluidigm::Collector',
    documentation => 'Utility object to collect target files. Overrides '.
        'attribute in Archivable Role.',
    lazy     => 1,
    builder  => '_build_collector',
    init_arg => undef,
);

our $VERSION = '';


sub _build_collector {
    # overrides build method in Archivable Role
    my ($self) = @_;
    my $collector = WTSI::NPG::Genotyping::Fluidigm::Collector->new(
        root       => abs_path($self->target_dir),
        depth      => 2,
        regex      => $self->dir_regex,
        irods_root => $self->irods_root,
    );
    return $collector;
}


=head2 find_directories_to_archive

  Args       : None

  Example    : $dirs = $archiver->find_directories_to_archive()
  Description: Return an Array containing directories which are candidates
               for archiving. Overrides method in the Archivable Role.
  Returntype : Array[Str]

=cut

sub find_directories_to_archive {
    my ($self) = @_;
    my $now = DateTime->now;
    my $threshold = DateTime->from_epoch
        (epoch => $now->epoch)->subtract(days => $self->days_ago);
    return $self->collector->collect_archivable_dirs($threshold->epoch);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG:::Genotyping::Fluidigm::Archiver

=head1 DESCRIPTION

Class to archive Fluidigm data in gzipped tar files.

Find candidate files/directories for archiving; check their last
modification time and iRODS publication status; and, if they meet given
criteria, store them in gzipped tar files.

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
