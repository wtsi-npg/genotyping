use utf8;

package WTSI::NPG::Utilities::Archivable;

use Cwd qw/abs_path cwd/;
use DateTime;
use File::Basename qw/fileparse/;
use File::Find;
use File::Spec qw/catfile/;
use WTSI::DNAP::Utilities::Runnable;
use WTSI::NPG::Utilities::Collector;

use Moose::Role;

with qw/WTSI::DNAP::Utilities::Loggable/;

has 'days_ago' => (
    is  => 'ro',
    isa => 'Int',
    documentation => 'Minimum time, in days, since last modification '.
        'for directory to be archived',
    default => 90,
    );

has 'dir_regex' =>
    (is       => 'ro',
     isa      => 'RegexpRef',
     required => 1,
     documentation => 'Regexp to identify directory names (not paths) for '.
         'possible archiving.',
 );

has 'output_dir' => (
    is  => 'ro',
    isa => 'Str',
    documentation => 'Directory for .tar.gz archive files',
    default => sub { cwd() },
    );

has 'output_prefix' => (
    is  => 'ro',
    isa => 'Str',
    documentation => 'Prefix for .tar.gz archive filenames, eg. "fluidigm_"',
    required => 1,
    );

has 'pigz_processes' => (
    is  => 'ro',
    isa => 'Int',
    documentation => 'Number of processes to use for pigz compression',
    default => 4,
    );

has 'target_dir' => (
    is  => 'ro',
    isa => 'Str',
    documentation => 'Root directory to search for input files',
    default => sub { cwd() },
    );

has 'remove' => (
    is  => 'ro',
    isa => 'Bool',
    documentation => 'If true, remove input files after adding to an archive',
    default => 1,
    );

has 'collector' => (
    is       => 'ro',
    isa      => 'WTSI::NPG::Utilities::Collector',
    documentation => 'Utility object to collect target files',
    lazy     => 1,
    builder  => '_build_collector',
    init_arg => undef,
);

our $VERSION = '';

sub BUILD {
    my ($self) = @_;
    if (! -e $self->output_dir) {
        $self->logcroak("Output directory path '", $self->output_dir,
                        "' does not exist");
    } elsif (! -d $self->output_dir) {
        $self->logcroak("Output directory path '", $self->output_dir,
                        "' is not a directory");
    }

    if (! -e $self->target_dir) {
        $self->logcroak("Target directory path '", $self->target_dir,
                        "' does not exist");
    } elsif (! -d $self->target_dir) {
        $self->logcroak("Target directory path '", $self->target_dir,
                        "' is not a directory");
    }
}

sub _build_collector {
    my ($self) = @_;
    my $collector = WTSI::NPG::Utilities::Collector->new(
        root  => abs_path($self->target_dir),
        depth => 2,
        regex => $self->dir_regex,
    );
    return $collector;
}


=head2 add_to_archives

  Arg [1]    : [ArrayRef] One or more file or directory paths to be archived
  Arg [2]    : [Bool] Dry-run status. If true, log archive files which would
               be used, but do not actually archive any files. Optional,
               defaults to False.

  Example    : add_to_archives($files, $dry_run);
  Description: Add the given list of files to monthly .tar.gz archives
               with names of the form [prefix]_yyy-mm, creating the
               archives if necessary.

  Returntype : [Array] Paths to archive files

=cut

sub add_to_archives {
    my ($self, $inputs, $dry_run) = @_;
    my %inputs_by_archive; # hash of arrays of input paths
    foreach my $input (@{$inputs}) {
        my $mtime = $self->collector->stat_mtime($input);
        my $archive_name = $self->monthly_archive_filename($mtime);
        my $archive_path = File::Spec->catfile
            ($self->output_dir, $archive_name);
        push @{$inputs_by_archive{$archive_path}}, $input;
    }
    my @archives = sort(keys(%inputs_by_archive));
    $self->info("Archive files to be written: ", join(", ", @archives));
    foreach my $archive (@archives) {
        my $total = scalar(@{$inputs_by_archive{$archive}});
        if ($dry_run) {
            $self->info("Dry-run mode: ", $total,
                        " inputs for archive path ", $archive);
        } else {
            $self->info("Adding ", $total, " input(s) to archive path ",
                        $archive);
            $self->_add_to_archive($inputs_by_archive{$archive}, $archive);
        }
    }
    return @archives;
}


=head2 find_directories_to_archive

  Args       : None

  Example    : $dirs = $archiver->find_directories_to_archive()
  Description: Return an Array containing directories which are candidates
               for archiving.
  Returntype : Array[Str]

=cut

sub find_directories_to_archive {
    my ($self) = @_;
    my $now = DateTime->now;
    my $threshold = DateTime->from_epoch
        (epoch => $now->epoch)->subtract(days => $self->days_ago);
    my $t = $threshold->epoch;
    return $self->collector->collect_dirs_last_modified_before($t);
}


=head2 monthly_archive_filename

     Arg [1]    : last modification time of a file, in seconds since the epoch

     Example    : $name = $archiver->month_archive_filename($time)
     Description: Get the name of a .tar.gz file, of the form
                  [prefix]_[date].tar.gz, where [date] is of the form yyyy-mm.
                  Example: fluidigm_2015-08.tar.gz
     Returntype : Str

=cut

sub monthly_archive_filename {

  my ($self, $epoch_time) = @_;
  my $mod_time = DateTime->from_epoch(epoch => $epoch_time);
  my $filename = sprintf("%s_%d-%02d.tar.gz",
			 $self->output_prefix,
			 $mod_time->year,
			 $mod_time->month
			);
  return $filename;
}


sub _add_to_archive {
    # This method does not use Perl's Archive::Tar package. This is because
    # Archive::Tar reads the entire tarfile into memory, so is not
    # appropriate for large archive files.
    my ($self, $target_files, $archive_path) = @_;
    if (-e $archive_path) {
        # limitation of tar: cannot append to compressed files
        # instead, need to uncompress, append, and recompress
        # use pigz instead of gzip for greater speed
        $self->debug("Archive file '", $archive_path, "' already exists");
        WTSI::DNAP::Utilities::Runnable->new(
            executable => 'pigz',
            arguments  => ['-p', $self->pigz_processes, '-d', $archive_path],
        )->run();
        my ($uncompressed_name, $dirs, $suffix) = fileparse
            ($archive_path, qr/[.][^.]*/msx);
        my $uncompressed_path = $dirs.$uncompressed_name;
        my @args = ('-uf', $uncompressed_path);
        if ($self->remove) {
            $self->debug(scalar @{$target_files},
                         " target files will be removed after archiving");
            unshift @args, '--remove-files';
        }
        push(@args, @{$target_files});
        WTSI::DNAP::Utilities::Runnable->new(
            executable => 'tar',
            arguments  => \@args,
        )->run();
        WTSI::DNAP::Utilities::Runnable->new(
            executable => 'pigz',
            arguments  => ['-p', $self->pigz_processes, $uncompressed_path],
        )->run();
        $self->debug("Appended to archive file, '", $archive_path, "'");
    } else {
        my @args = ('-czf', $archive_path);
        if ($self->remove) {
            $self->debug(scalar @{$target_files},
                         " target files will be removed after archiving");
            unshift @args, '--remove-files';
        }
        push(@args, @{$target_files});
        WTSI::DNAP::Utilities::Runnable->new(
            executable => 'tar',
            arguments  => \@args,
        )->run();
        $self->debug("Created new archive file '", $archive_path, "'");
    }
    return $archive_path;
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Utilities::Archiver

=head1 DESCRIPTION

Role to find candidate files/directories for archiving, and store them
in gzipped tar files.

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
