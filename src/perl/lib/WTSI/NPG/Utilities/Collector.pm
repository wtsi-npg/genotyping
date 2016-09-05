use utf8;

package WTSI::NPG::Utilities::Collector;

use DateTime;
use File::Find;
use File::stat;

use Moose;

our $VERSION = '';

with qw/WTSI::DNAP::Utilities::Loggable/;

has 'root' =>
  (is       => 'ro',
   isa      => 'Str',
   documentation => "Root path in which to begin file/directory search",
   required => 1);

has 'depth' =>
  (is       => 'ro',
   isa      => 'Maybe[Int]',
   documentation => "Maximum depth of search",
   default  => sub { undef; } );

has 'regex' =>
  (is       => 'ro',
   isa      => 'Maybe[RegexpRef]',
   lazy     => 1,
   documentation => "Regular expression to filter files/directories",
   default  => sub { undef; } );

has 'start_depth' =>
  (is       => 'ro',
   isa      => 'Int',
   documentation => "Starting depth of search",
   lazy     => 1,
   builder  => '_build_start_depth',
   init_arg => undef);

has 'stop_depth' =>
  (is       => 'ro',
   isa      => 'Maybe[Int]',
   lazy     => 1,
   documentation => "Starting depth of search",
   builder  => '_build_stop_depth',
   init_arg => undef);

sub BUILD {

  my ($self) = @_;
  if ($self->root eq '') {
      $self->logcroak('A non-empty root argument is required');
  } elsif (! -d $self->root) {
      $self->logcroak("Root argument '", $self->root, "' is not a directory");
  } else {
      $self->debug("Root for filesystem search: '", $self->root, "'");
  }
}

sub _build_start_depth {
  my ($self) = @_;
  my $start_depth = $self->root =~ tr[/][];
  $self->debug("Start depth for filesystem search is ", $start_depth);
  return $start_depth;
}

sub _build_stop_depth {
  my ($self) = @_;
  my $stop_depth;
  if (defined $self->depth) {
    $stop_depth = $self->start_depth + $self->depth;
    $self->debug("Stop depth for filesystem search is ", $stop_depth);
  } else {
    $self->debug("Stop depth not defined, searching entire directory tree.");
  }
  return $stop_depth;
}

=head2 collect_dirs

  Arg [1]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.

  Example    : @dirs = $collector->collect_dirs($modified)
  Description: Return an array of directory names present for which the test
               predicate returns true.
  Returntype : array of strings (dir names)

=cut

sub collect_dirs {
  my ($self, $test) = @_;
  my @dirs;
  my $collector = $self->make_collector_function($test, \@dirs);

  find({preprocess => sub {
          my $current_depth = $File::Find::name =~ tr[/][];

          my @dirs;
          if (!defined $self->stop_depth ||
                  $current_depth < $self->stop_depth) {
            @dirs = grep { -d && ! /^[.]+$/msx } @_;
          }

          return @dirs;
        },
        wanted => sub {
          my $current_depth = $File::Find::name =~ tr[/][];

          if (!defined $self->stop_depth ||
                  $current_depth < $self->stop_depth) {
            if ($self->regex) {
              $collector->($File::Find::name) if $_ =~ $self->regex;
            }
            else {
              $collector->($File::Find::name);
            }
          }
        }
       }, $self->root);

  return @dirs;
}


=head2 collect_dirs_last_modified_before

  Arg [1]    : Finish time, in seconds since the epoch

  Example    : @dirs = $c->collect_dirs_last_modified_before($finish)
  Description: Return an array of directory names present under the specified
               root, for which the test predicate returns true, up to the
               specified depth.
  Returntype : array of strings (dir names)

=cut

sub collect_dirs_last_modified_before {
    my ($self, $finish) = @_;
    return $self->collect_dirs($self->last_modified_before($finish));
}


=head2 collect_dirs_modified_between

  Arg [1]    : Start time, in seconds since the epoch
  Arg [2]    : Finish time, in seconds since the epoch

  Example    : @dirs = $c->collect_dirs_modified_between($start, $finish)
  Description: Return an array of directory names present under the specified
               root, for which the test predicate returns true, up to the
               specified depth.
  Returntype : array of strings (dir names)

=cut

sub collect_dirs_modified_between {
    my ($self, $start, $finish) = @_;
    my $test = $self->modified_between($start, $finish);
    return $self->collect_dirs($test);
}

=head2 collect_files

  Arg [1]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.

  Example    : @files = $collector->collect_files($modified)
  Description: Returns an array of file names present, for which the test
               predicate returns true.
  Returntype : array of strings (file names)

=cut

sub collect_files {
  my ($self, $test) = @_;
  my @files;
  my $collector = $self->make_collector_function($test, \@files);

  find({preprocess => sub {
          my $current_depth = $File::Find::dir =~ tr[/][];

          my @elts;
          if (!defined $self->stop_depth ||
                  $current_depth < $self->stop_depth) {
            # Remove any dirs except . and ..
            @elts = grep { ! /^[.]+$/msx } @_;
          }

          return @elts;
        },
        wanted => sub {
          my $current_depth = $File::Find::dir =~ tr[/][];

          if (!defined $self->stop_depth ||
                  $current_depth < $self->stop_depth) {
            if (-f) {
              if ($self->regex) {
                $collector->($File::Find::name) if $_ =~ $self->regex;
              }
              else {
                $collector->($File::Find::name)
              }
            }
          }
        }
       }, $self->root);

  return @files;
}

=head2 collect_files_simple

  Args       : None
  Example    : @files = $collector->collect_files_simple()
  Description: Collect files, restricted only by the 'regex' and 'depth'
               attributes (if any). No other tests are applied.
  Returntype : array of strings (dir names)

=cut

sub collect_files_simple {
    my ($self) = @_;
    my $test = sub { return 1 };

    return $self->collect_files(sub {return 1;});
}

=head2 make_collector_function

  Arg [1]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.
  Arg [2]    : arrayref of an array into which matched object will be pushed
               if the test returns true.

  Example    : $coll_fun = make_collector_function(sub { ... }, \@found);
  Description: Returns a function that will push matched objects onto a
               specified array.
  Returntype : coderef

=cut

sub make_collector_function {
  my ($self, $test, $listref) = @_;

  return sub {
    my ($arg) = @_;

    my $collect = $test->($arg);
    if ($collect) {
        push(@{$listref}, $arg);
        $self->debug("Added '$arg' to array of filesystem objects");
    }
    return $collect;
  }
}

=head2 modified_between

  Arg [1]    : time in seconds since the epoch
  Arg [2]    : time in seconds since the epoch

  Example    : $test = $collector->modified_between($start_time, $end_time)
  Description: Return a function that accepts a single argument (a
               file name string) and returns true if that file has
               last been modified between the two specified times in
               seconds (inclusive).

               Return value can be used as the $test argument to
               make_collector_function().
  Returntype : coderef

=cut

sub modified_between {
  my ($self, $start, $finish) = @_;

  return sub {
    my ($file) = @_;
    my $mtime = $self->stat_mtime($file);
    return ($start <= $mtime) && ($mtime <= $finish);
  }
}


=head2 last_modified_before

  Arg [1]    : time in seconds since the epoch

  Example    : $test = $collector->last_modified_before($time)
  Description: Return a function that accepts a single argument (a
               file name string) and returns true if that file was
               last modified before (ie. is older than) the specified
               time in seconds.

               Return value can be used as the $test argument to
               make_collector_function().
  Returntype : coderef

=cut

sub last_modified_before {
  my ($self, $threshold) = @_;

  return sub {
    my ($file) = @_;
    my $mtime = $self->stat_mtime($file);
    return $mtime <= $threshold;
  }
}

=head2 stat_mtime

  Arg [1]    : file path

  Example    : $time = $collector->stat_mtime($file);
  Description: Safely stat the given file and return its mtime.
  Returntype : Int

=cut

sub stat_mtime {
    my ($self, $file) = @_;
    my $stat = stat($file);
    unless (defined $stat) {
      my $wd = `pwd`;
      $self->logcroak("Failed to stat file '$file' in $wd: $!");
    }
    my $mtime = $stat->mtime;
    return $mtime;
}


no Moose;

1;



__END__

=head1 NAME

WTSI::NPG::Utilities::Collector

=head1 DESCRIPTION

Utility class to evaluate whether files and directories meet given criteria,
and search a filesystem tree for matching entries. Acts as a wrapper for
the Perl File::Find module.

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
