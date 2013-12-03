use utf8;

package WTSI::NPG::Utilities;

use strict;
use warnings;
use Carp;
use DateTime;
use File::Find;
use File::stat;
use Log::Log4perl;

use base 'Exporter';
our @EXPORT_OK = qw(collect_dirs
                    collect_files
                    common_stem
                    make_collector
                    md5sum
                    modified_between
                    trim
                    user_session_log);

our $MD5SUM = 'md5sum';
our $USER_SESSION_LOG_DIR = '/nfs/srpipe_data/logs/user_session_logs/';

=head2 common_stem

  Arg [1]    : string
  Arg [2]    : string

  Example    : $stem = common_stem("foo13240a", "foo199")
  Description: Return the common part of the two arguments, starting
               from the left (index 0). If one or more of the arguments
               are empty strings, or the arguments differ at the first
               character, an empty string is returned.
  Returntype : Str

=cut

sub common_stem {
  my ($str1, $str2) = @_;
  my $stem = '';

  my $len1 = length($str1);
  my $len2 = length($str2);
  my $end;
  if ($len1 < $len2) {
    $end = $len1;
  } else {
    $end = $len2;
  }

  for (my $i = 0; $i < $end; ++$i) {
    my $c1 = substr($str1, $i, 1);
    my $c2 = substr($str2, $i, 1);

    last if $c1 ne $c2;

    $stem .= $c1;
  }

  return $stem;
}

=head2 trim

  Arg [1]    : string

  Example    : $trimmed = trim("  foo ");
  Description: Trim leading and trailing whitespace, withina  line, from a copy
               of the argument. Return the trimmed string.
  Returntype : Str

=cut

sub trim {
  my ($str) = @_;

  my $copy = $str;
  $copy =~ s/^\s*//;
  $copy =~ s/\s*$//;

  return $copy;
}

=head2 user_session_log

  Arg [1]    : UID string
  Arg [2]    : Session name string

  Example    : $log = user_session_log($uid, 'my_session');
  Description: Return a log file path for a program user session.
  Returntype : Str

=cut

sub user_session_log {
  my ($uid, $session_name) = @_;

  unless (defined $uid) {
    croak "A defined uid argument is required\n";
  }
  unless (defined $session_name) {
    croak "A defined session_name argument is required\n";
  }
  unless ($uid =~ /[A-Za-z0-9]+/) {
    croak "The uid argument must match [A-Za-z0-9]+\n";
  }
  unless ($session_name =~ /[A-Za-z0-9]+/) {
    croak "The session_name argument must match [A-Za-z0-9]+\n";
  }

  my $now = DateTime->now;
  return sprintf("%s/%s.%s.%s.log", $USER_SESSION_LOG_DIR,
                 $session_name, $uid, $now->strftime("%F"));
}

=head2 collect_files

  Arg [1]    : Root directory
  Arg [2]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.
  Arg [3]    : Maximum depth to search below the starting directory.
               Optional (undef for unlimited depth).
  Arg [4]    : A file matching regex that is applied in addition to to
               the test. Optional.

  Example    : @files = collect_files('/home', $modified, 3, qr/.txt$/i)
  Description: Returns an array of file names present under the specified
               root, for which the test predicate returns true, up to the
               specified depth.
  Returntype : array of strings (file names)

=cut

sub collect_files {
  my ($root, $test, $depth, $regex) = @_;

  $root eq '' and croak 'A non-empty root argument is required';

  my @files;
  my $collector = make_collector($test, \@files);

  my $start_depth = $root =~ tr[/][];
  my $stop_depth;
  if (defined $depth) {
    $stop_depth = $start_depth + $depth;
  }

  find({preprocess => sub {
          my $current_depth = $File::Find::dir =~ tr[/][];

          my @elts;
          if (!defined $stop_depth || $current_depth < $stop_depth) {
            # Remove any dirs except . and ..
            @elts = grep { ! /^\.+$/ } @_;
          }

          return @elts;
        },
        wanted => sub {
          my $current_depth = $File::Find::dir =~ tr[/][];

          if (!defined $stop_depth || $current_depth < $stop_depth) {
            if (-f) {
              if ($regex) {
                $collector->($File::Find::name) if $_ =~ $regex;
              }
              else {
                $collector->($File::Find::name)
              }
            }
          }
        }
       }, $root);

  return @files;
}

=head2 collect_dirs

  Arg [1]    : Root directory
  Arg [2]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.
  Arg [3]    : Maximum depth to search below the starting directory.
  Arg [4]    : A file matching regex that is applied in addition to to
               the test. Optional.

  Example    : @dirs = collect_dirs('/home', $modified, 2)
  Description: Return an array of directory names present under the specified
               root, for which the test predicate returns true, up to the
               specified depth.
  Returntype : array of strings (dir names)

=cut

sub collect_dirs {
  my ($root, $test, $depth, $regex) = @_;

  $root eq '' and croak 'A non-empty root argument is required';

  my @dirs;
  my $collector = make_collector($test, \@dirs);

  my $start_depth = $root =~ tr[/][];
  my $stop_depth;
  if (defined $depth) {
    $stop_depth = $start_depth + $depth;
  }

  find({preprocess => sub {
          my $current_depth = $File::Find::name =~ tr[/][];

          my @dirs;
          if (!defined $stop_depth || $current_depth < $stop_depth) {
            @dirs = grep { -d && ! /^\.+$/ } @_;
          }

          return @dirs;
        },
        wanted => sub {
          my $current_depth = $File::Find::name =~ tr[/][];

          if (!defined $stop_depth || $current_depth < $stop_depth) {
            if ($regex) {
              $collector->($File::Find::name) if $_ =~ $regex;
            }
            else {
              $collector->($File::Find::name);
            }
          }
        }
       }, $root);

  return @dirs;
}

=head2 make_collector

  Arg [1]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.
  Arg [2]    : arrayref of an array into which matched object will be pushed
               if the test returns true.

  Example    : $collector = make_collector(sub { ... }, \@found);
  Description: Returns a function that will push matched objects onto a
               specified array.
  Returntype : coderef

=cut

sub make_collector {
  my ($test, $listref) = @_;

  return sub {
    my ($arg) = @_;

    my $collect = $test->($arg);
    push(@{$listref}, $arg) if $collect;

    return $collect;
  }
}

=head2 md5sum

  Arg [1]    : string path to a file

  Example    : my $md5 = md5sum($filename)
  Description: Calculate the MD5 checksum of a file.
  Returntype : Str

=cut

sub md5sum {
  my ($file) = @_;

  defined $file or croak 'A defined file argument is required';
  $file eq '' and croak 'A non-empty file argument is required';

  my @result = WTSI::NPG::Runnable->new(executable  => $MD5SUM,
                                        arguments   => [$file])->run;
  my $raw = shift @result;

  my ($md5) = $raw =~ m{^(\S+)\s+.*}msx;

  return $md5;
}

=head2 modified_between

  Arg [1]    : time in seconds since the epoch
  Arg [2]    : time in seconds since the epoch

  Example    : $test = modified_between($start_time, $end_time)
  Description: Return a function that accepts a single argument (a
               file name string) and returns true if that file has
               last been modified between the two specified times in
               seconds (inclusive).
  Returntype : coderef

=cut

sub modified_between {
  my ($start, $finish) = @_;

  return sub {
    my ($file) = @_;

    my $stat = stat($file);
    unless (defined $stat) {
      my $wd = `pwd`;
      croak "Failed to stat file '$file' in $wd: $!";
    }

    my $mtime = $stat->mtime;

    return ($start <= $mtime) && ($mtime <= $finish);
  }
}

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2012 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
