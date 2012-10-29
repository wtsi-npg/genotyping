use utf8;

package WTSI::Genotyping;

use strict;
use warnings;
use Carp;
use Cwd qw(abs_path);
use File::Find;
use Log::Log4perl;

use vars qw(@ISA @EXPORT_OK);

use Exporter;
@ISA = qw(Exporter);

@EXPORT_OK = qw(common_stem
                collect_files
                collect_dirs
                make_collector
                modified_between
                md5sum
                hash_path
                run_command);

my $log = Log::Log4perl->get_logger('genotyping');

=head2 common_stem

  Arg [1]    : string
  Arg [2]    : string
  Example    : $stem = common_stem("foo13240a", "foo199")
  Description: Returns the common part of the two arguments, starting
               from the left (index 0). If one or more of the arguments
               are empty strings, or the arguments differ at the first
               character, an empty string is returned.
  Returntype : string
  Caller     : general

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

=head2 collect_files

  Arg [1]    : Root directory
  Arg [2]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.
  Arg [3]    : Maximum depth to search below the starting directory.
  Arg [4]    : A file matching regex that is applied in addition to to
               the test. Optional.
  Example    : @files = collect_files('/home', $modified, 3, qr/.txt$/i)
  Description: Returns an array of file names present under the specified
               root, for which the test predicate returns true, up to the
               specified depth.
  Returntype : array of strings (file names)
  Caller     : general

=cut

sub collect_files {
  my ($root, $test, $depth, $regex) = @_;

  my @files;
  my $collector = make_collector($test, \@files);

  my $start_depth = $root =~ tr[/][];
  my $abs_depth = $start_depth + $depth;

  find({preprocess => sub {
          my $d = $File::Find::dir =~ tr[/][];

          my @elts;
          if ($d < $abs_depth) {
            @elts = grep { ! /^\.+$/ } @_;
          }

          return @elts;
        },
        wanted => sub {
          my $d = $File::Find::dir =~ tr[/][];

          if ($d < $abs_depth && -f) {
            if ($regex) {
              $collector->($File::Find::name) if $_ =~ $regex;
            }
            else {
              $collector->($File::Find::name)
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
  Example    : @dirs = collect_dirs('/home', $modified, 2)
  Description: Returns an array of directory names present under the specified
               root, for which the test predicate returns true, up to the
               specified depth.
  Returntype : array of strings (dir names)
  Caller     : general

=cut

sub collect_dirs {
  my ($root, $test, $depth) = @_;

  my @dirs;
  my $collector = make_collector($test, \@dirs);

  my $start_depth = $root =~ tr[/][];
  my $abs_depth = $start_depth + $depth;

  find({preprocess => sub {
          my $d = $File::Find::name =~ tr[/][];

          my @dirs;
          if ($d < $abs_depth) {
            @dirs = grep { -d && ! /^\.+$/ } @_;
          }

          return @dirs;
        },
        wanted => sub {
          my $d = $File::Find::name =~ tr[/][];

          if ($d < $abs_depth) {
            $collector->($File::Find::name);
          }
        }
       }, $root);

  return @dirs;
}

=head2 make_collector

  Arg [1]    : coderef of a function that accepts a single argument and
               returns true if that object is to be collected.
  Arg [2]    : arrayref of an array into which matched objectc will be pushed
               if the test returns true.
  Example    : $collector = make_collector(sub { ... }, \@found);
  Description: Returns a function that will push matched objects onto a
               specified array.
  Returntype : coderef
  Caller     : general

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

=head2 modified_between

  Arg [1]    : time in seconds since the epoch
  Arg [2]    : time in seconds since the epoch
  Example    : $test = modified_between($start_time, $end_time)
  Description: Returns a function that accepts a single argument (a
               file name string) and returns true if that file has
               last been modified between the two specified times in
               seconds (inclusive).
  Returntype : coderef
  Caller     : general

=cut

sub modified_between {
  my ($start, $finish) = @_;

  return sub {
    my ($file) = @_;
    my $mtime = (stat $file)[9];

    return ($start <= $mtime) && ($mtime <= $finish);
  }
}

=head2 md5sum

  Arg [1]    : string path to a file
  Example    : my $md5 = md5sum($filename)
  Description: Calculates the MD5 checksum of a file.
  Returntype : string
  Caller     : general

=cut

sub md5sum {
  my ($file) = @_;

  my @result = run_command("md5sum $file");
  my $raw = shift @result;
  my ($md5) = $raw =~ m{^(\S+)\s+\S+$}msx;

  return $md5;
}

=head2 hash_path

  Arg [1]    : string path to a file
  Arg [2]    : MD5 checksum (optional)
  Example    : my $path = hash_path($filename)
  Description: Returns a hashed path 3 directories deep, each level having
               a maximum of 256 subdirectories, calculated from the file's
               MD5. If the optional MD5 argument is supplied, the MD5
               calculation is skipped and the provided value is used instead.
  Returntype : string
  Caller     : general

=cut

sub hash_path {
  my ($file, $md5sum) = @_;

  $md5sum ||= md5sum($file);

  my @levels = $md5sum =~ m{\G(..)}gmsx;

  return join('/', @levels[0..2]);
}

sub run_command {
  my @command = @_;

  my $command = join(' ', @command);

  open(my $exec, '-|', "$command")
    or $log->logconfess("Failed open pipe to command '$command': $!");

  $log->debug("Running child '$command'");

  my @result;
  while (<$exec>) {
    chomp;
    push(@result, $_);
  }

  close($exec) or warn "Failed to close pipe to command '$command'\n";

  my $returned = $?;
  if ($returned) {
    my $signal = $returned & 127;
    my $exit = $returned >> 8;

    if ($signal) {
      $log->logconfess("Execution of '$command' died from signal: $signal");
    }
    else {
      $log->logconfess("Execution of '$command' failed with exit code: $exit");
    }
  }

  return @result;
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
