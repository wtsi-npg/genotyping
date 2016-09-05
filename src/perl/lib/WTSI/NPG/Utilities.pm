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
our @EXPORT_OK = qw(common_stem
                    md5sum
                    trim
                    depad_well
                    user_session_log);

our $VERSION = '';

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
  $copy =~ s/^\s*//msx;
  $copy =~ s/\s*$//msx;

  return $copy;
}

=head2 depad_well

  Arg [1]    : string

  Example    : $depadded = depad_well('A01');
  Description: Removes the leading '0' from well addresses so that
               'A01' becomes 'A1' etc.
  Returntype : Str

=cut

sub depad_well {
  my ($str) = @_;

  if(! defined $str) {
    croak "The str argument was undefined";
  }
  if ($str !~ m{^[[:upper:]]\d\d?$}msx) {
    croak "Unexpected well address '$str'. Cannot depad this.";
  }
  if ($str =~ m{^[[:upper:]]00$}msx) {
    croak "Unexpected well address '$str'. Cannot depad this.";
  }

  my $depadded = $str;
  $depadded =~ s/^([[:upper:]])0?(\d)$/$1$2/msx;

  return $depadded;
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
  unless ($uid =~ m{[[:alnum:]]+}msx) {
    croak "The uid argument must match [A-Za-z0-9]+\n";
  }
  unless ($session_name =~ m{[[:alnum:]]+}msx) {
    croak "The session_name argument must match [A-Za-z0-9]+\n";
  }

  my $now = DateTime->now;
  return sprintf("%s/%s.%s.%s.log", $USER_SESSION_LOG_DIR,
                 $session_name, $uid, $now->strftime("%F"));
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

  my @result = WTSI::DNAP::Utilities::Runnable->new
    (executable  => $MD5SUM,
     arguments   => [$file])->run->split_stdout;
  my $raw = shift @result;

  my ($md5) = $raw =~ m{^(\S+)\s+.*}msx;

  return $md5;
}

1;

__END__

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
