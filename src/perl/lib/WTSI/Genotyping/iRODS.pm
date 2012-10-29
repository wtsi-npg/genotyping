use utf8;

package WTSI::Genotyping::iRODS;

use strict;
use warnings;
use Carp;
use Log::Log4perl;
use File::Basename qw(basename fileparse);

use WTSI::Genotyping qw(run_command);

use vars qw(@ISA @EXPORT_OK);

use Exporter;
@ISA = qw(Exporter);

@EXPORT_OK = qw(ipwd
                list_object
                add_object
                get_object_checksum
                checksum_object
                remove_object
                add_object_meta
                batch_object_meta
                get_object_meta
                remove_object_meta

                list_collection
                add_collection
                put_collection
                remove_collection
                add_collection_meta

                get_collection_meta
                remove_collection_meta
                meta_exists);

our $ICHKSUM = 'ichksum';
our $IMETA = 'imeta';
our $IMKDIR = 'imkdir';
our $IPUT = 'iput';
our $IQUEST = 'iquest';
our $IRM = 'irm';
our $IPWD = 'ipwd';

our $log = Log::Log4perl->get_logger('genotyping');

=head2 ipwd

  Arg [1]    : None
  Example    : $dir = ipwd()
  Description: Returns the current iRODS working directory.
  Returntype : string
  Caller     : general

=cut

sub ipwd {
  my @wd = run_command($IPWD);

  return shift @wd;
}

=head2 get_object_checksum

  Arg [1]    : iRODS data object name
  Example    : $cs = get_object_checksum('/my/path/lorem.txt')
  Description: Returns the MD5 checksum of an iRODS data object.
  Returntype : string
  Caller     : general

=cut

sub get_object_checksum {
  my ($object) = @_;

  $object or
    $log->logconfess('A non-empty object argument is required');

  my ($data_name, $collection) = fileparse($object);

  $collection =~ s!/$!!;

  if ($collection eq '.') {
    $collection = ipwd();
  }

  my @raw_checksum = run_command($ICHKSUM, qq('$object'));
  unless (@raw_checksum) {
    $log->logconfess("Failed to get iRODS checksum for '$object'");
  }

  my $checksum = shift @raw_checksum;
  $checksum =~ s/.*([0-9a-f]{32})$/$1/;

  return $checksum;
}


=head2 checksum_object

  Arg [1]    : iRODS data object name
  Example    : checksum_object('/my/path/lorem.txt')
  Description: Returns true if the MD5 checksum in the metadata of an iRODS
               object is identical to the MD5 caluclated by iRODS.
  Returntype : boolean
  Caller     : general

=cut

sub checksum_object {
  my ($object) = @_;

  my $identical = 0;
  my %meta = get_object_meta($object);

  if (exists $meta{md5}) {
    my $irods_md5 = get_object_checksum($object);
    my $md5 = shift @{$meta{md5}};

    if ($md5 eq $irods_md5) {
      $log->debug("Confirmed '$object' MD5 as ", $md5);
      $identical = 1;
    }
    else {
      $log->warn("Expected MD5 of $irods_md5 but found $md5 for '$object'");
    }
  }
  else {
    $log->warn("MD5 metadata is missing from '$object'");
  }

  return $identical;
}

=head2 list_object

  Arg [1]    : iRODS data object name
  Example    : $obj = list_object($object)
  Description: Returns the full path of the object.
  Returntype : string
  Caller     : general

=cut

sub list_object {
  my ($object) = @_;

  $object or $log->logconfess('A non-empty object argument is required');

  my ($data_name, $collection) = fileparse($object);
  $collection =~ s!/$!!;

  if ($collection eq '.') {
      $collection = ipwd();
  }

  my $command = join(' ', $IQUEST, '"%s"',
                     qq("SELECT DATA_NAME \
                         WHERE DATA_NAME = '$data_name' \
                         AND COLL_NAME = '$collection'"));

  my $name = `$command 2> /dev/null`;
  chomp($name);

  return $name;
}

=head2 add_object

  Arg [1]    : Name of file to add to iRODs
  Arg [2]    : iRODS data object name
  Example    : add_object('lorem.txt', '/my/path/lorem.txt')
  Description: Adds a file to iRODS.
  Returntype : string
  Caller     : general

=cut

sub add_object {
  my ($file, $target) = @_;

  $file or $log->logconfess('A non-empty file argument is required');
  $target or
    $log->logconfess('A non-empty target (object) argument is required');

  $target = _ensure_absolute($target);
  $log->debug("Adding object '$target'");
  run_command($IPUT, $file, $target);

  return $target;
}

=head2 remove_object

  Arg [1]    : iRODS data object name
  Example    : remove_object('/my/path/lorem.txt')
  Description: Removes a data object.
  Returntype : string
  Caller     : general

=cut

sub remove_object {
  my ($target) = @_;

  $target or
    $log->logconfess('A non-empty target (object) argument is required');

  $log->debug("Removing object '$target'");
  return _irm($target);
}

=head2 get_object_meta

  Arg [1]    : iRODS data object name
  Example    : get_object_meta('/my/path/lorem.txt')
  Description: Gets metadata on a data object. Where there are multiple
               values for one key, the values are contained in an array under
               that key.
  Returntype : hash
  Caller     : general

=cut

sub get_object_meta {
  my ($object) = @_;

  $object or $log->logconfess('A non-empty object argument is required');
  list_object($object) or $log->logconfess("Object '$object' does not exist");

  return _parse_raw_meta(run_command($IMETA, 'ls', '-d', $object))
}

=head2 add_object_meta

  Arg [1]    : iRODS data object name
  Arg [2]    : key
  Arg [3]    : value
  Arg [4]    : units (optional)
  Example    : add_object_meta('/my/path/lorem.txt', 'id', 'ABCD1234')
  Description: Adds metadata to a data object. Returns an array of
               the new key, value and units.
  Returntype : array
  Caller     : general

=cut

sub add_object_meta {
  my ($object, $key, $value, $units) = @_;

  $units ||= '';

  $log->debug("Adding metadata pair '$key' -> '$value' to $object");
  if (meta_exists($key, $value, get_object_meta($object))) {
    $log->logconfess("Metadata pair '$key' -> '$value' ",
                     "already exists for $object");
  }

  run_command($IMETA, 'add', '-d', qq($object "$key" "$value" "$units"));

  return ($key, $value, $units);
}

sub batch_object_meta {
  my ($object, $meta_tuples) = @_;

  open(my $imeta, '|', "$IMETA > /dev/null")
    or $log->logconfess("Failed open pipe to command '$IMETA': $!");
  foreach my $tuple (@$meta_tuples) {
    my ($key, $value, $units) = @$tuple;
    $units ||= '';

    $log->debug("Adding metadata pair '$key' -> '$value' to $object");
    print $imeta qq(add -d $object "$key" "$value" "$units"), "\n";
  }
  close($imeta) or warn "Failed to close pipe to command '$IMETA'\n";

  # WARNING: imeta exits with the error code for the last operation in
  # the batch. An error followed by a success will be reported as a
  # success.

  if ($?) {
    $log->logconfess("Execution of '$IMETA' failed with exit code: $?");
  }

  return $object;
}

=head2 remove_object_meta

  Arg [1]    : iRODS data object name
  Arg [2]    : key
  Arg [3]    : value
  Arg [4]    : units (optional)
  Example    : remove_object_meta('/my/path/lorem.txt', 'id', 'ABCD1234')
  Description: Removes metadata to a data object. Returns an array of
               the removed key, value and units.
  Returntype : array
  Caller     : general

=cut

sub remove_object_meta {
  my ($object, $key, $value, $units) = @_;

  $object or $log->logconfess('A non-empty object argument is required');
  $key or $log->logconfess('A non-empty key argument is required');
  $value or $log->logconfess('A non-empty value argument is required');
  $units ||= '';

  $log->debug("Removing metadata pair '$key' -> '$value' from $object");
  if (!meta_exists($key, $value, get_object_meta($object))) {
    $log->logcluck("Metadata pair '$key' -> '$value' ",
                   "does not exist for $object");
  }

  run_command($IMETA, 'rm', '-d', qq($object "$key" "$value" "$units"));

  return ($key, $value, $units);
}

=head2 list_collection

  Arg [1]    : iRODS collection name
  Example    : $dir = list_collection($coll)
  Description: Returns the contents of the collectionas two arrayrefs,
               the first listing data objects, the second listing nested
               collections.
  Returntype : array
  Caller     : general

=cut

sub list_collection {
  my ($collection) = @_;

  $collection or
    $log->logconfess('A non-empty collection argument is required');
  $collection =~ s!/$!!;
  $collection = _ensure_absolute($collection);

  my @root = _safe_select(qq("SELECT COUNT(COLL_NAME) \
                              WHERE COLL_NAME = '$collection'"),
                          qq("SELECT COLL_NAME \
                              WHERE COLL_NAME = '$collection'"));

  $log->debug("Listing collection '$collection'");

  if (@root) {
    $log->debug("Collection '$collection' exists");
    my @objs = _safe_select(qq("SELECT COUNT(DATA_NAME)
                                WHERE COLL_NAME = '$collection'"),
                            qq("SELECT DATA_NAME \
                                WHERE COLL_NAME = '$collection'"));
    my @colls = _safe_select(qq("SELECT COUNT(COLL_NAME) \
                                 WHERE COLL_PARENT_NAME = '$collection'"),
                             qq("SELECT COLL_NAME \
                                 WHERE COLL_PARENT_NAME = '$collection'"));

    $log->debug("Collection '$collection' contains ", scalar @objs,
                " data objects and ", scalar @colls, " collections");


    return (\@objs, \@colls);
  }
  else {
    $log->debug("Collection '$collection' does not exist");
    return;
  }
}

=head2 add_collection

  Arg [1]    : iRODS collection name
  Example    : add_collection('/my/path/foo')
  Description: Makes a new collection in iRODS. Returns the new collection.
  Returntype : string
  Caller     : general

=cut

sub add_collection {
  my ($collection) = @_;

  $collection or
    $log->logconfess('A non-empty collection argument is required');
  $collection =~ s!/$!!;
  $collection = _ensure_absolute($collection);

  $log->debug("Adding collection '$collection'");
  run_command($IMKDIR, '-p', $collection);

  return $collection;
}

=head2 put_collection

  Arg [2]    : iRODS collection name
  Example    : put_collection('/my/path/foo', )
  Description: Makes a new collection in iRODS. Returns the new collection.
  Returntype : string
  Caller     : general

=cut

sub put_collection {
  my ($dir, $target) = @_;

  $dir or
    $log->logconfess('A non-empty directory argument is required');
  $target =~ s!/$!!;
  $target = _ensure_absolute($target);

  $log->debug("Putting collection '$dir'");
  run_command($IPUT, '-r', $dir, $target);

  return $target . '/' . basename($dir);
}

=head2 remove_collection

  Arg [1]    : iRODS collection name
  Example    : remove_collection('/my/path/foo')
  Description: Removes a collection and contents, recursively.
  Returntype : string
  Caller     : general

=cut

sub remove_collection {
  my ($target) = @_;

  $target or
    $log->logconfess('A non-empty target (object) argument is required');

  $log->debug("Removing collection '$target'");
  return _irm($target);
}

=head2 get_collection_meta

  Arg [1]    : iRODS data collection name
  Example    : get_collection_meta('/my/path/lorem.txt')
  Description: Gets metadata on a collection. Where there are multiple
               values for one key, the values are contained in an array under
               that key.
  Returntype : hash
  Caller     : general

=cut

sub get_collection_meta {
  my ($collection) = @_;

  $collection or
    $log->logconfess('A non-empty collection argument is required');
  $collection =~ s!/$!!;

  return _parse_raw_meta(run_command($IMETA, 'ls', '-C', $collection))
}

=head2 add_collection_meta

  Arg [1]    : iRODS collection name
  Arg [2]    : key
  Arg [3]    : value
  Arg [4]    : units (optional)
  Example    : add_collection_meta('/my/path/foo', 'id', 'ABCD1234')
  Description: Adds metadata to a collection. Returns an array of
               the new key, value and units.
  Returntype : array
  Caller     : general

=cut

sub add_collection_meta {
  my ($collection, $key, $value, $units) = @_;

  $collection or
    $log->logconfess('A non-empty collection argument is required');

  $key or $log->logconfess('A non-empty key argument is required');
  $value or $log->logconfess('A non-empty value argument is required');

  $units ||= '';
  $collection =~ s!/$!!;

  $log->debug("Adding metadata pair '$key' -> '$value' to $collection");
  if (meta_exists($key, $value, get_collection_meta($collection))) {
    $log->logconfess("Metadata pair '$key' -> '$value' ",
                     "already exists for $collection");
  }

  run_command($IMETA, 'add', '-C', qq($collection "$key" "$value" "$units"));

  return ($key, $value, $units);
}

=head2 remove_collection_meta

  Arg [1]    : iRODS collection name
  Arg [2]    : key
  Arg [3]    : value
  Arg [4]    : units (optional)
  Example    : remove_collection_meta('/my/path/foo', 'id', 'ABCD1234')
  Description: Removes metadata to a data object. Returns an array of
               the removed key, value and units.
  Returntype : array
  Caller     : general

=cut

sub remove_collection_meta {
  my ($collection, $key, $value, $units) = @_;

  $collection or
    $log->logconfess('A non-empty collection argument is required');
  $key or $log->logconfess('A non-empty key argument is required');
  $value or $log->logconfess('A non-empty value argument is required');

  $units ||= '';
  $collection =~ s!/$!!;

  $log->debug("Removing metadata pair '$key' -> '$value' from $collection");
  if (!meta_exists($key, $value, get_collection_meta($collection))) {
    $log->logcluck("Metadata pair '$key' -> '$value' ",
                   "does not exist for $collection");
  }

  run_command($IMETA, 'rm', '-C', qq($collection "$key" "$value" "$units"));

  return ($key, $value, $units);
}

sub meta_exists {
  my ($key, $value, %meta) = @_;

  return exists $meta{$key} and grep { $_ eq $value } @{$meta{$key}};
}

sub _ensure_absolute {
  my ($target) = @_;

  my $absolute = $target;
  unless ($target =~ m/^\//) {
    $absolute = ipwd() . '/' . $absolute;
  }

  return $absolute;
}

sub _parse_raw_meta {
  my @raw_meta = @_;

  @raw_meta = grep { m/^[attribute|value|units]/ } @raw_meta;
  my $n = scalar @raw_meta;
  unless ($n % 3 == 0) {
    $log->logconfess("Expected imeta triples, but found $n elements");
  }

  my %meta;
  for (my $i = 0; $i < $n; $i += 3) {
    my ($str0, $str1, $str2) = @raw_meta[$i .. $i + 2];

    my ($attribute) = $str0 =~ m/^attribute: (.*)/ or
      $log->logconfess("Invalid triple $i: expected an attribute but found ",
                     "'$str0'");

    my ($value) = $str1 =~ m/^value: (.*)/ or
      $log->logconfess("Invalid triple $i: expected a value but found '$str1'");

    my ($units) = $str2 =~ m/^units: (.*)/ or
      $log->logconfess("Invalid triple $i: expected units but found '$str2'");

    if (exists $meta{$attribute}) {
      push(@{$meta{$attribute}}, $value);
    }
    else {
      $meta{$attribute} = [$value];
    }
  }

  return %meta;
}

sub _safe_select {
  my ($icount, $iquery) = @_;

  my @result;

  my @count = run_command($IQUEST, '"%s"', $icount);
  if (@count && $count[0] > 0) {
    push(@result, run_command($IQUEST, '"%s"', $iquery));
  }

  return @result;
}

sub _irm {
  my @args = @_;

  run_command($IRM, '-r', join(' ', @args));

  return @args;
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
