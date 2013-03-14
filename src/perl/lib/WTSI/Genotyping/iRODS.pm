use utf8;

package WTSI::Genotyping::iRODS;

use strict;
use warnings;
use Carp;
use Cwd qw(abs_path);
use File::Find;
use Log::Log4perl;
use File::Basename qw(basename fileparse);

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
                find_objects_by_meta

                list_collection
                add_collection
                put_collection
                remove_collection
                add_collection_meta
                get_collection_meta
                remove_collection_meta
                find_collections_by_meta

                meta_exists

                make_group_name
                find_or_make_group
                group_exists
                add_group
                set_group_access

                collect_files
                collect_dirs
                make_collector
                modified_between
                md5sum
                hash_path);

our $IADMIN = 'iadmin';
our $ICHKSUM = 'ichksum';
our $IMETA = 'imeta';
our $IMKDIR = 'imkdir';
our $IPUT = 'iput';
our $IQUEST = 'iquest';
our $IRM = 'irm';
our $IPWD = 'ipwd';
our $ICHMOD = 'ichmod';

our $log = Log::Log4perl->get_logger('npg.irods.publish');


=head2 make_group_name

  Arg [1]    : A SequenceScape study ID.
  Example    : make_group_name(1234))
  Description: Returns an iRODS group name given a SequenceScape study ID.
  Returntype : string
  Caller     : general

=cut

sub make_group_name {
  my ($study_id) = @_;

  return "ss_" . $study_id;
}

=head2 find_or_make_group

  Arg [1]    : iRODS group name
  Example    : find_or_create_group($name)
  Description: Creates a new iRODS group if it does not exist. Returns
               the group name.
  Returntype : string
  Caller     : general

=cut

sub find_or_make_group {
  my ($group_name) = @_;

  my $group;

  $log->debug("Checking for iRODS group '$group_name'");

  if (group_exists($group_name)) {
    $group = $group_name;
    $log->debug("An iRODS group '$group' exists; a new group will not be added");
  }
  else {
    $group = add_group($group_name);
    $log->info("Added a new iRODS group '$group'");
  }

  return $group;
}


=head2 group_exists

  Arg [1]    : iRODS group name
  Example    : group_exists($name)
  Description: Returns true if the group exists, or false otherwise
  Returntype : boolean
  Caller     : general

=cut

sub group_exists {
  my ($name) = @_;

  grep { /^$name$/ } _run_command($IADMIN, 'lg');
}


=head2 add_group

  Arg [1]    : new iRODS group name
  Example    : add_group($name)
  Description: Creates a new group. Raises an error if the group exists
               already. Returns the group name. The group name is not escaped
               in nay way.
  Returntype : string
  Caller     : general

=cut

sub add_group {
  my ($name) = @_;

  if (group_exists($name)) {
    $log->logconfess("Failed to create iRODS group '$name' because it exists already");
  }

  _run_command($IADMIN, 'mkgroup', $name);

  return $name;
}


=head2 remove_group

  Arg [1]    : An existing iRODS group name.
  Example    : remove_group($name)
  Description: Creates a new group. Raises an error if the group exists
               already. Returns the group name. The group name is not escaped
               in any way.
  Returntype : string
  Caller     : general

=cut

sub remove_group {
  my ($name) = @_;

  if (! group_exists($name)) {
    $log->logconfess("Unable to remove group '$name' because it doesn't exist");
  }

  _run_command($IADMIN, 'rmgroup', $name);

  return $name;
}


=head2 set_group_access

  Arg [1]    : A permission string, 'read', 'write', 'own' or undef ('null')
  Arg [2]    : An iRODS group name.
  Arg [3]    : One or more data objects
  Example    : (set_group_access('read', 'public', $object1, $object2)
  Description: Sets the access rights on one or more objects for a group,
               returning the objects.
  Returntype : array
  Caller     : general

=cut

sub set_group_access {
  my ($permission, $group, @objects) = @_;

  my $perm_str;
  if (defined $permission) {
    $perm_str = $permission;
  }
  else {
    $perm_str = 'null';
  }

  _run_command($ICHMOD, $perm_str, $group, @objects);

  return @objects;
}


=head2 ipwd

  Arg [1]    : None
  Example    : $dir = ipwd()
  Description: Returns the current iRODS working directory.
  Returntype : string
  Caller     : general

=cut

sub ipwd {
  my @wd = _run_command($IPWD);

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

  my @raw_checksum = _run_command($ICHKSUM, qq('$object'));
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
                     qq("SELECT DATA_NAME
                         WHERE DATA_NAME = '$data_name'
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

  _run_command($IPUT, $file, $target);

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

  return _parse_raw_meta(_run_command($IMETA, 'ls', '-d', $object))
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

  _run_command($IMETA, 'add', '-d', qq($object "$key" "$value" "$units"));

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
  Description: Removes metadata from a data object. Returns an array of
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

  _run_command($IMETA, 'rm', '-d', qq($object "$key" "$value" "$units"));

  return ($key, $value, $units);
}


=head2 find_objects_by_meta

  Arg [1]    : iRODS collection pattern
  Arg [2]    : key
  Arg [3]    : value
  Arg [4]    : units
  Example    : find_objects_by_meta('/my/path/foo%', 'id', 'ABCD1234')
  Description: Finds objects by their metadata, restricted to a parent collection.
               Returns a list of collections.
  Returntype : array
  Caller     : general

=cut

sub find_objects_by_meta {
  my ($root, $key, $value, $units) = @_;

  my $unit_clause;
  if (defined $units) {
    $unit_clause = qq(AND META_DATA_ATTR_UNITS = '$units');
  }
  else {
    $unit_clause = '';
  }

  $log->debug("Finding by metadata '$key'  => '$value'");

  my @objs = _safe_select('"%s"', qq("SELECT COUNT(DATA_NAME)
                                      WHERE META_DATA_ATTR_NAME = '$key'
                                      AND META_DATA_ATTR_VALUE = '$value' $unit_clause
                                      AND COLL_NAME LIKE '$root'"),
                          '"%s/%s"', qq("SELECT COLL_NAME, DATA_NAME
                                         WHERE META_DATA_ATTR_NAME = '$key'
                                         AND META_DATA_ATTR_VALUE = '$value' $unit_clause
                                         AND COLL_NAME LIKE '$root'"));
  return \@objs;
}


=head2 list_collection

  Arg [1]    : iRODS collection name
  Example    : $dir = list_collection($coll)
  Description: Returns the contents of the collection as two arrayrefs,
               the first listing data objects, the second listing nested
               collections.
  Returntype : array
  Caller     : general

=cut

sub list_collection {
  my ($collection) = @_;

  $collection or
    $log->logconfess('A non-empty collection argument is required');

  $collection = _ensure_absolute($collection);
  $collection =~ s!/$!!;

  my @root = _safe_select('"%s"',
                          qq("SELECT COUNT(COLL_NAME)
                              WHERE COLL_NAME = '$collection'"),
                          '"%s"',
                          qq("SELECT COLL_NAME
                              WHERE COLL_NAME = '$collection'"));

  $log->debug("Listing collection '$collection'");

  if (@root) {
    $log->debug("Collection '$collection' exists");
    my @objs = _safe_select('"%s"', qq("SELECT COUNT(DATA_NAME)
                                        WHERE COLL_NAME = '$collection'"),
                            '"%s"', qq("SELECT DATA_NAME
                                        WHERE COLL_NAME = '$collection'"));
    my @colls = _safe_select('"%s"', qq("SELECT COUNT(COLL_NAME)
                                         WHERE COLL_PARENT_NAME = '$collection'"),
                             '"%s"', qq("SELECT COLL_NAME
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
  $collection = _ensure_absolute($collection);

  $log->debug("Adding collection '$collection'");
  _run_command($IMKDIR, '-p', $collection);

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

  $dir or $log->logconfess('A non-empty directory argument is required');

  $target = _ensure_absolute($target);
  $target =~ s!/$!!;

  $log->debug("Putting collection '$dir' into '$target'");
  _run_command($IPUT, '-r', $dir, $target);

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

  return _parse_raw_meta(_run_command($IMETA, 'ls', '-C', $collection))
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

  _run_command($IMETA, 'add', '-C', qq($collection "$key" "$value" "$units"));

  return ($key, $value, $units);
}

=head2 remove_collection_meta

  Arg [1]    : iRODS collection name
  Arg [2]    : key
  Arg [3]    : value
  Arg [4]    : units (optional)
  Example    : remove_collection_meta('/my/path/foo', 'id', 'ABCD1234')
  Description: Removes metadata from a collection object. Returns an array of
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

  _run_command($IMETA, 'rm', '-C', qq($collection "$key" "$value" "$units"));

  return ($key, $value, $units);
}


=head2 find_collections_by_meta

  Arg [1]    : iRODS collection pattern
  Arg [2]    : key
  Arg [3]    : value
  Arg [4]    : units
  Example    : find_collections_by_meta('/my/path/foo%', 'id', 'ABCD1234')
  Description: Finds collections by their metadata, restricted to a parent collection.
               Returns a list of collections.
  Returntype : array
  Caller     : general

=cut

sub find_collections_by_meta {
  my ($root, $key, $value, $units) = @_;

  my $unit_clause;
  if (defined $units) {
    $unit_clause = qq(AND META_COLL_ATTR_UNITS = '$units');
  }
  else {
    $unit_clause = '';
  }

  my @colls = _safe_select('"%s"', qq("SELECT COUNT(COLL_NAME)
                                      WHERE META_COLL_ATTR_NAME = '$key'
                                      AND META_COLL_ATTR_VALUE = '$value' $unit_clause
                                      AND COLL_NAME LIKE '$root'"),
                          '"%s"', qq("SELECT COLL_NAME
                                      WHERE META_COLL_ATTR_NAME = '$key'
                                      AND META_COLL_ATTR_VALUE = '$value' $unit_clause
                                      AND COLL_NAME LIKE '$root'"));
  return \@colls;
}


sub meta_exists {
  my ($key, $value, %meta) = @_;

  my $exists = 0;
  if (exists $meta{$key}) {
    foreach my $meta_value (@{$meta{$key}}) {
      if ($value eq $meta_value) {
        $exists = 1;
        last;
      }
    }
  }

  return $exists;
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

  my @result = _run_command("md5sum $file");
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

sub _run_command {
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
  my ($ctemplate, $icount, $qtemplate, $iquery) = @_;

  my @result;

  my @count = _run_command($IQUEST, $ctemplate, $icount);
  if (@count && $count[0] > 0) {
    push(@result, _run_command($IQUEST, '--no-page', $qtemplate, $iquery));
  }

  return @result;
}

sub _irm {
  my @args = @_;

  _run_command($IRM, '-r', join(' ', @args));

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
