use utf8;

package WTSI::Genotyping::iRODS;

use strict;
use warnings;
use Carp;
use File::Basename qw(fileparse);

use vars qw(@ISA @EXPORT_OK);

use Exporter;
@ISA = qw(Exporter);

@EXPORT_OK = qw(ipwd
                list_object
                add_object
                remove_object
                add_object_meta
                get_object_meta
                remove_object_meta

                list_collection
                add_collection
                remove_collection
                add_collection_meta
                get_collection_meta
                remove_collection_meta);

our $IMETA = 'imeta';
our $IMKDIR = 'imkdir';
our $IPUT = 'iput';
our $IQUEST = 'iquest';
our $IRM = 'irm';
our $IPWD = 'ipwd';

sub ipwd {
  my @wd = _run_command($IPWD);

  return shift @wd;
}

sub list_object {
  my ($object) = @_;

  $object or croak "A non-empty object argument is required\n";

  my ($data_name, $collection) = fileparse($object);
  $collection =~ s!/$!!;

  if ($collection eq '.') {
      $collection = ipwd();
  }

  my @objects =
    _run_command($IQUEST, '"%s"',
                 "\"SELECT DATA_NAME WHERE DATA_NAME = '$data_name' AND " .
                 "COLL_NAME = '$collection'\"");

  return $objects[0] if @objects;
}

sub add_object {
  my ($file, $target) = @_;

  $file or croak "A non-empty file argument is required\n";
  $target or croak "A non-empty target (object) argument is required\n";

  $target = _ensure_absolute($target);
  _run_command($IPUT, $file, $target);

  return $target;
}

sub remove_object {
  my ($target) = @_;

  $target or croak "A non-empty target (object) argument is required\n";

  _irm($target);
}

sub get_object_meta {
  my ($object) = @_;

  $object or croak "A non-empty object argument is required\n";
  list_object($object) or croak "Object '$object' does not exist\n";

  return _parse_raw_meta(_run_command($IMETA, 'ls', '-d', $object))
}

sub add_object_meta {
  my ($object, $key, $value, $units) = @_;

  $units ||= '';

  if (meta_exists($key, $value, get_object_meta($object))) {
    croak "Metadata pair '$key' -> '$value' already exists for $object\n";
  }

  _run_command($IMETA, 'add', '-d', $object, $key, $value, $units);

  return ($key, $value, $units);
}

sub remove_object_meta {
  my ($object, $key, $value, $units) = @_;

  $object or croak "A non-empty object argument is required\n";
  $key or croak "A non-empty key argument is required\n";
  $value or croak "A non-empty value argument is required\n";
  $units ||= '';

  if (!meta_exists($key, $value, get_object_meta($object))) {
    croak "Metadata pair '$key' -> '$value' does not exist for $object\n";
  }

  _run_command($IMETA, 'rm', '-d', $object, $key, $value, $units);

  return ($key, $value, $units);
}

sub list_collection {
  my ($collection) = @_;

  $collection or croak "A non-empty collection argument is required\n";
  $collection =~ s!/$!!;

  my @objects = _safe_select(qq("SELECT COUNT(DATA_NAME)
                                 WHERE COLL_NAME = '$collection'"),
                             qq("SELECT DATA_NAME
                                 WHERE COLL_NAME = '$collection'"));
  my @collections = _safe_select(qq("SELECT COUNT(COLL_NAME)
                                     WHERE COLL_PARENT_NAME = '$collection'"),
                                 qq("SELECT COLL_NAME
                                     WHERE COLL_PARENT_NAME = '$collection'"));

  return (\@objects, \@collections);
}

sub add_collection {
  my ($dir, $target) = @_;

  $dir or croak "A non-empty dir argument is required\n";
  $target or croak "A non-empty target (collection) argument is required\n";

  $target = _ensure_absolute($target);
  _run_command($IPUT, '-r', $dir, $target);

  return $target;
}

sub remove_collection {
  my ($target) = @_;

  $target or croak "A non-empty target (object) argument is required\n";

  _irm($target);
}

sub get_collection_meta {
  my ($collection) = @_;

  $collection or croak "A non-empty collection argument is required\n";
  $collection =~ s!/$!!;

  return _parse_raw_meta(_run_command($IMETA, 'ls', '-C', $collection))
}

sub add_collection_meta {
  my ($collection, $key, $value, $units) = @_;

  $collection or croak "A non-empty collection argument is required\n";
  $key or croak "A non-empty key argument is required\n";
  $value or croak "A non-empty value argument is required\n";

  $units ||= '';
  $collection =~ s!/$!!;

  _run_command($IMETA, 'add', '-C', $collection, $key, $value, $units);

  return ($key, $value, $units);
}

sub remove_collection_meta {
  my ($collection, $key, $value, $units) = @_;

  $collection or croak "A non-empty collection argument is required\n";
  $key or croak "A non-empty key argument is required\n";
  $value or croak "A non-empty value argument is required\n";

  $units ||= '';
  $collection =~ s!/$!!;

  if (!meta_exists($key, $value, get_collection_meta($collection))) {
    croak "Metadata pair '$key' -> '$value' does not exist for $collection\n";
  }

  _run_command($IMETA, 'rm', '-C', $collection, $key, $value, $units);

  return ($key, $value, $units);
}

sub meta_exists {
  my ($key, $value, %meta) = @_;

  exists $meta{$key} and grep { $value } @{$meta{$key}};
}

sub _ensure_absolute {
  my ($target) = @_;

  my $absolute = $target;
  unless ($target =~ /^\//) {
    $absolute = ipwd() . '/' . $absolute;
  }

  return $absolute;
}

sub _parse_raw_meta {
  my @raw_meta = @_;

  @raw_meta = grep { m/^[attribute|value|units]/ } @raw_meta;
  my $n = scalar @raw_meta;
  unless ($n % 3 == 0) {
    croak "Expected imeta triples, but found $n elements\n";
  }

  my %meta;
  for (my $i = 0; $i < $n; $i += 3) {
    my ($str0, $str1, $str2) = @raw_meta[$i .. $i + 2];

    my ($attribute) = $str0 =~ /^attribute: (.*)/ or
      croak "Invalid triple $i: expected an attribute but found '$str0'\n";

    my ($value) = $str1 =~ /^value: (.*)/ or
      croak "Invalid triple $i: expected a value but found '$str1'\n";

    my ($units) = $str2 =~ /^units: (.*)/ or
      croak "Invalid triple $i: expected units but found '$str2'";

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

  my @count = _run_command($IQUEST, '"%s"', $icount);
  if (@count && $count[0] > 0) {
    push(@result, _run_command($IQUEST, '"%s"', $iquery));
  }

  return @result;
}

sub _irm {
  my (@args) = @_;

  _run_command($IRM, '-r', join(" ", @args));

  return @args;
}

sub _run_command {
  my @command = @_;

  my $command = join(' ', @command);

  open(EXEC, "$command |")
    or die "Failed open pipe to command '$command': $!\n";

  my @result;
  while (<EXEC>) {
    chomp;
    push(@result, $_);
  }

  close(EXEC);

  if ($?) {
    croak "Execution of '$command' failed with exit code: $?\n";
  }

  return @result;
}

1;
