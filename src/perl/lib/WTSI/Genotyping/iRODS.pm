use utf8;

package WTSI::Genotyping::iRODS;

use strict;
use warnings;
use Carp;
use File::Basename qw(fileparse);

use vars qw(@ISA @EXPORT_OK);

use Exporter;
@ISA = qw(Exporter);

@EXPORT_OK = qw(ils
                irm

                add_object
                add_object_meta
                get_object_meta
                remove_object_meta

                add_collection
                add_collection_meta
                get_collection_meta
                remove_collection_meta);

our $ILS = 'ils';
our $IPUT = 'iput';
our $IMETA = 'imeta';
our $IMKDIR = 'imkdir';
our $IRM = 'irm';

sub add_object {
  my ($file, $target) = @_;

  _run_command($IPUT, $file, $target);

  return $target;
}

sub get_object_meta {
  my ($object) = @_;

  unless (ils($object)) {
    croak "Object '$object' does not exist\n";
  }

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
  $units ||= '';

  if (!meta_exists($key, $value, get_object_meta($object))) {
    croak "Metadata pair '$key' -> '$value' does not exist for $object\n";
  }

  _run_command($IMETA, 'rm', '-d', $object, $key, $value, $units);

  return ($key, $value, $units);
}

sub add_collection {
  my ($collection) = @_;
  $collection or croak "A non-empty collection argument is required\n";

}

sub get_collection_meta {
  my ($collection) = @_;

  # iRODS will not recognise a collection with trailing slash
  $collection =~ s!/$!!;

  return _parse_raw_meta(_run_command($IMETA, 'ls', '-C', $collection))
}

sub add_collection_meta {
  my ($collection, $key, $value, $units) = @_;
  $collection or croak "A non-empty collection argument is required\n";
  $key or croak "A non-empty key argument is required\n";
  $value or croak "A non-empty value argument is required\n";
  $units ||= '';

  # iRODS will not recognise a collection with trailing slash
  $collection =~ s!/$!!;

  _run_command($IMETA, 'add', '-C', $collection, $key, $value, $units);
}

sub ils {
  my ($target) = @_;

  return _run_command($ILS, $target);
}

sub irm {
  my ($target, @args) = @_;

  _run_command($IRM, $target);

  return $target;
}

sub meta_exists {
  my ($key, $value, %meta) = @_;

  exists $meta{$key} and grep { $value } @{$meta{$key}};
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
