
use utf8;

package WTSI::NPG::iRODS::Path;

use JSON;
use File::Spec;
use Moose::Role;

use WTSI::NPG::Metadata qw($STUDY_ID_META_KEY);
use WTSI::NPG::iRODS2;

with 'WTSI::NPG::Loggable', 'WTSI::NPG::Annotatable';

has 'collection' => (is => 'ro', isa => 'Str', required => 1,
                     default => '.', lazy => 1,
                     predicate => 'has_collection');

has 'irods' => (is => 'ro', isa => 'WTSI::NPG::iRODS2', required => 1);

# The following overrides the definition in
# WTSI::NPG::Annotatable. Apparently our old version of Moose doesn't
# support this attribute inheritance
#
has 'metadata' => (is => 'rw',
                   isa => 'ArrayRef',
                   predicate => 'has_metadata',
                   clearer => 'clear_metadata');
#
# When on a newer Moose, remove the above and replace with this:
#
# has '+metadata' => (predicate => 'has_metadata',
#                     clearer => 'clear_metadata');

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 2 && ref $args[0] eq 'WTSI::NPG::iRODS2') {
    return $class->$orig(irods      => $args[0],
                         collection => $args[1]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub BUILD {
  my ($self) = @_;

  # Make our logger be the iRODS logger by default
  $self->logger($self->irods->logger);
}

around 'metadata' => sub {
   my ($orig, $self, @args) = @_;

   my @sorted = sort { $a->{attribute} cmp $b->{attribute} ||
                       $a->{value}     cmp $b->{value}     ||
                       $a->{units}     cmp $b->{units} } @{$self->$orig(@args)};
   return \@sorted;
};

=head2 get_avu

  Arg [1]    : attribute
  Arg [2]    : value (optional)
  Arg [2]    : units (optional)

  Example    : $path->get_avu('foo')
  Description: Return a single matching AVU. If multiple candidate AVUs
               match the arguments, an error is raised.
  Returntype : HashRef
  Caller     : general

=cut

sub get_avu {
  my ($self, $attribute, $value, $units) = @_;
  $attribute or $self->logcroak("An attribute argument is required");

  my @exists = $self->find_in_metadata($attribute, $value, $units);

  my $avu;
  if (@exists) {
    if (scalar @exists == 1) {
      $avu = $exists[0];
    }
    else {
      $value ||= '';
      $units ||= '';

      my $fn = sub {
        my $avu = shift;

        return sprintf("{'%s', '%s', '%s'}", $avu->{attribute}, $avu->{value},
                       $avu->{units});
      };

      my $matched = join ", ", map { $fn->($_) } @exists;

      $self->logconfess("Failed to get a single AVU matching ",
                        "{'$attribute', '$value', '$units'}: ",
                        "matched [$matched]");
    }
  }

  return $avu;
}

sub find_in_metadata {
  my ($self, $attribute, $value, $units) = @_;

  my @meta = @{$self->metadata};
  my @exists;

  if ($value && $units) {
    @exists = grep { $_->{attribute} eq $attribute &&
                     $_->{value}     eq $value &&
                     $_->{units}     eq $units } @meta;
  }
  elsif ($value) {
    @exists = grep { $_->{attribute} eq $attribute &&
                     $_->{value}     eq $value } @meta;
  }
  else {
    @exists = grep { $_->{attribute} eq $attribute } @meta;
  }

  return @exists;
}

=head2 expected_irods_groups

  Arg [1]    : None

  Example    : @groups = $path->expected_irods_groups
  Description: Return an array of iRODS group names given metadata containing
               >=1 study_id under the key $STUDY_ID_META_KEY
  Returntype : Array

=cut

sub expected_irods_groups {
  my ($self) = @_;

  my @ss_study_avus = $self->find_in_metadata($STUDY_ID_META_KEY);
  unless (@ss_study_avus) {
    $self->logwarn("Did not find any study information in metadata");
  }

  my @groups;
  foreach my $avu (@ss_study_avus) {
    my $study_id = $avu->{value};
    my $group = $self->irods->make_group_name($study_id);
    push(@groups, $group);
  }

  return @groups;
}

sub meta_str {
  my ($self) = @_;

  return $self->json;
}

sub meta_json {
  my ($self) = @_;

  return JSON->new->utf8->encode($self->metadata);
}

no Moose;

1;
