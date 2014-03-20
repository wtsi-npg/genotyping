use utf8;

package WTSI::NPG::iRODS;

use Encode qw(decode);
use English;
use File::Basename qw(basename);
use File::Spec;
use JSON;
use List::AllUtils qw(any);
use Moose;

use WTSI::NPG::iRODS::Lister;
use WTSI::NPG::iRODS::MetaLister;
use WTSI::NPG::iRODS::MetaModifier;
use WTSI::NPG::iRODS::ACLModifier;
use WTSI::NPG::Runnable;

with 'WTSI::NPG::Loggable';

has 'environment' =>
  (is       => 'ro',
   isa      => 'HashRef',
   required => 1,
   default  => sub { \%ENV });

has 'working_collection' =>
  (is        => 'rw',
   isa       => 'Str',
   predicate => 'has_working_collection',
   clearer   => 'clear_working_collection');

has 'lister' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::Lister',
   required => 1,
   lazy     => 1,
   default  => sub {
     my ($self) = @_;

     return WTSI::NPG::iRODS::Lister->new
       (environment => $self->environment,
        max_size    => 1024 * 1204,
        arguments   => ['--acl'],
        logger      => $self->logger)->start;
   });

has 'meta_lister' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::MetaLister',
   required => 1,
   lazy     => 1,
   default  => sub {
     my ($self) = @_;

     return WTSI::NPG::iRODS::MetaLister->new
       (environment => $self->environment,
        logger      => $self->logger)->start;
   });

has 'meta_adder' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::MetaModifier',
   required => 1,
   lazy     => 1,
   default  => sub {
     my ($self) = @_;

     return WTSI::NPG::iRODS::MetaModifier->new
       (arguments   => ['--operation', 'add'],
        environment => $self->environment,
        logger      => $self->logger)->start;
   });

has 'meta_remover' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::MetaModifier',
   required => 1,
   lazy     => 1,
   default  => sub {
     my ($self) = @_;

     return WTSI::NPG::iRODS::MetaModifier->new
       (arguments   => ['--operation', 'rem'],
        environment => $self->environment,
        logger      => $self->logger)->start;
   });

has 'acl_modifier' =>
  (is         => 'ro',
   isa      => 'WTSI::NPG::iRODS::ACLModifier',
   required => 1,
   lazy     => 1,
   default  => sub {
     my ($self) = @_;

     return WTSI::NPG::iRODS::ACLModifier->new
       (environment => $self->environment,
        logger      => $self->logger)->start;
   });

our $IGROUPADMIN = 'igroupadmin';
our $ICD     = 'icd';
our $ICHKSUM = 'ichksum';
our $IGET    = 'iget';
our $IMETA   = 'imeta';
our $IMKDIR  = 'imkdir';
our $IMV     = 'imv';
our $IPUT    = 'iput';
our $IRM     = 'irm';
our $IPWD    = 'ipwd';
# our $ICHMOD  = 'ichmod';
our $MD5SUM  = 'md5sum';

our @VALID_PERMISSIONS = qw(null read write own);

around 'working_collection' => sub {
  my ($orig, $self, @args) = @_;

  if (@args) {
    my $collection = $args[0];
    $collection eq '' and
      $self->logconfess('A non-empty collection argument is required');

    $collection = File::Spec->canonpath($collection);
    $collection = $self->_ensure_absolute_path($collection);
    $self->debug("Changing working_collection to '$collection'");

    WTSI::NPG::Runnable->new(executable  => $ICD,
                             arguments   => [$collection],
                             environment => $self->environment)->run;
    $self->$orig($collection);
  }
  elsif (!$self->has_working_collection) {
    my ($wc) = WTSI::NPG::Runnable->new
      (executable  => $IPWD,
       environment => $self->environment)->run->split_stdout;

    $self->$orig($wc);
  }

  return $self->$orig;
};

=head2 find_zone_name

  Arg [1]    : An absolute iRODS path.

  Example    : $irods->find_zone('/zonename/path')
  Description: Return an iRODS zone name given a path.
  Returntype : Str

=cut

sub find_zone_name {
  my ($self, $path) = @_;

  defined $path or $self->logconfess('A defined path argument is required');

  $path = File::Spec->canonpath($path);
  my $abs_path = $self->_ensure_absolute_path($path);
  $abs_path =~ s/^\///;

  $self->debug("Determining zone from path '", $abs_path, "'");

  # If no zone if given, assume the current zone
  unless ($abs_path) {
    $self->debug("Using '", $self->working_collection, "' to determine zone");
    $abs_path = $self->working_collection;
  }

  my @path = grep { $_ ne '' } File::Spec->splitdir($abs_path);
  unless (@path) {
    $self->logconfess("Failed to parse iRODS zone from path '$path'");
  }

  my $zone = shift @path;
  return $zone;
}

=head2 make_group_name

  Arg [1]    : A SequenceScape study ID.

  Example    : $irods->make_group_name(1234)
  Description: Return an iRODS group name given a SequenceScape study ID.
  Returntype : Str

=cut

sub make_group_name {
  my ($self, $study_id) = @_;

  return "ss_" . $study_id;
}

=head2 list_groups

  Arg [1]    : None

  Example    : $irods->list_groups
  Description: Returns a list of iRODS groups
  Returntype : Array

=cut

sub list_groups {
  my ($self, @args) = @_;

  my @groups = WTSI::NPG::Runnable->new
    (executable  => $IGROUPADMIN,
     arguments   => ['lg'],
     environment => $self->environment)->run->split_stdout;
  return @groups;
}

=head2 group_exists

  Arg [1]    : iRODS group name
  Example    : group_exists($name)
  Description: Return true if the group exists, or false otherwise
  Returntype : Bool

=cut

sub group_exists {
  my ($self, $name) = @_;

  return grep { /^$name$/ } $self->list_groups;
}

=head2 add_group

  Arg [1]    : new iRODS group name
  Example    : $irods->add_group($name)
  Description: Create a new group. Raises an error if the group exists
               already. Returns the group name. The group name is not escaped
               in any way.
  Returntype : Str

=cut

sub add_group {
  my ($self, $name) = @_;

  if ($self->group_exists($name)) {
    $self->logconfess("Failed to create iRODS group '$name' because it exists");
  }

  WTSI::NPG::Runnable->new(executable  => $IGROUPADMIN,
                           arguments   => ['mkgroup', $name],
                           environment => $self->environment)->run;
  return $name;
}

=head2 remove_group

  Arg [1]    : An existing iRODS group name.
  Example    : remove_group($name)
  Description: Remove a group. Raises an error if the group does not exist.
               already. Returns the group name. The group name is not escaped
               in any way.
  Returntype : Str

=cut

sub remove_group {
  my ($self, $name) = @_;

  unless (group_exists($name)) {
    $self->logconfess("Unable to remove group '$name' because ",
                      "it doesn't exist");
  }

  WTSI::NPG::Runnable->new(executable  => $IGROUPADMIN,
                           arguments   => ['rmgroup', $name],
                           environment => $self->environment)->run;
  return $name;
}

=head2 set_group_access

  Arg [1]    : A permission string, 'read', 'write', 'own' or undef ('null')
  Arg [2]    : An iRODS group name.
  Arg [3]    : One or more data objects or collections

  Example    : $irods->set_group_access('read', 'public', $object1, $object2)
  Description: Set the access rights on one or more objects for a group,
               returning the objects.
  Returntype : Array

=cut

sub set_group_access {
  my ($self, $permission, $group, @objects) = @_;

  my $perm_str = defined $permission ? $permission : 'null';

  foreach my $object (@objects) {
    $self->set_object_permissions($perm_str, $group, $object);
  }

  return @objects;
}

=head2 reset_working_collection

  Arg [1]    : None

  Example    : $irods->reset_working_collection
  Description: Reset the current iRODS working collection to the home
               collection and return self.
  Returntype : WTSI::NPG::iRODS

=cut

sub reset_working_collection {
  my ($self) = @_;

  WTSI::NPG::Runnable->new(executable  => $ICD,
                           environment => $self->environment)->run;
  $self->clear_working_collection;

  return $self;
}

=head2 list_collection

  Arg [1]    : Str iRODS collection name
  Arg [1]    : Bool recurse flag.

  Example    : my ($objs, $colls) = $irods->list_collection($coll)
  Description: Return the contents of the collection as two arrayrefs,
               the first listing data objects, the second listing nested
               collections.
  Returntype : Array

=cut

sub list_collection {
  my ($self, $collection, $recurse) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq ''
    and $self->logconfess('A non-empty collection argument is required');

  $collection = File::Spec->canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);

  my $recursively = $recurse ? 'recursively' : '';
  $self->debug("Listing collection '$collection' $recursively");

  return $self->lister->list_collection($collection, $recurse);
}

=head2 add_collection

  Arg [1]    : iRODS collection name

  Example    : $irods->add_collection('/my/path/foo')
  Description: Make a new collection in iRODS. Return the new collection.
  Returntype : Str

=cut

sub add_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq ''
    and $self->logconfess('A non-empty collection argument is required');

  $collection = File::Spec->canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);
  $self->debug("Adding collection '$collection'");

  WTSI::NPG::Runnable->new(executable  => $IMKDIR,
                           arguments   => ['-p', $collection],
                           environment => $self->environment)->run;
  return $collection;
}

=head2 put_collection

  Arg [1]    : Local directory name
  Arg [2]    : iRODS collection name

  Example    : $irods->put_collection('/my/path/foo', '/archive')
  Description: Make a new collection in iRODS. Return the new collection.
  Returntype : Str

=cut

sub put_collection {
  my ($self, $dir, $target) = @_;

  defined $dir or
    $self->logconfess('A defined directory argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $dir eq '' and
    $self->logconfess('A non-empty directory argument is required');
  $target eq '' and
    $self->logconfess('A non-empty target (object) argument is required');

  # iput does not accept trailing slashes on directories
  $dir = File::Spec->canonpath($dir);
  $target = File::Spec->canonpath($target);
  $target = $self->_ensure_absolute_path($target);
  $self->debug("Putting directory '$dir' into collection '$target'");

  my @args = ('-r', $dir, $target);
  WTSI::NPG::Runnable->new(executable  => $IPUT,
                           arguments   => \@args,
                           environment => $self->environment)->run;

  # FIXME - this is handling a case where the target collection exists
  return $target . '/' . basename($dir);
}

=head2 move_collection

  Arg [1]    : iRODS collection name
  Arg [2]    : iRODS collection name

  Example    : $irods->move_collection('/my/path/lorem.txt',
                                       '/my/path/ipsum.txt')
  Description: Move a collection.
  Returntype : Str

=cut

sub move_collection {
  my ($self, $source, $target) = @_;

  defined $source or
    $self->logconfess('A defined source (collection) argument is required');
  defined $target or
    $self->logconfess('A defined target (collection) argument is required');

  $source eq '' and
    $self->logconfess('A non-empty source (collection) argument is required');
  $target eq '' and
    $self->logconfess('A non-empty target (collection) argument is required');

  $source = File::Spec->canonpath($source);
  $source = $self->_ensure_absolute_path($source);
  $target = File::Spec->canonpath($target);
  $target = $self->_ensure_absolute_path($target);
  $self->debug("Moving collection from '$source' to '$target'");

  WTSI::NPG::Runnable->new(executable  => $IMV,
                           arguments   => [$source, $target],
                           environment => $self->environment)->run;
  return $target;
}

=head2 remove_collection

  Arg [1]    : iRODS collection name

  Example    : $irods->remove_collection('/my/path/foo')
  Description: Remove a collection and contents, recursively, and return
               self.
  Returntype : WTSI::NPG::iRODS

=cut

sub remove_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq '' and
    $self->logconfess('A non-empty collection argument is required');

  $collection = File::Spec->canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);
  $self->debug("Removing collection '$collection'");

  WTSI::NPG::Runnable->new(executable  => $IRM,
                           arguments   => ['-r', '-f', $collection],
                           environment => $self->environment)->run;
  return $collection;
}

sub get_collection_permissions {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq ''
    and $self->logconfess('A non-empty collection argument is required');

  return $self->lister->get_collection_acl($collection);
}

sub set_collection_permissions {
  my ($self, $level, $owner, $collection) = @_;

  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $owner eq '' and
    $self->logconfess('A non-empty owner argument is required');
  $collection eq '' and
    $self->logconfess('A non-empty collection argument is required');

  my $perm_str = defined $level ? $level : 'null';

  grep { $perm_str eq $_ } @VALID_PERMISSIONS or
    $self->logconfess("Invalid permission level '$perm_str'");

  $self->debug("Setting permissions on '$collection' to ",
               "'$perm_str' for '$owner'");

  my @acl = $self->get_collection_permissions($collection);

  if (any { $_->{owner} eq $owner and
            $_->{level} eq $perm_str } @acl) {
    $self->debug("'$collection' already has permission ",
                 "'$perm_str' for '$owner'");
  }
  else {
    $self->acl_modifier->chmod_collection($perm_str, $owner, $collection);
  }

  return $collection;
}

=head2 get_collection_meta

  Arg [1]    : iRODS data collection name

  Example    : $irods->get_collection_meta('/my/path/')
  Description: Get metadata on a collection as an array of AVUs
  Returntype : Array[HashRef]

=cut

sub get_collection_meta {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq '' and
    $self->logconfess('A non-empty collection argument is required');

  $collection = File::Spec->canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);

  return $self->meta_lister->list_collection_meta($collection);
}

=head2 add_collection_avu

  Arg [1]    : iRODS collection name
  Arg [2]    : attribute
  Arg [3]    : value
  Arg [4]    : units (optional)

  Example    : $irods->add_collection_avu('/my/path/foo', 'id', 'ABCD1234')
  Description: Add metadata to a collection. Return an array of
               the new attribute, value and units.
  Returntype : Array

=cut

sub add_collection_avu {
  my ($self, $collection, $attribute, $value, $units) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $collection eq '' and
    $self->logconfess('A non-empty collection argument is required');
  $attribute eq '' and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq '' and
    $self->logconfess('A non-empty value argument is required');

  my $units_str = defined $units ? "'$units'" : "'undef'";

  $collection = File::Spec->canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);

  $self->debug("Adding AVU ['$attribute', '$value', $units_str] ",
               "to '$collection'");

  my @current_meta = $self->get_collection_meta($collection);
  if ($self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logconfess("AVU ['$attribute', '$value', $units_str] ",
                      "already exists for '$collection'");
  }

  return $self->meta_adder->modify_collection_meta($collection, $attribute,
                                                   $value, $units);
}

=head2 remove_collection_avu

  Arg [1]    : iRODS collection name
  Arg [2]    : attribute
  Arg [3]    : value
  Arg [4]    : units (optional)

  Example    : $irods->remove_collection_avu('/my/path/foo', 'id', 'ABCD1234')
  Description: Removes metadata from a collection object. Return an array of
               the removed attribute, value and units.
  Returntype : Array

=cut

sub remove_collection_avu {
  my ($self, $collection, $attribute, $value, $units) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $collection eq '' and
    $self->logconfess('A non-empty collection argument is required');
  $attribute eq '' and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq '' and
    $self->logconfess('A non-empty value argument is required');

  my $units_str = defined $units ? "'$units'" : "'undef'";

  $collection = File::Spec->canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);

  $self->debug("Removing AVU ['$attribute', '$value', $units_str] ",
               "from '$collection'");

  my @current_meta = $self->get_collection_meta($collection);
  if (!$self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logcluck("AVU ['$attribute', '$value', $units_str] ",
                    "does not exist for '$collection'");
  }

  return $self->meta_remover->modify_collection_meta($collection, $attribute,
                                                     $value, $units);
}

=head2 find_collections_by_meta

  Arg [1]    : iRODS collection
  Arg [2]    : ArrayRef attribute value tuples

  Example    : $irods->find_collections_by_meta('/my/path/foo',
                                                ['id' => 'ABCD1234'])
  Description: Find collections by their metadata, restricted to a parent
               collection.
               Return a list of collections.
  Returntype : Array

=cut

sub find_collections_by_meta {
  my ($self, $root, @query_specs) = @_;

  defined $root or $self->logconfess('A defined root argument is required');
  $root eq '' and $self->logconfess('A non-empty root argument is required');

  $root = File::Spec->canonpath($root);
  $root = $self->_ensure_absolute_path($root);

  my $zone = $self->find_zone_name($root);
  my @query = $self->_make_imeta_query(@query_specs);

  my @args = ('-z', $zone, 'qu', '-C', @query);
  my @raw_results = WTSI::NPG::Runnable->new
    (executable  => $IMETA,
     arguments   => \@args,
     environment => $self->environment)->run->split_stdout;

  my @results;
  foreach my $row (@raw_results) {
    if ($row =~ m{^----$}) {
      next;
    }
    elsif ($row =~ m{^collection:\s(\S*)}) {
      my $coll = $1;
      push @results, $coll;
    }
  }

  # imeta doesn't permit filtering by path, natively.
  return grep { /^$root/ } @results;
}

=head2 list_object

  Arg [1]    : iRODS data object name

  Example    : $obj = $irods->list_object($object)
  Description: Return the full path of the object.
  Returntype : Str

=cut

sub list_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq '' and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->_ensure_absolute_path($object);
  $self->debug("Listing object '$object'");

  return $self->lister->list_object($object);
}

=head2 add_object

  Arg [1]    : Name of file to add to iRODs
  Arg [2]    : iRODS data object name

  Example    : $irods->add_object('lorem.txt', '/my/path/lorem.txt')
  Description: Add a file to iRODS.
  Returntype : Str

=cut

sub add_object {
  my ($self, $file, $target) = @_;

  defined $file or
    $self->logconfess('A defined file argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $file eq '' and
    $self->logconfess('A non-empty file argument is required');
  $target eq '' and
    $self->logconfess('A non-empty target (object) argument is required');

  $target = $self->_ensure_absolute_path($target);
  $self->debug("Adding '$file' as new object '$target'");

  WTSI::NPG::Runnable->new(executable  => $IPUT,
                           arguments   => [$file, $target],
                           environment => $self->environment)->run;
  return $target;
}

=head2 replace_object

  Arg [1]    : Name of file to add to iRODs
  Arg [2]    : iRODS data object name

  Example    : $irods->add_object('lorem.txt', '/my/path/lorem.txt')
  Description: Replace a file in iRODS.
  Returntype : Str

=cut

sub replace_object {
  my ($self, $file, $target) = @_;

  defined $file or
    $self->logconfess('A defined file argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $file eq '' and
    $self->logconfess('A non-empty file argument is required');
  $target eq '' and
    $self->logconfess('A non-empty target (object) argument is required');

  $target = $self->_ensure_absolute_path($target);
  $self->debug("Replacing object '$target' with '$file'");

  WTSI::NPG::Runnable->new(executable  => $IPUT,
                           arguments   => ['-f', $file, $target],
                           environment => $self->environment)->run;
  return $target;
}

=head2 move_object

  Arg [1]    : iRODS data object name
  Arg [2]    : iRODS data object name

  Example    : $irods->move_object('/my/path/lorem.txt', '/my/path/ipsum.txt')
  Description: Move a data object.
  Returntype : Str

=cut

sub move_object {
  my ($self, $source, $target) = @_;

  defined $source or
    $self->logconfess('A defined source (object) argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $source eq '' and
    $self->logconfess('A non-empty source (object) argument is required');
  $target eq '' and
    $self->logconfess('A non-empty target (object) argument is required');

  $source = $self->_ensure_absolute_path($source);
  $target = $self->_ensure_absolute_path($target);
  $self->debug("Moving object from '$source' to '$target'");

  WTSI::NPG::Runnable->new(executable  => $IMV,
                           arguments   => [$source, $target],
                           environment => $self->environment)->run;
  return $target
}

=head2 remove_object

  Arg [1]    : iRODS data object name

  Example    : $irods->remove_object('/my/path/lorem.txt')
  Description: Remove a data object.
  Returntype : Str

=cut

sub remove_object {
  my ($self, $target) = @_;

  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $target eq '' and
    $self->logconfess('A non-empty target (object) argument is required');

  $self->debug("Removing object '$target'");

  WTSI::NPG::Runnable->new(executable  => $IRM,
                           arguments   => [$target],
                           environment => $self->environment)->run;
  return $target;
}

sub slurp_object {
  my ($self, $target) = @_;

  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $target eq '' and
    $self->logconfess('A non-empty target (object) argument is required');

  $self->debug("Slurping object '$target'");

  my $iget = WTSI::NPG::Runnable->new(executable  => $IGET,
                                      arguments   => [$target, '-']);

  my $runnable = WTSI::NPG::Runnable->new
    (executable  => $IGET,
     arguments   => [$target, '-'])->run;

  my $copy = decode('UTF-8', ${$runnable->stdout}, Encode::FB_CROAK);

  return $copy;
}

sub get_object_permissions {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq '' and
    $self->logconfess('A non-empty object argument is required');

  return $self->lister->get_object_acl($object);
}

sub set_object_permissions {
  my ($self, $level, $owner, $object) = @_;

  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $object or
    $self->logconfess('A defined object argument is required');

  $owner eq '' and
    $self->logconfess('A non-empty owner argument is required');
  $object eq '' and
    $self->logconfess('A non-empty object argument is required');

  my $perm_str = defined $level ? $level : 'null';

  grep { $perm_str eq $_ } @VALID_PERMISSIONS or
    $self->logconfess("Invalid permission level '$perm_str'");

  $self->debug("Setting permissions on '$object' to '$perm_str' for '$owner'");
  my @acl = $self->get_object_permissions($object);

  if (any { $_->{owner} eq $owner and
            $_->{level} eq $perm_str } @acl) {
    $self->debug("'$object' already has permission '$perm_str' for '$owner'");
  }
  else {
    $self->acl_modifier->chmod_object($perm_str, $owner, $object);
  }

  return $object;
}

=head2 get_object_meta

  Arg [1]    : iRODS data object name

  Example    : $irods->get_object_meta('/my/path/lorem.txt')
  Description: Get metadata on a data object as an array of AVUs
  Returntype : Array[HashRef]

=cut

sub get_object_meta {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  $object eq '' and
    $self->logconfess('A non-empty object argument is required');

  return $self->meta_lister->list_object_meta($object);
}

=head2 add_object_avu

  Arg [1]    : iRODS data object name
  Arg [2]    : attribute
  Arg [3]    : value
  Arg [4]    : units (optional)

  Example    : add_object_avu('/my/path/lorem.txt', 'id', 'ABCD1234')
  Description: Add metadata to a data object. Return an array of
               the new attribute, value and units.
  Returntype : array

=cut

sub add_object_avu {
  my ($self, $object, $attribute, $value, $units) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $object eq '' and
    $self->logconfess('A non-empty object argument is required');
  $attribute eq '' and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq '' and
    $self->logconfess('A non-empty value argument is required');

  my $units_str = defined $units ? "'$units'" : "'undef'";

  $self->debug("Adding AVU {'$attribute', '$value', $units_str} to '$object'");

  my @current_meta = $self->get_object_meta($object);
  if ($self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logconfess("AVU {'$attribute', '$value', $units_str} ",
                      "already exists for '$object'");
  }

  return $self->meta_adder->modify_object_meta($object, $attribute,
                                               $value, $units);
}

=head2 remove_object_avu

  Arg [1]    : iRODS data object path
  Arg [2]    : attribute
  Arg [3]    : value
  Arg [4]    : units (optional)

  Example    : $irods->remove_object_avu('/my/path/lorem.txt', 'id',
               'ABCD1234')
  Description: Remove metadata from a data object. Return an array of
               the removed attribute, value and units.
  Returntype : Array

=cut

sub remove_object_avu {
  my ($self, $object, $attribute, $value, $units) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $object eq '' and
    $self->logconfess('A non-empty object argument is required');
  $attribute eq '' and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq '' and
    $self->logconfess('A non-empty value argument is required');

  my $units_str = defined $units ? "'$units'" : "'undef'";

  $self->debug("Removing AVU {'$attribute', '$value', $units_str} ",
               "from '$object'");

  my @current_meta = $self->get_object_meta($object);
  if (!$self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logconfess("AVU {'$attribute', '$value', $units_str} ",
                      "does not exist for '$object'");
  }

  return $self->meta_remover->modify_object_meta($object, $attribute,
                                                 $value, $units);
}

=head2 find_objects_by_meta

  Arg [1]    : iRODS collection
  Arg [2]    : ArrayRefs of attribute value tuples

  Example    : $irods->find_objects_by_meta('/my/path/foo',
                                            ['id' => 'ABCD1234'])
  Description: Find objects by their metadata, restricted to a parent
               collection.
               Return a list of collections.
  Returntype : Array

=cut

sub find_objects_by_meta {
  my ($self, $root, @query_specs) = @_;

  defined $root or $self->logconfess('A defined root argument is required');
  $root eq '' and $self->logconfess('A non-empty root argument is required');

  $root = File::Spec->canonpath($root);
  $root = $self->_ensure_absolute_path($root);

  my $zone = $self->find_zone_name($root);
  my @query = $self->_make_imeta_query(@query_specs);

  my @args = ('-z', $zone, 'qu', '-d', @query);
  my @raw_results = WTSI::NPG::Runnable->new
    (executable  => $IMETA,
     arguments   => \@args,
     environment => $self->environment)->run->split_stdout;

  my @results;
  my $coll;
  my $obj;
  foreach my $row (@raw_results) {
    if ($row =~ m{^----$}) {
      next;
    }
    elsif ($row =~ m{^collection:\s(\S*)}) {
      $coll = $1;
    }
    elsif ($row =~ m{^dataObj:\s(\S*)}) {
      $obj = $1;

      unless ($coll) {
        $self->logconfess("Failed to parse imeta output; missing collection ",
                          "in [", join(', ', @raw_results), "]");
      }

      push @results, "$coll/$obj";
      undef $coll;
      undef $row;
    }
  }

  # imeta doesn't permit filtering by path, natively.
  return grep { /^$root/ } @results;
}

=head2 calculate_checksum

  Arg [1]    : iRODS data object name

  Example    : $cs = $irods->calculate_checksum('/my/path/lorem.txt')
  Description: Return the MD5 checksum of an iRODS data object.
  Returntype : Str

=cut

sub calculate_checksum {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq '' and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->_ensure_absolute_path($object);

  my @raw_checksum = WTSI::NPG::Runnable->new
    (executable  => $ICHKSUM,
     arguments   => [$object],
     environment => $self->environment)->run->split_stdout;
  unless (@raw_checksum) {
    $self->logconfess("Failed to get iRODS checksum for '$object'");
  }

  my $checksum = shift @raw_checksum;
  $checksum =~ s/.*([0-9a-f]{32})$/$1/;

  return $checksum;
}

=head2 validate_checksum_metadata

  Arg [1]    : iRODS data object path

  Example    : $irods->validate_checksum_metadata('/my/path/lorem.txt')
  Description: Return true if the MD5 checksum in the metadata of an iRODS
               object is identical to the MD5 calculated by iRODS.
  Returntype : boolean

=cut

sub validate_checksum_metadata {
  my ($self, $object) = @_;

  my $identical = 0;
  my @md5 = grep { $_->{attribute} eq 'md5' } $self->get_object_meta($object);

  unless (@md5) {
    $self->logconfess("Failed to validate MD5 metadata for '$object' ",
                      "because it is missing");
  }

  if (scalar @md5 > 1) {
    $self->logconfess("Failed to validate MD5 metadata for '$object' ",
                      "because it has multiple values");
  }

  my $avu = $md5[0];
  my $irods_md5 = $self->calculate_checksum($object);
  my $md5 = $avu->{value};

  if ($md5 eq $irods_md5) {
    $self->debug("Confirmed '$object' MD5 as ", $md5);
    $identical = 1;
  }
  else {
    $self->debug("Expected MD5 of $irods_md5 but found $md5 for '$object'");
  }

  return $identical;
}

=head2 md5sum

  Arg [1]    : string path to a file

  Example    : my $md5 = md5sum($filename)
  Description: Calculate the MD5 checksum of a file.
  Returntype : Str

=cut

sub md5sum {
  my ($self, $file) = @_;

  defined $file or $self->logconfess('A defined file argument is required');
  $file eq '' and $self->logconfess('A non-empty file argument is required');

  my @result = WTSI::NPG::Runnable->new
    (executable  => $MD5SUM,
     arguments   => [$file],
     environment => $self->environment)->run->split_stdout;
  my $raw = shift @result;

  my ($md5) = $raw =~ m{^(\S+)\s+.*}msx;

  return $md5;
}

=head2 hash_path

  Arg [1]    : string path to a file
  Arg [2]    : MD5 checksum (optional)

  Example    : my $path = $irods->hash_path($filename)
  Description: Return a hashed path 3 directories deep, each level having
               a maximum of 256 subdirectories, calculated from the file's
               MD5. If the optional MD5 argument is supplied, the MD5
               calculation is skipped and the provided value is used instead.
  Returntype : Str

=cut

sub hash_path {
  my ($self, $file, $md5sum) = @_;

  $md5sum ||= $self->md5sum($file);
  unless ($md5sum) {
    $self->logconfess("Failed to caculate an MD5 for $file");
  }

  my @levels = $md5sum =~ m{\G(..)}gmsx;

  return join('/', @levels[0..2]);
}

sub _ensure_absolute_path {
  my ($self, $target) = @_;

  my $absolute = $target;
  unless ($target =~ m{^/}) {
    $absolute = $self->working_collection . '/' . $absolute;
  }

  return $absolute;
}

sub _meta_exists {
  my ($self, $attribute, $value, $units, $current_meta) = @_;

  ref $current_meta eq 'ARRAY' or
    $self->logconfess("current_meta argument must be an ArrayRef");

  if ($units) {
    return grep { $_->{attribute} eq $attribute &&
                  $_->{value}     eq $value &&
                  $_->{units}     eq $units } @$current_meta;
  }
  else {
    return grep { $_->{attribute} eq $attribute &&
                  $_->{value}     eq $value} @$current_meta;
  }
}

sub _make_imeta_query {
  my ($self, @query_specs) = @_;

  scalar @query_specs or
    $self->logconfess('At least one query_spec argument is required');

  my @query_clauses;
  my $i = 0;
  foreach my $spec (@query_specs) {
    unless (ref $spec eq 'ARRAY') {
      $self->logconfess("The query_spec '$spec' was not an array reference");
    }

    my ($attribute, $value, $operator) = @$spec;
    if (defined $operator) {
      unless ($operator eq '='    ||
              $operator eq 'like' ||
              $operator eq '<'    ||
              $operator eq '>') {
        $self->logconfess("Invalid query operator '$operator' in query spec ",
                         "[$attribute, $value, $operator]");
      }
    }
    else {
      $operator = '=';
    }

    unless (defined $value) {
      $self->logconfess("Invalid query value 'undef' in query spec ",
                        "[$attribute, undef, $operator]");
    }
    if ($value eq '') {
      $self->logconfess("Invalid query value '$value' in query spec ",
                        "[$attribute, $value, $operator]");
    }

    if ($i > 0) {
      push(@query_clauses, 'and')
    }
    $i++;

    push(@query_clauses, $attribute, $operator, $value);
  }

  return @query_clauses;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS

=head1 SYNOPSIS

  my $irods = WTSI::NPG::iRODS->new;
  my $rods_path = $irods->add_object("file.txt", "irods/path");
  print $irods->list_object($rods_path), "\n";

  $irods->add_object_avu(rods_path, 'a', 'b', 'c');
  $irods->add_object_avu(rods_path, 'x', 'y1', 'z');
  $irods->add_object_avu(rods_path, 'x', 'y2', 'z');

  my @objs = $irods->find_objects_by_meta('/',
                                          [a => 'b'],
                                          [y => 'z%', 'like']);

=head1 DESCRIPTION

This class provides access to iRODS operations on data objects,
collections and metadata. It does so by launching several client
programs in the background, each of which holds open a connection

On creation, an instance captures a copy of %ENV which it uses for all
its child processes.

iRODS paths to data objects and collections are represented as
strings. AVUs are represented as HashRefs of the form

  { attribute => <attribute name>,
    value     => <attribute value>,
    units     => <attribute units> }

Units are optional.

Query clauses are represented as ArrayRefs of the form

  [ <attribute name>, <attribute value> <operator> ]

The operator is optional, defaulting to '='. Valid operators are '=',
'like', '<' and '>'.

e.g. The query

  [x => 'a'], [y => 'b'], [z => 'c%', 'like']

is translated to the iRODS imeta query

  x = a and y = b and x like c%

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2013 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
