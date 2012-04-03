
package WTSI::Genotyping::Database::Pipeline;

use strict;
use warnings;
use Carp;

use WTSI::Genotyping::Database;
use WTSI::Genotyping::Schema;

our @ISA = qw(WTSI::Genotyping::Database);
our $AUTOLOAD;

our $genders_ini = 'genders.ini';
our $methods_ini = 'methods.ini';
our $relations_ini = 'relations.ini';
our $snpsets_ini = 'snpsets.ini';
our $states_ini = 'states.ini';

sub populate {
  my $self = shift;

  my $ini = Config::IniFiles->new(-file => $self->inifile);
  my $ini_path = $ini->val($self->name, 'inipath');

  $self->_populate_addresses;
  $self->_populate_genders("$ini_path/$genders_ini");
  $self->_populate_relations("$ini_path/$relations_ini");
  $self->_populate_states("$ini_path/$states_ini");
  $self->_populate_methods("$ini_path/$methods_ini");
  $self->_populate_snpsets("$ini_path/$snpsets_ini");

  return $self;
}


=head2 connect

  See WTSI::Genotyping::Database.

=cut

sub connect {
  my $self = shift;
  my %args = @_;

  unless ($self->is_connected) {
    $self->{_schema} = WTSI::Genotyping::Schema->connect($self->data_source,
                                                         $self->username,
                                                         $self->password,
                                                         \%args);
  }

  return $self;
}

sub schema {
 my $self = shift;
 return $self->{_schema};
}


=head2 dbh

  See WTSI::Genotyping::Database.

=cut

sub dbh {
  my $self = shift;
  if ($self->schema) {
    return $self->{_schema}->storage->dbh;
  }
}


=head2 disconnect

  See WTSI::Genotyping::Database.

=cut

sub disconnect {
  my $self = shift;
  $self->schema->disconnect;
}

# Populates plate well addresses dictionary with the two styles of
# address label used.
sub _populate_addresses {
  my ($self) = @_;

  foreach my $row (1..16) {
    foreach my $col (1..24) {
      my $label1 = sprintf("%c%02d", 64+ $row, $col);
      my $label2 = sprintf("%c%d", 64+ $row, $col);
      $self->address->find_or_create({label1 => $label1,
                                      label2 => $label2});
    }
  }
}

# Populates snpsets dictionary (Infinium and Sequenom SNP sets).
sub _populate_snpsets {
  my ($self, $inifile) = @_;
  return $self->_insert_list_from_ini('Snpset', $inifile, 'name');
}

# Populates the gender dictionary.
sub _populate_genders {
  my ($self, $inifile) = @_;
  return $self->_insert_from_ini('Gender', $inifile);
}

# Populates the analysis method dictionary.
sub _populate_methods {
  my ($self, $inifile) = @_;
  return $self->_insert_from_ini('Method', $inifile);
}

# Populates the sample-sample relation method dictionary.
sub _populate_relations {
  my ($self, $inifile) = @_;
  return $self->_insert_from_ini('Relation', $inifile);
}

# Populates the sample state dictionary.
sub _populate_states {
  my ($self, $inifile) = @_;
  return $self->_insert_from_ini('State', $inifile);
}

sub _insert_list_from_ini {
  my ($self, $class, $inifile, $param) = @_;

  my @objects;
  my $ini = Config::IniFiles->new(-file => $inifile);
  foreach my $sect ($ini->Sections) {
    foreach my $elt ($ini->val($sect, $param)) {
      push @objects,
        $self->schema->resultset($class)->find_or_create({$param => $elt});
    }
  }

  return \@objects;
}

sub _insert_from_ini {
  my ($self, $class, $inifile) = @_;

  my @objects;
  my $ini = Config::IniFiles->new(-file => $inifile);
  foreach my $sect ($ini->Sections) {
    my %args;
    foreach my $param ($ini->Parameters($sect)) {
      $args{$param} = $ini->val($sect, $param);
    }

    push @objects, $self->schema->resultset($class)->find_or_create(\%args);
  }

  return \@objects;
}

# Autoloads methods corresponding to the Schema->sources. By default,
# you can do this if you have any Result instance. E.g.
#
# my @samples = $dataset->samples;
#
# However, to obtain a Result instance if you have none, you must do
# something like this. E.g.
#
# my @samples = $db->schema->resultset('Samples')->all;
#
# This autoload permits shortcut methods on the database object for cases
# where there is no Result instance handy. E.g.
#
# my @samples = $db->sample->all;
#
sub AUTOLOAD {
  my $self = shift;
  my $type = ref($self) or croak "$self is not an object\n";

  return if $AUTOLOAD =~ /::DESTROY$/;

  unless ($self->is_connected) {
    croak "$self is not connected\n";
  }

  my $schema = $self->schema;
  my %lookup;
  foreach my $source_name ($schema->sources) {
    my $name = $schema->source($source_name)->name;
    $lookup{$name} = $source_name;
  }

  my $method_name = $AUTOLOAD;
  $method_name =~ s/.*://;
  unless (exists $lookup{$method_name} ) {
    croak "An invalid method `$method_name' was called " .
      "on an object of $type. Permitted methods are [" .
        join(", ", sort keys %lookup) . "]\n";
  }

 SYMBOL_TABLE: {
    no strict qw(refs);

    *$AUTOLOAD = sub {
      my $self = shift;
      return $self->schema->resultset($lookup{$method_name});
    };
  }

  unshift @_, $self;
  goto &$AUTOLOAD;
}

1;
