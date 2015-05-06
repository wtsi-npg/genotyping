use utf8;

package WTSI::NPG::Genotyping::Database::Pipeline;

use Carp;
use Moose;

use WTSI::NPG::Genotyping;
use WTSI::NPG::Genotyping::Database::Pipeline::Schema;

extends 'WTSI::NPG::Database';

with 'WTSI::NPG::Database::DBIx';

has 'config_dir' =>
  (is      => 'ro',
   isa     => 'Str',
   default => sub { return WTSI::NPG::Genotyping::config_dir() },
   required => 1);

has 'dbfile' =>
  (is        => 'rw',
   isa       => 'Str',
   required  => 1,
   lazy      => 1,
   default   => sub {
     my ($self) = @_;

     my $ds = $self->data_source;
     my ($base, $file) = $ds =~ m/^(dbi:SQLite:dbname=)(.*)/;
     unless ($base && $file) {
       $self->logconfess("Failed to parse datasource string '$ds' in ",
                         $self->inifile);
     }

     return  $file;
   });

has 'overwrite' =>
  (is       => 'ro',
   isa      => 'Bool',
   required => 1,
   default  => 0);

has 'schema' =>
  (is       => 'rw',
   isa      => 'WTSI::NPG::Genotyping::Database::Pipeline::Schema',
   required => 0);

our $AUTOLOAD;

our $default_sqlite   = 'sqlite3';
our $default_ddl_file = 'pipeline_ddl.sql';

our $pipeline_ini  = 'pipeline.ini';
our $genders_ini   = 'genders.ini';
our $methods_ini   = 'methods.ini';
our $relations_ini = 'relations.ini';
our $snpsets_ini   = 'snpsets.ini';
our $states_ini    = 'states.ini';

sub BUILD {
  my ($self) = @_;

  my $ds = $self->data_source;
  my ($base, $file) = $ds =~ m/^(dbi:SQLite:dbname=)(.*)/;
  unless ($base && $file) {
    $self->logconfess("Failed to parse datasource string '$ds' in ",
                      $self->inifile);
  }

  $self->data_source($base . $self->dbfile);

  if (-e $self->dbfile) {
    if ($self->overwrite) {
      unlink($self->dbfile);
      $self->create($self->dbfile, $self->ini);
    }
  }
  else {
    $self->create($self->dbfile, $self->ini);
  }

  $self->username(getpwent());

  return $self;
}

=head2 create

  Arg [1]    : Database file
  Arg [2]    : Config::IniFiles ini file

  Example    : $db->create($file, $ini)
  Description: Writes the SQLite database file. This is called automatically
               by the constructor, but may be called at any time to re-write
               the database file.
  Returntype : WTSI::NPG::Genotyping::Database::Pipeline

=cut

sub create {
  my ($self, $file, $ini) = @_;

  my $config_dir = $self->config_dir;
  my $default_sql_path = "$config_dir/$default_ddl_file";

  my $sql_path = $ini->val($self->name, 'sqlpath', $default_sql_path);
  my $sqlite = $ini->val($self->name, 'sqlite', $default_sqlite);

  unless (-e $sql_path) {
    $self->logconfess("Failed to create database: ",
                      "DDL file '$sql_path' is missing");
  }

  if (-e $file) {
    $self->logconfess("Failed to create database: ",
                      "database '$file' already exists");
  }
  else {
    system("$sqlite $file < $sql_path") == 0
      or $self->logconfess("Failed to create SQLite database '$file': $?");
  }

  return $self;
}

=head2 populate

  Arg [1]    : None

  Example    : $db->populate
  Description: Populates the dictionary tables of a database from the
               default .ini files located in the 'inipath' path given
               in the database configuration (See
               WTSI::NPG::Genotyping::Database::configure). May be called
               safely multiple times on the same .ini data.
  Returntype : WTSI::NPG::Genotyping::Database::Pipeline

=cut

sub populate {
  my $self = shift;

  my $default_ini_path = $self->config_dir;
  my $ini_path = $self->ini->val($self->name, 'inipath', $default_ini_path);

  unless ($self->is_connected) {
    $self->logconfess('Failed to populate database: not connected');
  }

  $self->_populate_addresses;
  $self->_populate_genders("$ini_path/$genders_ini");
  $self->_populate_relations("$ini_path/$relations_ini");
  $self->_populate_states("$ini_path/$states_ini");
  $self->_populate_methods("$ini_path/$methods_ini");
  $self->_populate_snpsets("$ini_path/$snpsets_ini");

  return $self;
}

sub connect {
  my ($self, %args) = @_;
  $self->schema(WTSI::NPG::Genotyping::Database::Pipeline::Schema->connect
                ($self->data_source,
                 $self->username,
                 $self->password,
                 \%args));
  return $self;
}

=head2 snpset_names_for_method

Args [1]     : Name of genotying method (eg. Infinium, Sequenom, Fluidigm)
               Must have an entry in the method table of the pipeline DB

Description  : Return the snpset name(s), if any, which have snp_results
               for the given genotyping method.
Returntype   : Arrayref

=cut

sub snpset_names_for_method {
    my ($self, $method) = @_;

    # check that method argument has an entry in the DB method table
    my $method_entry = $self->method->find({name => $method});
    unless ($method_entry) {
        $self->logcroak("Method '", $method,
                        "' is not defined in database methods table");
    }

    # get snpsets for method
    my @results = $self->schema->resultset('Snpset')->search(
        { 'method.name' => $method },
        { join => { snps => { snp_results => { result => 'method' }}},
          distinct => 1,
        }
    );
    my @names;
    foreach my $result (@results) { push @names, $result->name(); }
    return \@names;
}


=head2 total_results_for_method

Args [1]     : Name of genotying method (eg. Infinium, Sequenom, Fluidigm)
               Must have an entry in the method table of the pipeline DB

Count the number of results for the given genotyping method.

=cut

sub total_results_for_method {

    my ($self, $method) = @_;

    # check that method argument has an entry in the DB method table
    my $method_entry = $self->method->find({name => $method});
    unless ($method_entry) {
        $self->logcroak("Method '", $method,
                        "' is not defined in database methods table");
    }
    my $total = $self->schema->resultset('Result')->search(
        {'method.name' => $method}, {join => 'method'})->count;
    return $total;
}

=head2 address

Returns a DBIx::Class::ResultSet for the registered source 'address'.

=cut

=head2 dataset

Returns a DBIx::Class::ResultSet for the registered source 'dataset'.

=cut


=head2 datasupplier

Returns a DBIx::Class::ResultSet for the registered source 'datasupplier'.

=cut


=head2 gender

Returns a DBIx::Class::ResultSet for the registered source 'gender'.

=cut


=head2 method

Returns a DBIx::Class::ResultSet for the registered source 'method'.

=cut


=head2 piperun

Returns a DBIx::Class::ResultSet for the registered source 'piperun'.

=cut


=head2 plate

Returns a DBIx::Class::ResultSet for the registered source 'plate'.

=cut


=head2 result

Returns a DBIx::Class::ResultSet for the registered source 'result'.

=cut


=head2 snp

Returns a DBIx::Class::ResultSet for the registered source 'snp'.

=cut


=head2 snpset

Returns a DBIx::Class::ResultSet for the registered source 'snpset'.

=cut


=head2 state

Returns a DBIx::Class::ResultSet for the registered source 'state'.

=cut


=head2 well

Returns a DBIx::Class::ResultSet for the registered source 'well'.

=cut

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

  return $self;
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

  $self->debug("Loading list of '$class' from INI file '$inifile' '$param'");

  my @objects;
  my $ini = Config::IniFiles->new(-file => $inifile);
  foreach my $sect ($ini->Sections) {
    foreach my $elt ($ini->val($sect, $param)) {
      push @objects,
        $self->schema->resultset($class)->find_or_create({$param => $elt});
    }
  }

  $self->debug("Loaded a list of ", scalar @objects,
               " instances of '$class' from INI file '$inifile' '$param'");

  return \@objects;
}

sub _insert_from_ini {
  my ($self, $class, $inifile) = @_;

  $self->debug("Loading '$class' from INI file '$inifile'");

  my @objects;
  my $ini = Config::IniFiles->new(-file => $inifile);
  foreach my $sect ($ini->Sections) {
    my %args;
    foreach my $param ($ini->Parameters($sect)) {
      $args{$param} = $ini->val($sect, $param);
    }

    push @objects, $self->schema->resultset($class)->find_or_create(\%args);
  }

  $self->debug("Loaded ", scalar @objects,
               " instances of '$class' from INI file '$inifile'");

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
  my ($self) = @_;
  my $type = ref($self) or confess "$self is not an object";

  return if $AUTOLOAD =~ /::DESTROY$/;

  #if (!$self->is_connected) {
  #  $self->connect;
  #}

  if (!$self->is_connected) {
    $self->logconfess("$self is not connected");
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
    $self->logconfess("An invalid method `$method_name' was called ",
                      "on an object of $type. Permitted methods are [",
                      join(", ", sort keys %lookup), "]");
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

__END__

=head1 NAME

WTSI::NPG::Genotyping::Database::Pipeline

=head1 DESCRIPTION

A class for accessing the genotyping pipeline analysis database.

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
