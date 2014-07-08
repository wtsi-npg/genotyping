use utf8;

package WTSI::NPG::Genotyping::Schema::Result::Sample;

use strict;
use warnings;
use Carp;
use URI;

use base 'DBIx::Class::Core';

__PACKAGE__->table('sample');
__PACKAGE__->add_columns
  ('id_sample',        { data_type => 'integer',
                        is_auto_increment => 1,
                        is_nullable => 0 },
   'name',             { data_type => 'text',
                         is_nullable => 0 },
   'sanger_sample_id', { data_type => 'text',
                         is_nullable => 1 },
   'beadchip',         { data_type => 'text',
                         is_nullable => 0 },
   'id_dataset',       { data_type => 'integer',
                         is_foreign_key => 1,
                         is_nullable => 0 },
   'supplier_name',    { data_type => 'text',
                         is_nullable => 1 },
   'cohort',           { data_type => 'text',
                         is_nullable => 1 },
   'rowcol',           { data_type => 'text',
                         is_nullable => 1 },
   'include',          { data_type => 'integer',
                         is_nullable => 0 });

__PACKAGE__->set_primary_key('id_sample');
__PACKAGE__->add_unique_constraint(['name']);


__PACKAGE__->belongs_to('dataset',
                        'WTSI::NPG::Genotyping::Schema::Result::Dataset',
                        { 'foreign.id_dataset' => 'self.id_dataset' });

__PACKAGE__->has_many('wells',
                      'WTSI::NPG::Genotyping::Schema::Result::Well',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->has_many('results', 'WTSI::NPG::Genotyping::Schema::Result::Result',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->has_many('sample_genders',
                      'WTSI::NPG::Genotyping::Schema::Result::SampleGender',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->has_many('sample_states',
                      'WTSI::NPG::Genotyping::Schema::Result::SampleState',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->many_to_many('genders' => 'sample_genders', 'gender');

__PACKAGE__->many_to_many('states' => 'sample_states', 'state');

__PACKAGE__->has_many('related_samples',
                      'WTSI::NPG::Genotyping::Schema::Result::RelatedSample',
                      { 'foreign.id_sample_a' => 'self.id_sample' });

__PACKAGE__->many_to_many('related' => 'related_samples', 'sample_b');


sub update {
  my ($self, @args) = @_;

  unless ($self->validate_states) {
    croak "Sample '", $self->name, " has invalid states: [" .
      join(", ", sort map { $_->name } $self->states) . "]";
  }
  $self->next::method(@args);

  return $self;
}

=head2 validate_states

  Arg [1]    : None
  Example    : $sample->validate_states
  Description: Return true if the sample states are valid. Checks for
               criteria that are not constrained by the relational schema.
  Returntype : boolean, true if sample has valid states.

=cut

sub validate_states {
  my ($self) = @_;

  my @states = $self->states;
  my $valid = 1;
  if (scalar @states > 1) {
    if ((grep { $_->name eq 'pi_excluded' } @states) &&
        (grep { $_->name eq 'pi_approved' } @states)) {
      $valid = 0;
    }

    if ((grep { $_->name eq 'autocall_pass' } @states) &&
        (grep { $_->name eq 'autocall_fail' } @states)) {
      $valid = 0;
    }
  }

  return $valid;
}

=head2 include_from_state

  Arg [1]    : None
  Example    : $sample->include_from_state
  Description: Modifies $self based on its state to indicate whether or not
               it is to be included in analysis.
  Returntype : boolean, true if sample is included in analysis.

=cut

sub include_from_state {
  my ($self) = @_;

  unless ($self->validate_states) {
    croak "Sample '", $self->name, " has invalid states: [" .
      join(", ", sort map { $_->name } $self->states) . "]";
  }

  my @states = $self->states;

  # Default is to exclude
  $self->include(0);

  # An autocall_pass flips the sample to included.
  if    (grep { $_->name eq 'autocall_pass' }  @states) { $self->include(1) }
  elsif (grep { $_->name eq 'autocall_fail' }  @states) { $self->include(0) };

  # An explicit state of excluded flips the sample to excluded, even
  # if autocall_pass.
  if (grep { $_->name eq 'excluded' }          @states) { $self->include(0) };

  # If the sample has been superseded, it should be excluded.
  if (grep { $_->name eq 'repicked' }          @states) { $self->include(0) };

  # pi_approved / excluded overrides any of the above.
  if    (grep { $_->name eq 'pi_approved' }    @states) { $self->include(1) }
  elsif (grep { $_->name eq 'pi_excluded' }    @states) { $self->include(0) };

  # Consent withdrawn overrides everything above.
  if (grep { $_->name eq 'consent_withdrawn' } @states) { $self->include(0) };
  # Also, if the data are unavailable, we cannot analyse.
  if (grep { $_->name eq 'gtc_unavailable' }   @states) { $self->include(0) };

  return $self->include;
}

=head2 uri

  Arg [1]    : None
  Example    : $sample->uri
  Description: Returns a URI for the sample.
  Returntype : URI object.

=cut

sub uri {
  my ($self) = @_;

  my $nid = $self->dataset->datasupplier->namespace;
  my $nss = $self->name;
  my $uri = URI->new("urn:$nid:$nss", 'URN');

  return $uri->canonical;
}

=head2 gtc

  Arg [1]    : None
  Example    : $sample->gtc
  Description: Returns the path of the sample GTC file.
  Returntype : string file path

=cut

sub gtc {
  my ($self) = @_;

  my $file;
  my $result = $self->results->find({'method.name' =>'Autocall'},
                                    {join          => 'method'});

  if ($result && $result->value) {
    $file = $result->value;

    # Munge the windows path into the correspoding NFS mount
    $file = _map_netapp_path($file);
  }

  return $file;
}

=head2 idat

  Arg [1]    : string channel, 'red' or 'green'
  Example    : $sample->gtc
  Description: Returns the path of the IDAT file for one of the two channels
  Returntype : string file path

=cut

sub idat {
  my ($self, $channel) = @_;

  $channel or confess('A channel argument is required');
  unless ($channel =~ m{^red|green$}msx) {
    confess "Invalid channel argument '$channel' must be one of [red, green]";
  }

  my @result = $self->results->search({'method.name' =>'Infinium'},
                                      {join          => 'method'});
  my @values = map { $_->value } @result;

  my @files;
  if ($channel eq 'red') {
    @files = grep { defined $_ and m{red}msx } @values;
  } else {
    @files = grep { defined $_ and m{grn}msx } @values;
  }

  my $file = shift @files;

  # Horrible, fragile munging because the Infinium LIMS doesn't store
  # the correct path case and the result is then exposed as an NFS mount.
  if ($file) {
    $file = _map_netapp_path($file);
    $file =~ s{_r(\d+)c(\d+)_}{_R$1C$2_}msx;
    $file =~ s{grn}{Grn}msx;
    $file =~ s{red}{Red}msx;
  }

  return $file;
}

sub _map_netapp_path {
  my ($netapp_path) = @_;

  $netapp_path =~ s{\\}{/}gmsx;
  $netapp_path =~ s{//}{/}msx;
  $netapp_path =~ s{netapp\d[ab]/illumina_geno(\d)}{nfs/new_illumina_geno0$1}msx;

  return $netapp_path;
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
