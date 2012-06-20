use utf8;

package WTSI::Genotyping::Schema::Result::Sample;

use strict;
use warnings;

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
   'include',          { data_type => 'integer',
                         is_nullable => 0 });

__PACKAGE__->set_primary_key('id_sample');
__PACKAGE__->add_unique_constraint(['name']);


__PACKAGE__->belongs_to('dataset',
                        'WTSI::Genotyping::Schema::Result::Dataset',
                        { 'foreign.id_dataset' => 'self.id_dataset' });

__PACKAGE__->has_many('wells',
                      'WTSI::Genotyping::Schema::Result::Well',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->has_many('results', 'WTSI::Genotyping::Schema::Result::Result',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->has_many('sample_genders',
                      'WTSI::Genotyping::Schema::Result::SampleGender',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->has_many('sample_states',
                      'WTSI::Genotyping::Schema::Result::SampleState',
                      { 'foreign.id_sample' => 'self.id_sample' });

__PACKAGE__->many_to_many('genders' => 'sample_genders', 'gender');

__PACKAGE__->many_to_many('states' => 'sample_states', 'state');

__PACKAGE__->has_many('related_samples',
                      'WTSI::Genotyping::Schema::Result::RelatedSample',
                      { 'foreign.id_sample_a' => 'self.id_sample' });

__PACKAGE__->many_to_many('related' => 'related_samples', 'sample_b');

sub uri {
  my $self = shift;

  my $nid = $self->dataset->datasupplier->namespace;
  my $nss = $self->name;
  my $uri = URI->new("urn:$nid:$nss", 'URN');

  return $uri->canonical;
}

sub gtc {
  my $self = shift;
  my $method = shift;

  my $file;
  my $result = $self->results->find({'method.name' =>'Infinium'},
                                    {join => 'method'});

  if ($result && $result->value) {
    # Munge the windows path into the correspoding NFS mount
    $file = $result->value;
    $file =~ s|\\|/|g;
    $file =~ s|//|/|;
    $file =~ s|netapp6[ab]/illumina|nfs/new_illumina|;
    $file =~ s|geno(\d)|geno0$1|;
  }

  return $file;
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
