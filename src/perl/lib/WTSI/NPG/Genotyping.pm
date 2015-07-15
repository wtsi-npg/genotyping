use utf8;

package WTSI::NPG::Genotyping;

use warnings;
use strict;
use Carp;
use Cwd qw(abs_path getcwd);
use File::Spec;
use JSON;

use base 'Exporter';
our @EXPORT_OK = qw(base_dir
                    config_dir
                    read_sample_json
                    read_snp_json);

our $VERSION = '';

=head2 base_dir

  Arg [1]    : None.

  Example    : my $base = base_dir();
  Description: Return the installed base directory.
  Returntype : string

=cut

sub base_dir {
  my ($vol, $dirs, $file) =
    File::Spec->splitpath($INC{"WTSI/NPG/Genotyping.pm"});

  my ($base) = $dirs =~ m{^(.+)\blib}msx;

  unless (defined $base) {
    my $cwd = getcwd();
    warn "Failed to parse installed base directory from '$dirs', using $cwd\n";
    $base = $cwd;;
  }

  return abs_path($base);
}

=head2 config_dir

  Arg [1]    : None.

  Example    : my $dir = config_dir);
  Description: Return the installed configuration file directory.
  Returntype : string

=cut

sub config_dir {
  my $base = base_dir();
  return abs_path(File::Spec->catdir($base, 'etc'));
}


=head2 read_sample_json

  Arg [1]    : filename

  Example    : @samples = read_sample_json($file)
  Description: Return sample metadata hashes, one per sample, from a JSON file.
  Returntype : array

=cut

sub read_sample_json {
  my ($file) = @_;

  open(my $fh, '<', "$file")
    or confess "Failed to open JSON file '$file' for reading: $!\n";
  my $str = do { local $/ = undef; <$fh> };
  close($fh) or warn "Failed to close JSON file '$file'\n";

  return @{from_json($str, {utf8 => 1})};
}

1;

=head2 read_snp_json

  Arg [1]    : filename

  Example    : @snps = read_snp_json($file)
  Description: Return SNP metadata hashes, one per SNP, from a JSON file.
  Returntype : array

=cut

sub read_snp_json {
  my ($file) = @_;

  open(my $fh, '<', "$file")
    or confess "Failed to open JSON file '$file' for reading: $!\n";
  my $str = do { local $/ = undef; <$fh> };
  close($fh) or warn "Failed to close JSON file '$file'\n";

  return @{from_json($str, {utf8 => 1})};
}

1;

__END__

=head1 NAME

WTSI::NPG::Genotyping

=head1 DESCRIPTION

General purpose utilities that may be used by genotyping projects.
See individual POD for details.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2012, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
