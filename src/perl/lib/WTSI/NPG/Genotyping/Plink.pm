use utf8;

package WTSI::NPG::Genotyping::Plink;

use strict;
use warnings;
use Carp;
use File::Basename;
use File::Copy;

use WTSI::NPG::Genotyping qw(read_snp_json
                             read_sample_json);

use base 'Exporter';
our @EXPORT_OK = qw(update_placeholder 
                    update_snp_locations
                    update_sample_genders);

sub update_placeholder {
    my ($input, $output, $placeholder, $tmp_dir) = @_;
    my ($in_base, $in_path, $in_suffix) = fileparse($input, '.bed');
    my $in_fam = $in_path . '/' . $in_base . '.fam';
    my $tmp_fam = $tmp_dir . $in_base . '.fam';
    open(my $in, '<', $in_fam) or confess "Failed to open '$in_fam': $!\n";
    open(my $out, '>', $tmp_fam)  or confess "Failed to open '$tmp_fam': $!\n";
    my $num_updated = _update_placeholder($in, $out, $placeholder);
    close($in) or warn "Failed to close $in\n" ;
    close($out) or warn "Failed to close $out\n";
    my ($out_base, $out_path, $out_suffix) = fileparse($output, '.bed');
    my $out_fam = $out_path . '/' . $out_base . '.fam';
    move($tmp_fam, $out_fam) or
	confess "Failed to move $tmp_fam to $out_fam: $!\n";

    return $num_updated;
}

sub update_snp_locations {
  my ($input, $output, $snps, $tmp_dir) = @_;

  # SNP information
  my @snps = read_snp_json($snps);

  my ($in_base, $in_path, $in_suffix) = fileparse($input, '.bed');
  my $in_bim = $in_path . '/' . $in_base . '.bim';
  my $tmp_bim = $tmp_dir . $in_base . '.bim';
  open(my $in, '<', $in_bim) or confess "Failed to open '$in_bim': $!\n";
  open(my $out, '>', $tmp_bim)  or confess "Failed to open '$tmp_bim': $!\n";

  my %locations;
  foreach my $snp (@snps) {
    $locations{$snp->{name}} = [$snp->{chromosome}, $snp->{position}];
  }
  my $num_updated = _update_snp_locations($in, $out, \%locations);

  close($in) or warn "Failed to close $in\n" ;
  close($out) or warn "Failed to close $out\n";

  my ($out_base, $out_path, $out_suffix) = fileparse($output, '.bed');
  my $out_bim = $out_path . '/' . $out_base . '.bim';

  move($tmp_bim, $out_bim) or
    confess "Failed to move $tmp_bim to $out_bim: $!\n";

  return $num_updated;
}

sub update_sample_genders {
  my ($input, $output, $samples, $tmp_dir) = @_;

  my @samples = read_sample_json($samples);

  my ($base, $path, $suffix) = fileparse($input, '.bed');
  my $fam_file = $path . '/' . $base . '.fam';
  my $tmp_fam = $tmp_dir . $base . '.fam';
  open(my $in, '<', $fam_file) or confess "Failed to open '$fam_file': $!\n";
  open(my $out, '>', $tmp_fam)  or confess "Failed to open '$tmp_fam': $!\n";

  my %genders;
  foreach my $sample (@samples) {
    $genders{$sample->{uri}} = $sample->{gender_code};
  }

  my $num_updated = _update_sample_genders($in, $out, \%genders);

  close($in) or warn "Failed to close $in\n";
  close($out) or warn "Failed to close $out\n";

  my ($out_base, $out_path, $out_suffix) = fileparse($output, '.bed');
  my $out_fam = $out_path . '/' . $out_base . '.fam';

  move($tmp_fam, $out_fam) or
    confess "Failed to move $tmp_fam to $out_fam: $!\n";

  return $num_updated;
}

=head2 _update_placeholder

  Arg [1]    : filehandle
  Arg [2]    : filehandle
  Arg [3]    : string containing a placeholder value for missing data in
               .fam files (typically 0 or -9)
  Example    : $n = update_placeholder(\*STDIN, \*STDOUT, 0)
  Description: Update a stream of Plink FAM format records with new placeholder
               value for missing data
  Returntype : integer, number of records processed
  Caller     : general

=cut

sub _update_placeholder {
    my ($in, $out, $placeholder) = @_;
    my $n = 0;
    while (my $line = <$in>) {
	chomp($line);
	my @fields = split /\s+/, $line;
	my $updated = 0;
	for (my $i=2;$i<@fields;$i++) {
	    if ($fields[$i] eq '0' || $fields[$i] eq '-9') { 
		$fields[$i] = $placeholder;
		$updated = 1;
	    }
	}
	print $out join("\t", @fields)."\n";
	if ($updated) { $n++; }
    }
    return $n;
}

=head2 _update_snp_locations

  Arg [1]    : filehandle
  Arg [2]    : filehandle
  Arg [3]    : hashref of locations, each key beiong a SNP name and each value
               being an arrayref of two values; chromosome and physical
               position.
  Example    : $n = update_snp_locations(\*STDIN, \*STDOUT, \%locations)
  Description: Update a stream of Plink BIM format records with new SNP
               location (chromosome name and physical position) information
               and writes it to another stream. The chromosome names must be
               in Plink encoded numeric format.
  Returntype : integer, number of records processed
  Caller     : general

=cut

sub _update_snp_locations {
  my ($in, $out, $locations) = @_;
  my $n = 0;

  while (my $line = <$in>) {
    chomp($line);
    my ($chr, $snp_name, $genetic_pos, $physical_pos, $allele1, $allele2) =
      split /\s+/, $line;

    unless (exists $locations->{$snp_name}) {
      confess "Failed to update the location of SNP '$snp_name'; " .
        "no location was provided\n";
    }

    my $new_loc = $locations->{$snp_name};
    unless (ref($new_loc) eq 'ARRAY' && scalar @$new_loc == 2) {
      confess "Failed to update the location of SNP '$snp_name'; " .
        "location was not a 2-element array\n";
    }

    my $new_chr = $new_loc->[0];
    my $new_pos = $new_loc->[1];

    print $out join("\t", $new_chr, $snp_name, $genetic_pos, $new_pos,
                    $allele1, $allele2), "\n";
    ++$n;
  }

  return $n;
}

sub _update_sample_genders {
  my ($in, $out, $genders) = @_;

  my $n = 0;

  while (my $line = <$in>) {
    chomp($line);

    my ($family_id, $individual_id, $paternal_id,
		$maternal_id, $gender, $phenotype) = split /\s+/, $line;

    unless (exists $genders->{$family_id}) {
      confess "Failed to update the gender of '$family_id'; " .
        "no gender was provided\n";
    }

    my $new_gender = $genders->{$family_id};
    print $out join("\t", $family_id, $individual_id, $paternal_id,
					$maternal_id, $new_gender, $phenotype), "\n";
    ++$n;
  }

  return $n;
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
