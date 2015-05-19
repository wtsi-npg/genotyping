
package BuildFluidigm;

use strict;
use warnings;
use List::AllUtils qw(any);

use base 'WTSI::DNAP::Utilities::Build';

# Build only this subset of files
our @fluidigm_subset = ('publish_fluidigm_genotypes.pl',
                        'publish_snpset.pl',
                        'update_fluidigm_metadata.pl',
                        'WTSI/NPG/Addressable.pm',
                        'WTSI/NPG/Annotator.pm',
                        'WTSI/NPG/Database.pm',
                        'WTSI/NPG/Database/DBIx.pm',
                        'WTSI/NPG/Database/MLWarehouse.pm',
                        'WTSI/NPG/Genotyping/Annotation.pm',
                        'WTSI/NPG/Genotyping/Annotator.pm',
                        'WTSI/NPG/Genotyping/Fluidigm/AssayDataObject.pm',
                        'WTSI/NPG/Genotyping/Fluidigm/AssayResult.pm',
                        'WTSI/NPG/Genotyping/Fluidigm/AssayResultSet.pm',
                        'WTSI/NPG/Genotyping/Fluidigm/ExportFile.pm',
                        'WTSI/NPG/Genotyping/Fluidigm/Publisher.pm',
                        'WTSI/NPG/Genotyping/Fluidigm/ResultSet.pm',
                        'WTSI/NPG/Genotyping/Fluidigm/Subscriber.pm',
                        'WTSI/NPG/Genotyping/Reference.pm',
                        'WTSI/NPG/Genotyping/SNP.pm',
                        'WTSI/NPG/Genotyping/SNPSet.pm',
                        'WTSI/NPG/Genotyping/SNPSetPublisher.pm',
                        'WTSI/NPG/Genotyping/Types.pm',
                        'WTSI/NPG/Publisher.pm');

sub ACTION_test {
  my ($self) = @_;

  {
    # Ensure that the tests can see the Perl scripts
    local $ENV{PATH} = "./bin:" . $ENV{PATH};

    $self->SUPER::ACTION_test;
  }
}

sub ACTION_code {
  my ($self) = @_;

  $self->SUPER::ACTION_code;

  # Prune everything apart from the Fluidigm components
  my @built_r       = _find_files(qr{\.R$}msx,  'blib/bin');
  my @built_modules = _find_files(qr{\.pm$}msx, 'blib/lib');
  my @built_scripts = _find_files(qr{\.pl$}msx, 'blib/script');
  foreach my $file (@built_r, @built_modules, @built_scripts) {
    if (any { $file =~ m{$_$} } @fluidigm_subset) {
      $self->log_debug("Matched $file with Fluidigm subset\n");
    }
    else {
      $self->log_debug("Pruning $file from ./blib\n");
      unlink $file or warn "Failed to unlink $file: $!";
    }
  }
}

sub _find_files {
  my ($regex, $root) = @_;

  my @results;
  if (-d $root) {
    File::Find::find(sub {
                       if (m{$regex} and -f) {
                         push @results, $File::Find::name;
                       }
                     }, $root);
  }

  return @results;
}

1;
