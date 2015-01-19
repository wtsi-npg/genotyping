use utf8;

package Build;

use strict;
use warnings;
use Carp;
use File::Basename;
use File::Copy;
use File::Path qw(make_path);
use File::Spec;

use base 'WTSI::DNAP::Utilities::Build';

#
# Prepare configuration for tests
#
sub ACTION_test {
  my ($self) = @_;

  $self->copy_files_by_category('conf_files', './blib');
  $self->copy_files_by_category('ini_files', './blib');

  {
    # Ensure that the tests can see the Perl and R scripts
    local $ENV{PATH} = "./bin:../r/bin:$ENV{PATH}";

    $self->SUPER::ACTION_test;
  }
}

#
# Subclass the install action to include config and R files
#
sub ACTION_install {
    my ($self) = @_;

    $self->copy_files_by_category('conf_files', $self->install_base, 1);
    $self->copy_files_by_category('ini_files', $self->install_base, 1);
    $self->copy_files_by_category('R_files', $self->install_base, 1);
    $self->SUPER::ACTION_install;
    return $self;
}

#
# Additional target to install gendermix standalone
#
sub ACTION_install_gendermix {
  my ($self) = @_;
  my $gendermix_manifest = './etc/gendermix_manifest.txt';
  $self->process_alternate_manifest($gendermix_manifest);
  return $self;
}

sub process_alternate_manifest {
    # reads an alternate manifest
    # manifest lists one file per line, followed by (optional) translation
    # installs each item in manifest, under the install_base directory
    # use to install standalone gendermix check
    my ($self, $manifest_path) = @_;
    open my $in, "<", $manifest_path ||
        croak "Cannot open manifest $manifest_path: $!";
    my %manifest;
    while (<$in>) {
        chomp;
        my @words = split;
        my $src = shift @words;
        my $dest = shift @words;
        if ($dest) {
          $manifest{$src} = $dest;
        }
        else {
          $manifest{$src} = $src;
        }
    }
    close $in || croak "Cannot close manifest $manifest_path: $!";
    my $dest_base = $self->install_base;
    my @installed;
    foreach my $src_file (keys %manifest) {
        my $dest_file = File::Spec->catfile($dest_base, $manifest{$src_file});
        my $file = $self->copy_if_modified(from => $src_file, to => $dest_file);
        if ($file) {
            push(@installed, $file);
            print STDERR "Installing $file\n";
        }
    }
    return @installed;
}

sub copy_files_by_category {
  my ($self, $category, $destination, $verbose) = @_;

  print STDERR "Installing category $category\n" if $verbose;

  # This is horrible - there must be a better way
  if ($self->current_action eq 'install' ||
      $self->current_action eq 'test') {
    my $translations = $self->{properties}->{$category};

    my @installed;
    foreach my $src_file (keys %$translations) {
      my $dest_file = File::Spec->catfile($destination,
                                          $translations->{$src_file});

      my $file = $self->copy_if_modified(from => $src_file, to => $dest_file);
      if ($file) {
        push(@installed, $file);
        print STDERR "Installing $file\n" if $verbose;
      }
    }

    return @installed;
  }
}

1;
