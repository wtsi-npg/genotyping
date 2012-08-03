use utf8;

package Build;

use strict;
use warnings;
use File::Basename;
use File::Copy;
use File::Path qw(make_path);
use File::Spec;

use base 'Module::Build';

sub process_conf_files {
  my ($self) = @_;
  return $self->process_files_by_category('conf_files');
}

sub process_ini_files {
  my ($self) = @_;
  return $self->process_files_by_category('ini_files');
}

sub process_sql_files {
  my ($self) = @_;
  return $self->process_files_by_category('sql_files');
}

sub process_files_by_category {
  my ($self, $category) = @_;

  # Is 'code' the correct action? The 'config_files' action would seem
  # more appropriate. However, that doesn't seem to call any of the
  # process_*_files which the documentation says we should use to
  # install custom file types.
  if ($self->current_action eq 'code') {
    my $translations = $self->{properties}->{$category};
    my $dest_base = $self->install_base;

    my @installed;
    foreach my $src_file (keys %$translations) {
      my $dest_file = File::Spec->catfile($dest_base,
                                          $translations->{$src_file});

      my $file = $self->copy_if_modified(from => $src_file, to => $dest_file);
      if ($file) {
        push(@installed, $file);
        print STDERR "Installing $file\n";
      }
    }

    return @installed;
  }
}

1;
