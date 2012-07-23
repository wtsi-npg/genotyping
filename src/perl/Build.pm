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
  my $self = shift;

  return $self->process_files_by_category('conf_files');
}

sub process_ini_files {
  my $self = shift;

  return $self->process_files_by_category('ini_files');
}

sub process_sql_files {
  my $self = shift;

  return $self->process_files_by_category('sql_files');
}

sub process_files_by_category {
  my ($self, $category) = @_;

  if ($self->current_action eq 'install') {
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
