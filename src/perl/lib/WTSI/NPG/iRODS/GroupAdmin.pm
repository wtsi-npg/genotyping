package WTSI::NPG::iRODS::GroupAdmin;
use Moose;
use IPC::Run qw(start);

has '_in' => (
  'is' => 'ro',
  'isa' => 'ScalarRef',
  'default' => sub {my$t=''; \$t},
);

has '_out' => (
  'is' => 'ro',
  'isa' => 'ScalarRef',
  'default' => sub {my$t=''; \$t},
);


has '_harness' => (
  'is' => 'ro',
  'builder' => '_build__harness',
  'lazy' => 1
);

sub _build__harness {
                 my ($self) = @_;
                 my $out_ref = $self->_out;
#use Data::Dumper;
#warn Dumper [$self];
#warn $out_ref;
                 my $in_ref = $self->_in;
#warn $in_ref;
                 my $h = start [qw(igroupadmin)], q(<pty<), $in_ref, q(>pty>), $out_ref;
                 while (1){ $h->pump; last if ${$out_ref}=~/^groupadmin\>/sm; }
                 ${$out_ref}=q();
                 return $h;
}

sub lg {
  my $self = shift;
  my $out_ref = $self->_out;
  ${$out_ref}=q();
  my $in = join q( ), q(lg), @_, qq(\n);
  ${$self->_in} = $in;
  while (1){ $self->_harness->pump ; last if ${$out_ref}=~s/\r?\n\r?^groupadmin\>//sm; }
  ${$out_ref}=~s/\r//smg;
  ${$out_ref}=~s/\A$in//sm;
  if(@_){
    ${$out_ref}=~s/Members of group $_[0]:\n//sm;
  }
  my@results=split /\r?\n\r?/, ${$out_ref};
  ${$out_ref}=q();
  return @results;
}

sub DEMOLISH {
  my ($self) = @_;
  ${$self->_out}=q();
  ${$self->_in}="quit\n";
  $self->_harness->finish;
}


1;
