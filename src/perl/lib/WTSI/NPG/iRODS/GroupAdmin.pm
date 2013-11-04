package WTSI::NPG::iRODS::GroupAdmin;
use Moose;
use IPC::Run qw(start);

sub str_ref {
                 my $tmpstr='';
                 return \$tmpstr;
}

has '_foo' => (
  'is' => 'ro',
  'isa' => 'HashRef',
  'default' => sub {return {in=>'', out=>''} },
);

has '_in' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {str_ref},
);

has '_out' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {str_ref},
);


has '_harness' => (
  'is' => 'ro',
  'default' => sub {
                 my ($self) = @_;
                 #my $out_ref = $self->_out;
                 my$foo=$self->_foo;
use Data::Dumper;
warn Dumper [$self->_foo,$foo,$foo->{out},\$foo->{out}];
                 my $out_ref = \$foo->{out};
warn $out_ref;
                 #my $in_ref = $self->_in;
                 my $in_ref = \$foo->{in};
warn $in_ref;
                 my $h = start [qw(igroupadmin)], q(<pty<), $in_ref, q(>pty>), $out_ref;
                 while (1){ $h->pump; last if ${$out_ref}=~/^groupadmin\>/sm; }
                 ${$out_ref}=q();
                 return $h;
               }
);

sub lg {
  my $self = shift;
  #${$self->_out}=q();
  my$foo=$self->_foo;
  my $out_ref = \$foo->{out};
  ${$out_ref}=q();
  #${$self->_in}.= join q( ), q(lg), @_, qq(\n);
  my $in = join q( ), q(lg), @_, qq(\n);
  $foo->{in} = $in;
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
  #${$self->_out}=q();
  ${\$self->_foo->{out}}=q();
  #${$self->_in}="quit\n";
  ${\$self->_foo->{in}}="quit\n";
  $self->_harness->finish;
}


1;
