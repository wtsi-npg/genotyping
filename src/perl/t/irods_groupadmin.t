use strict;
use Test::More tests => 8;
use Test::Exception;


use_ok 'WTSI::NPG::iRODS::GroupAdmin';

#$ENV{irodsEnvFile}= `echo ~/.irods/.irodsEnv-dev`; #to work with a real (test) iRODS
$ENV{PATH}='t/irods_groupadmin:'.$ENV{PATH}; # use mock iRODS tools

my $iga;
lives_ok {
  $iga = WTSI::NPG::iRODS::GroupAdmin->new();
} 'create object';

cmp_ok scalar $iga->lg(), q(==), 2840, 'correct number of groups found';
is_deeply [sort $iga->lg(q(ss_2676))], [qw(ac18#Sanger1-dev am23#Sanger1-dev cm10#Sanger1-dev da2#Sanger1-dev dj3#Sanger1-dev dj6#Sanger1-dev gb11#Sanger1-dev glm#Sanger1-dev jg10#Sanger1-dev jo6#Sanger1-dev jws#Sanger1-dev kr4#Sanger1-dev ks5#Sanger1-dev lsq#Sanger1-dev mm6#Sanger1-dev om1#Sanger1-dev sa3#Sanger1-dev sa4#Sanger1-dev sc11#Sanger1-dev si3#Sanger1-dev so1#Sanger1-dev ty1#Sanger1-dev vrr#Sanger1-dev)], 'group membership';
cmp_ok scalar $iga->lg(q(ss_0)), q(==), 0, 'zero member group';
throws_ok {
  $iga->lg(q(ss_000));
} qr/does not exist/sm, 'non existent group throw';
throws_ok {
  $iga->lg(q());
} qr/empty string/sm, 'empty string group throw';

 throws_ok {
  $ENV{PATH}=q();
  WTSI::NPG::iRODS::GroupAdmin->new();
 } qr/Command 'i\S+' not found/sm, 'no igroupadmin';
1;

