use utf8;

package WTSI::NPG::iRODS::FileHint;

use Moose::Role;

requires 'name', 'num_criteria', 'test';

no Moose;

1;
