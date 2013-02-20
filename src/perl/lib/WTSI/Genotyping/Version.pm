
package WTSI::Genotyping::Version;

use strict;
use warnings;
use Carp;

use vars qw/$VERSION $YEAR @ISA @EXPORT_OK/;
use Exporter;
@ISA = qw/Exporter/;
@EXPORT_OK = qw/version_text write_version_log/;

$VERSION = '0.7.4';
$YEAR = '2013'; # year of last update

sub version_text {
    my $text = "WTSI Genotyping Pipeline version $VERSION\n".
        "Pipeline software copyright (c) $YEAR Genome Research Ltd.\n".
        "All rights reserved.\n";
    return $text;
}

sub write_version_log {
    # record version in given directory
    my $logdir = shift;
    if (!(-e $logdir && -d $logdir)) { 
        croak "Invalid log directory \"$logdir\"";
    }
    my $logpath =  $logdir."/version.log";
    open my $log, ">", $logpath || 
        croak "Cannot open version log \"$logpath\"";
    print $log version_text();
    close $log || croak "Cannot close version log \"$logpath\"";
    return 1;
}

return 1;
