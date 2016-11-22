#!/bin/bash

# Author: Iain Bancarz <ib5@sanger.ac.uk>, June 2016

# script to install Perl dependencies of the genotyping pipeline using cpanm
# also installs Perl and Ruby components of the pipeline
# does *not* install baton and other non-Perl dependencies; these are specified in the genotyping modulefile

# similar in purpose to npg_irods/scripts/travis_install.sh

set -e # script will exit if any command has a non-zero return value

# check for environment variables
if [ -z $INSTALL_ROOT ]; then
    echo "Environment variable INSTALL_ROOT must specify directory path for installation; exiting." >&2
    exit 1
fi
if [ -z $RUBY_HOME ]; then
    echo "Required environment variable RUBY_HOME not set; exiting." 1>&2
    exit 1
fi
if [ -z $GEM_PATH ]; then
    echo "Required environment variable GEM_PATH not set; exiting." 1>&2
    exit 1
fi
if [ -n $GEM_HOME ]; then
    echo "Warning: Existing GEM_HOME variable '$GEM_HOME' will be changed to value of INSTALL_ROOT" 1>&2
fi

# test that INSTALL_ROOT is a writable directory
if [ ! -e $INSTALL_ROOT ]; then
    echo "INSTALL_ROOT environment variable path $INSTALL_ROOT does not exist; exiting." 1>&2
    exit 1
elif [ ! -d $INSTALL_ROOT ]; then
    echo "INSTALL_ROOT environment variable path $INSTALL_ROOT is not a directory; exiting." 1>&2
    exit 1
elif  [ ! -w $INSTALL_ROOT ]; then
    echo "INSTALL_ROOT environment variable path $INSTALL_ROOT is not writable; exiting." 1>&2
    exit 1
else
    echo "Installing pipeline to root directory $INSTALL_ROOT"
fi

# set Ruby environment variables
export PATH=$RUBY_HOME/bin:$PATH
export MANPATH=$RUBY_HOME/share/man:$MANPATH
export GEM_HOME=$INSTALL_ROOT
# split GEM_PATH to find a bin directory
IFS=':' # IFS = Bash "Internal Field Separator"
GEM_ARRAY=($GEM_PATH)
unset IFS
GEM_BIN=${GEM_ARRAY[0]}/bin
if [ ! -e $GEM_BIN ]; then
    echo "Expected Ruby script directory '$GEM_BIN' does not exist" 1&>2
    exit 1
elif  [ ! -d $GEM_BIN ]; then
    echo "Expected Ruby script directory '$GEM_BIN' is not a directory" 1&>2
    exit 1
fi
export PATH=$GEM_BIN:$PATH
# update GEM_PATH
export GEM_PATH=$INSTALL_ROOT:$GEM_PATH

# version numbers
CPANM_VERSION="1.7042"
WTSI_DNAP_UTILITIES_VERSION="0.5.3"
ML_WAREHOUSE_VERSION="2.1"
ALIEN_TIDYP_VERSION="1.4.7"
NPG_TRACKING_VERSION="85.3"
NPG_IRODS_VERSION="2.5.0"

START_DIR=$PWD

# look for pipeline source code, relative to location of install script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PERL_DIR="$SCRIPT_DIR/../perl/"
RUBY_DIR="$SCRIPT_DIR/../ruby/genotyping-workflows/"
SRC_DIRS=($PERL_DIR $RUBY_DIR)
for DIR in ${SRC_DIRS[@]}; do
    if [ ! -d $DIR ]; then
        echo -n "Genotyping source code directory $DIR not found; "
        echo "install halted" 1>&2 
        exit 1
    fi
done

# ensure temporary directory is cleaned up (even after unexpected exit)
function finish {
    cd $START_DIR
    rm -Rf $TEMP
}
trap finish EXIT

# create and cd to temp directory
TEMP_NAME=`mktemp -d genotyping_temp.XXXXXXXX`
TEMP=`readlink -f $TEMP_NAME` # full path to temp directory
export PERL_CPANM_HOME="$TEMP/cpanm_home"
mkdir $PERL_CPANM_HOME
mkdir "$TEMP/cpanm" # cpanm installation directory
export PATH=$TEMP/cpanm/bin:$PATH # ensure cpanm download is on PATH
cd $TEMP

# Download, verify and install cpanm
# See checksum at http://www.cpan.org/authors/id/M/MI/MIYAGAWA/CHECKSUMS
CPANM_TARFILE=App-cpanminus-$CPANM_VERSION.tar.gz
CPANM_URL=http://search.cpan.org/CPAN/authors/id/M/MI/MIYAGAWA/$CPANM_TARFILE
wget $CPANM_URL
sha256sum -c $SCRIPT_DIR/cpanm.sha256 # verify checksum of the cpanm download
tar -xzf $CPANM_TARFILE
cd App-cpanminus-$CPANM_VERSION
perl Makefile.PL INSTALL_BASE=$TEMP/cpanm
make
make test
make install
CPANM_SCRIPT=$TEMP/cpanm/bin/cpanm
if [ ! -e $CPANM_SCRIPT ]; then
    echo "Cannot find cpanm script '$CPANM_SCRIPT'; install halted" 1>&2
    exit 1 
fi
echo -n "cpanm script is $CPANM_SCRIPT, version: "
$CPANM_SCRIPT --version
cd $TEMP

# use wget to download tarballs to install
URLS=(https://github.com/wtsi-npg/perl-dnap-utilities/releases/download/$WTSI_DNAP_UTILITIES_VERSION/WTSI-DNAP-Utilities-$WTSI_DNAP_UTILITIES_VERSION.tar.gz \
https://github.com/wtsi-npg/ml_warehouse/releases/download/$ML_WAREHOUSE_VERSION/ml_warehouse-$ML_WAREHOUSE_VERSION.tar.gz \
http://search.cpan.org/CPAN/authors/id/K/KM/KMX/Alien-Tidyp-v$ALIEN_TIDYP_VERSION.tar.gz \
https://github.com/wtsi-npg/npg_tracking/releases/download/$NPG_TRACKING_VERSION/npg-tracking-$NPG_TRACKING_VERSION.tar.gz \
https://github.com/wtsi-npg/perl-irods-wrap/releases/download/$NPG_IRODS_VERSION/WTSI-NPG-iRODS-$NPG_IRODS_VERSION.tar.gz )

for URL in ${URLS[@]}; do
    wget $URL
done
eval $(perl -Mlocal::lib=$INSTALL_ROOT) # set environment variables

# install prerequisites from tarfiles
TARFILES=(WTSI-DNAP-Utilities-$WTSI_DNAP_UTILITIES_VERSION.tar.gz \
ml_warehouse-$ML_WAREHOUSE_VERSION.tar.gz \
Alien-Tidyp-v$ALIEN_TIDYP_VERSION.tar.gz \
npg-tracking-$NPG_TRACKING_VERSION.tar.gz \
WTSI-NPG-iRODS-$NPG_IRODS_VERSION.tar.gz)

for FILE in ${TARFILES[@]}; do
    $CPANM_SCRIPT --installdeps $FILE --self-contained  --notest
    $CPANM_SCRIPT --install $FILE --notest
done

cd $PERL_DIR

$CPANM_SCRIPT --installdeps . --self-contained --notest

perl Build.PL
./Build install --install_base $INSTALL_ROOT

echo "Perl installation complete; now installing Ruby."

cd $RUBY_DIR
GENOTYPING_GEM=`rake gem | grep File | cut -f 4 -d " "`
GEM_FILE_PATH=pkg/$GENOTYPING_GEM
if [ ! -f $GEM_FILE_PATH ]; then
    echo "Expected gem file '$GEM_FILE_PATH' not found; install halted" 1>&2
    exit 1
fi
gem install $GEM_FILE_PATH

echo "Ruby installation complete."

exit 0
