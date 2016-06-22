#!/bin/bash

# Author: Iain Bancarz <ib5@sanger.ac.uk>, June 2016

# script to install Perl dependencies of the genotyping pipeline using cpanm
# also installs Perl and Ruby components of the pipeline
# does *not* install baton and other non-Perl dependencies; these are specified in the genotyping modulefile

# similar in purpose to npg_irods/scripts/travis_install.sh

if [ -z $INSTALL_ROOT ]; then
    echo "INSTALL_ROOT environment variable must specify the path to a directory for installation; exiting." >&2
    exit 1
elif [ ! -e $INSTALL_ROOT ]; then
    echo "INSTALL_ROOT environment variable path $INSTALL_ROOT does not exist; exiting." >&2
    exit 1
elif [ ! -d $INSTALL_ROOT ]; then
    echo "INSTALL_ROOT environment variable path $INSTALL_ROOT is not a directory; exiting." >&2
    exit 1
else
    echo "Installing pipeline to root directory $INSTALL_ROOT"
fi

WTSI_DNAP_UTILITIES_VERSION="0.5.1"
ML_WAREHOUSE_VERSION="2.1"
ALIEN_TIDYP_VERSION="1.4.7"
NPG_TRACKING_VERSION="85.3"
NPG_IRODS_VERSION="2.4.0"
RUBY_VERSION="1.8.7-p330"
LIB_RUBY_VERSION="0.3.0"

START_DIR=$PWD

# look for pipeline source code, relative to location of install script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PERL_DIR="$SCRIPT_DIR/../perl/"
RUBY_DIR="$SCRIPT_DIR/../ruby/genotyping-workflows/"
SRC_DIRS=($PERL_DIR $RUBY_DIR)
for DIR in ${SRC_DIRS[@]}; do
    if [ ! -d $DIR ]; then
        echo -n "Genotyping source code directory $DIR not found; "
        echo "install halted" >&2 
        exit 1
    fi
done

# create and cd to temp directory
TEMP_NAME=`mktemp -d genotyping_temp.XXXXXXXX`
TEMP=`readlink -f $TEMP_NAME` # full path to temp directory
export PATH=$TEMP:$PATH # ensure cpanm download is on PATH
cd $TEMP

# use wget to download tarballs to install
URLS=(https://github.com/wtsi-npg/perl-dnap-utilities/releases/download/$WTSI_DNAP_UTILITIES_VERSION/WTSI-DNAP-Utilities-$WTSI_DNAP_UTILITIES_VERSION.tar.gz \
https://github.com/wtsi-npg/ml_warehouse/releases/download/$ML_WAREHOUSE_VERSION/ml_warehouse-$ML_WAREHOUSE_VERSION.tar.gz \
http://search.cpan.org/CPAN/authors/id/K/KM/KMX/Alien-Tidyp-v$ALIEN_TIDYP_VERSION.tar.gz \
https://github.com/wtsi-npg/npg_tracking/releases/download/$NPG_TRACKING_VERSION/npg-tracking-$NPG_TRACKING_VERSION.tar.gz \
https://github.com/wtsi-npg/perl-irods-wrap/releases/download/$NPG_IRODS_VERSION/WTSI-NPG-iRODS-$NPG_IRODS_VERSION.tar.gz )

for URL in ${URLS[@]}; do
    wget $URL
    if [ $? -ne 0 ]; then
        echo -n "Failed to download $URL; non-zero exit status " >&2
        echo " from wget; install halted" >&2
        exit 1
    fi
done
eval $(perl -Mlocal::lib=$INSTALL_ROOT) # set environment variables

# use local installation of cpanm
module load cpanm/1.7042

# install prerequisites from tarfiles
TARFILES=(WTSI-DNAP-Utilities-$WTSI_DNAP_UTILITIES_VERSION.tar.gz \
ml_warehouse-$ML_WAREHOUSE_VERSION.tar.gz \
Alien-Tidyp-v$ALIEN_TIDYP_VERSION.tar.gz \
npg-tracking-$NPG_TRACKING_VERSION.tar.gz \
WTSI-NPG-iRODS-$NPG_IRODS_VERSION.tar.gz)

for FILE in ${TARFILES[@]}; do
    cpanm --installdeps $FILE --self-contained  --notest
    if [ $? -ne 0 ]; then
        echo "cpanm --installdeps failed for $FILE; install halted" >&2
        exit 1
    fi
    cpanm --install $FILE --notest
    if [ $? -ne 0 ]; then
        echo "cpanm --install failed for $FILE; install halted" >&2
        exit 1
    fi
done

cd $PERL_DIR
# need a version string acceptable to cpanm, eg. 1.12.1 or 1.13.0-rc
# version strings containing underscores _ won't work
if [ -z $GENOTYPING_VERSION ] ; then
    GIT_VERSION=`git describe --dirty --always`
    echo "GENOTYPING_VERSION environment variable not set." >&2
    echo "Using default version string '$GIT_VERSION'" >&2
else
    echo "GENOTYPING_VERSION environment variable found." >&2
    echo "Installing with version string '$GENOTYPING_VERSION'" >&2
fi

cpanm --installdeps . --self-contained --notest 
if [ $? -ne 0 ]; then
    echo "cpanm --installdeps failed for genotyping; install halted" >&2
    exit 1
fi

perl Build.PL
./Build install --install_base $INSTALL_ROOT
if [ $? -ne 0 ]; then
    echo "Genotyping pipeline Perl installation failed; install halted " >&2
    exit 1
fi

# now set Ruby environment variables and install
export RUBY_HOME=/software/gapi/pkg/ruby/$RUBY_VERSION
export PATH=$RUBY_HOME/bin:$PATH
export MANPATH=$RUBY_HOME/share/man:$MANPATH
export GEM_HOME=$INSTALL_ROOT
export GEM_PATH=/software/gapi/pkg/lib-ruby/$LIB_RUBY_VERSION
export GEM_PATH=$INSTALL_ROOT:$GEM_PATH
export PATH=/software/gapi/pkg/lib-ruby/$LIB_RUBY_VERSION/bin:$PATH

cd $RUBY_DIR
rake gem
if [ $? -ne 0 ]; then
    echo "'rake gem' failed for genotyping; install halted" >&2
    exit 1
fi

GENOTYPING_GEM=`ls -t pkg | head -n 1`
gem install pkg/$GENOTYPING_GEM
if [ $? -ne 0 ]; then
    echo "'gem install' failed for genotyping; install halted" >&2
    exit 1
fi

# clean up temporary directory
cd $START_DIR
rm -Rf $TEMP

exit 0
