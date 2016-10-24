#!/bin/bash

# Author: Iain Bancarz <ib5@sanger.ac.uk>, June 2016

# script to install Perl dependencies of the genotyping pipeline using cpanm
# also installs Perl and Ruby components of the pipeline
# does *not* install baton and other non-Perl dependencies; these are specified in the genotyping modulefile

# similar in purpose to npg_irods/scripts/travis_install.sh

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
CPANM_TEMP="$TEMP/cpanm"
mkdir $CPANM_TEMP
export PERL_CPANM_HOME=$CPANM_TEMP
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
        echo -n "Failed to download $URL; non-zero exit status " 1>&2
        echo " from wget; install halted" 1>&2
        exit 1
    fi
done
eval $(perl -Mlocal::lib=$INSTALL_ROOT) # set environment variables


# check that some version of cpanm is available
echo -n "cpanm script: "
which cpanm
if [ $? -ne 0 ]; then
    echo "cpanm not found; install halted" 1>&2
    exit 1
fi
# upgrade to latest versions of cpanm and its dependencies
cpanm --installdeps App::cpanminus
if [ $? -ne 0 ]; then
    echo "Failed on dependencies for cpanm upgrade; install halted" 1>&2
    exit 1
fi
cpanm --install App::cpanminus
if [ $? -ne 0 ]; then
    echo "Failed on latest version of cpanm; install halted" 1>&2
    exit 1
fi
# script location can be slow to update, so store in a variable
CPANM_SCRIPT=$INSTALL_ROOT/bin/cpanm
if [ ! -e $CPANM_SCRIPT ]; then
    echo "Cannot find cpanm script '$CPANM_SCRIPT'; install halted" 1>&2
    exit 1 
else
    echo -n "cpanm script is $CPANM_SCRIPT, version: "
    $CPANM_SCRIPT --version
fi

# install prerequisites from tarfiles
TARFILES=(WTSI-DNAP-Utilities-$WTSI_DNAP_UTILITIES_VERSION.tar.gz \
ml_warehouse-$ML_WAREHOUSE_VERSION.tar.gz \
Alien-Tidyp-v$ALIEN_TIDYP_VERSION.tar.gz \
npg-tracking-$NPG_TRACKING_VERSION.tar.gz \
WTSI-NPG-iRODS-$NPG_IRODS_VERSION.tar.gz)

for FILE in ${TARFILES[@]}; do
    $CPANM_SCRIPT --installdeps $FILE --self-contained  --notest
    if [ $? -ne 0 ]; then
        echo "$CPANM_SCRIPT --installdeps failed for $FILE; install halted" 1>&2
        exit 1
    fi
    $CPANM_SCRIPT --install $FILE --notest
    if [ $? -ne 0 ]; then
        echo "$CPANM_SCRIPT --install failed for $FILE; install halted" 1>&2
        exit 1
    fi
done

cd $PERL_DIR

$CPANM_SCRIPT --installdeps . --self-contained --notest
if [ $? -ne 0 ]; then
    echo "$CPANM_SCRIPT --installdeps failed for genotyping; install halted" 1>&2
    exit 1
fi

perl Build.PL
./Build install --install_base $INSTALL_ROOT
if [ $? -ne 0 ]; then
    echo "Genotyping pipeline Perl installation failed; install halted " 1>&2
    exit 1
fi

cd $RUBY_DIR
GENOTYPING_GEM=`rake gem | grep File | cut -f 4 -d " "`
if [ $? -ne 0 ]; then
    echo "'rake gem' failed for genotyping; install halted" 1>&2
    exit 1
fi
GEM_FILE_PATH=pkg/$GENOTYPING_GEM
if [ ! -f $GEM_FILE_PATH ]; then
    echo "Expected gem file '$GEM_FILE_PATH' not found; install halted" 1>&2
    exit 1
fi
gem install $GEM_FILE_PATH
if [ $? -ne 0 ]; then
    echo "'gem install' failed for genotyping; install halted" 1>&2
    exit 1
fi

exit 0
