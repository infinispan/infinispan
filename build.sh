#!/bin/bash

# $Id: build.sh 105735 2010-06-04 19:45:13Z pgier $

PROGNAME=`basename $0`
DIRNAME=`dirname $0`
GREP="grep"
ROOT="/"
MVN="mvn"

MAVEN_OPTS="$MAVEN_OPTS -Xmx768M"
export MAVEN_OPTS

#  Use the maximum available, or set MAX_FD != -1 to use that
MAX_FD="maximum"

#  OS specific support (must be 'true' or 'false').
cygwin=false;
darwin=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;

    Darwin*)
        darwin=true
        ;;
esac

#
#  Helper to complain.
#
die() {
    echo "${PROGNAME}: $*"
    exit 1
}

#
#  Helper to complain.
#
warn() {
    echo "${PROGNAME}: $*"
}

#
#  Helper to source a file if it exists.
#
source_if_exists() {
    for file in $*; do
        if [ -f "$file" ]; then
            . $file
        fi
    done
}

say() {
    if [ $darwin = "true" ]; then
        # On Mac OS, notify via Growl
        which -s growlnotify && growlnotify --name Maven --sticky --message "Infinispan build: $@"
    fi
    if [ `uname -s` == "Linux" ]; then
        # On Linux, notify via notify-send
        which notify-send && notify-send "Infinispan build: $@"
    fi
}

ulimit_check() {
    if [ $cygwin = "false" ]; then
        HARD_LIMIT=`ulimit -H $1`
        if [[ "$HARD_LIMIT" != "unlimited" && "$HARD_LIMIT" -lt "$2" ]]; then
            warn "The hard limit for $1 is $HARD_LIMIT which is lower than the expected $2. This might be a problem"
        else
            SOFT_LIMIT=`ulimit -S $1`
            if [[ "$SOFT_LIMIT" != "unlimited" && "$SOFT_LIMIT" -lt "$2" ]]; then
                ulimit $1 $2
                if [ $? -ne 0 ]; then
                    warn "Could not set ulimit $1 $2"
                fi
            fi
        fi
    fi
}

#
#  Main function.
#
main() {
    #  If there is a build config file, source it.
    source_if_exists "$DIRNAME/build.conf" "$HOME/.build.conf"

    #  Increase some limits if we can
    ulimit_check -n 1024
    ulimit_check -u 2048

    #  Setup some build properties
    MVN_OPTS="$MVN_OPTS -Dbuild.script=$0"

    #  Change to the directory where the script lives, so users are not forced
    #  to be in the same directory as build.xml.
    cd $DIRNAME

    #  Add smoke integration test directives before calling maven.
    MVN_SETTINGS_XML_ARGS="-s maven-settings.xml"
    MVN_GOAL="";
    ADDIT_PARAMS="";
    #  For each parameter, check for testsuite directives.
    for param in $@ ; do
        case $param in
            ## -s .../settings.xml - don't use our own.
            -s)      MVN_SETTINGS_XML_ARGS="";   ADDIT_PARAMS="$ADDIT_PARAMS $param";;
            -*)      ADDIT_PARAMS="$ADDIT_PARAMS $param";;
            clean)   MVN_GOAL="$MVN_GOAL$param ";;
            test)    MVN_GOAL="$MVN_GOAL$param ";;
            install) MVN_GOAL="$MVN_GOAL$param ";;
            deploy)  MVN_GOAL="$MVN_GOAL$param ";;
            site)    MVN_GOAL="$MVN_GOAL$param ";;
            *)       ADDIT_PARAMS="$ADDIT_PARAMS $param";;
        esac
    done
    #  Default goal if none specified.
    if [ -z "$MVN_GOAL" ]; then MVN_GOAL="install"; fi

    MVN_GOAL="$MVN_GOAL $TESTS"

    #  Export some stuff for maven.
    export MVN_OPTS MVN_GOAL

    # The default arguments.  `mvn -s ...` will override this.
    MVN_ARGS=${MVN_ARGS:-"$MVN_SETTINGS_XML_ARGS"};

    echo "$MVN $MVN_ARGS $MVN_GOAL $ADDIT_PARAMS"

    #  Execute in debug mode, or simply execute.
    if [ "x$MVN_DEBUG" != "x" ]; then
        /bin/sh -x $MVN $MVN_ARGS $MVN_GOAL $ADDIT_PARAMS
    else
        $MVN $MVN_ARGS $MVN_GOAL $ADDIT_PARAMS
    fi
    if [ $? -eq 0 ]; then
        say SUCCESS
    else
        say FAILURE
        exit $?
    fi
}

##
##  Bootstrap
##
main "$@"

