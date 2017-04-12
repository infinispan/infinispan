#!/bin/bash

while test $# -gt 0; do
    case "$1" in
            -h|--help)
                    echo "This script changes revisions for JDG package"
                    echo " "
                    echo "if no arguments specified - use interactive mode"
                    echo " "
                    echo "options:"
                    echo "-h, --help                Show brief help"
                    echo "--new-revision 			Specify new revision"
                    echo "--pom-location 			Main POM file location"
                    exit 1
                    ;;
            --new-revision*)
                    NEWVERSION=`echo $2`
                    shift
                    ;;
            --pom-location*)
                    POMLOCATION=`echo $2`
                    shift
                    ;;
            *)
                    shift
                    ;;
    esac
done

DIR=`dirname $0`
BASEDIR=`readlink -f "$DIR"`
if [ -z $POMLOCATION ]; then
	POMLOCATION='pom.xml'
fi
VERSION=`xsltproc $BASEDIR/extract-gav.xslt $POMLOCATION | cut -d: -f4`

echo "Pom file location: $POMLOCATION"
echo "Current version: $VERSION"
echo -n "New version: "
if [ -z $NEWVERSION ]; then
    read NEWVERSION
fi

echo "Changing from $VERSION to $NEWVERSION"

find . -name 'pom.xml' -exec sed -i "s/<version>$VERSION<\/version>/<version>$NEWVERSION<\/version>/g" {} \;
