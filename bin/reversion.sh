#!/bin/bash
PROCESS_SCHEMAS=true

function rewriteXSDs() {
    SEPARATOR=$1
    OLDMAJOR=$2
    OLDMINOR=$3
    NEWMAJOR=$4
    NEWMINOR=$5
    
    SCHEMAS=`find . -path "*/src/main/resources/*$OLDMAJOR$SEPARATOR$OLDMINOR.xsd"`
    for OLDSCHEMA in $SCHEMAS; do
        # Rewrite the filename
        NEWSCHEMA=`echo "$OLDSCHEMA"|sed "s/$OLDMAJOR$SEPARATOR$OLDMINOR/$NEWMAJOR$SEPARATOR$NEWMINOR/"`
        echo "$OLDSCHEMA ==> $NEWSCHEMA"
        mv $OLDSCHEMA $NEWSCHEMA
        # We need to do different replacements
        sed -i "s/:$OLDMAJOR.$OLDMINOR\"/:$NEWMAJOR.$NEWMINOR\"/g" $NEWSCHEMA
        sed -i "s/-$OLDMAJOR$SEPARATOR$OLDMINOR.xsd/-$NEWMAJOR$SEPARATOR$NEWMINOR.xsd/g" $NEWSCHEMA
        git rm $OLDSCHEMA
        git add $NEWSCHEMA
    done
}

function rewriteXML() {
    XML=$1
    OLDMAJOR=$2
    OLDMINOR=$3
    NEWMAJOR=$4
    NEWMINOR=$5

    sed -i "s/:$OLDMAJOR.$OLDMINOR/:$NEWMAJOR.$NEWMINOR/g" $XML
    sed -i "s/-$OLDMAJOR.$OLDMINOR.xsd/-$NEWMAJOR.$NEWMINOR.xsd/g" $XML
    git add $XML
}

while test $# -gt 0; do
    case "$1" in
            -h|--help)
                    echo "This script changes the version"
                    echo " "
                    echo "if no arguments specified - use interactive mode"
                    echo " "
                    echo "options:"
                    echo "-h, --help                Show brief help"
                    echo "--new-revision 			Specify new revision"
                    echo "--pom-location 			Main POM file location"
                    echo "--no-schemas              Do not upgrade the schemas"
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
            --no-schemas*)
                    PROCESS_SCHEMAS=false
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

for POM in $(find . -name 'pom.xml'); do
   sed -i "s/<version>$VERSION<\/version>/<version>$NEWVERSION<\/version>/g" $POM
   git add $POM
done

OLDSCHEMAMAJOR=`echo $VERSION|cut -d. -f1`
OLDSCHEMAMINOR=`echo $VERSION|cut -d. -f2`
OLDSCHEMAVERSION=`echo $VERSION|cut -d. -f1,2`
NEWSCHEMAMAJOR=`echo $NEWVERSION|cut -d. -f1`
NEWSCHEMAMINOR=`echo $NEWVERSION|cut -d. -f2`
NEWSCHEMAVERSION=`echo $NEWVERSION|cut -d. -f1,2`

if [ "$OLDSCHEMAVERSION" != "$NEWSCHEMAVERSION" ] && [ "$PROCESS_SCHEMAS" = true ] ;  then
    echo "Current schema: $OLDSCHEMAVERSION"
    echo "New schema:     $NEWSCHEMAVERSION"
    
    # Set master schema version
    sed -i "s/<infinispan.base.version>$OLDSCHEMAVERSION<\/infinispan.base.version>/<infinispan.base.version>$NEWSCHEMAVERSION<\/infinispan.base.version>/g" pom.xml
    # Set the codename to WIP
    sed -E -i "s/<infinispan.codename>[^<]+<\/infinispan.codename>/<infinispan.codename>WIP<\/infinispan.codename>/g" pom.xml
    git add pom.xml
    
    # Create new unified configuration
    cp core/src/test/resources/configs/unified/$OLDSCHEMAVERSION.xml core/src/test/resources/configs/unified/$NEWSCHEMAVERSION.xml
    rewriteXML core/src/test/resources/configs/unified/$NEWSCHEMAVERSION.xml $OLDSCHEMAMAJOR $OLDSCHEMAMINOR $NEWSCHEMAMAJOR $NEWSCHEMAMINOR

    # Update the distribution configurations
    CONFIGS=`find distribution/src/main/release/common/configs/config-samples -name '*.xml'`
    for CONFIG in $CONFIGS; do
         rewriteXML $CONFIG $OLDSCHEMAMAJOR $OLDSCHEMAMINOR $NEWSCHEMAMAJOR $NEWSCHEMAMINOR
    done

    # Rewrite the XSDs
    rewriteXSDs . $OLDSCHEMAMAJOR $OLDSCHEMAMINOR $NEWSCHEMAMAJOR $NEWSCHEMAMINOR
    rewriteXSDs _ $OLDSCHEMAMAJOR $OLDSCHEMAMINOR $NEWSCHEMAMAJOR $NEWSCHEMAMINOR

    echo ""
    echo "WARNING: you will need to manually modify the server namespaces and parsing testcases to look for the new schema"
    echo "- org.infinispan.server.jgroups.subsystem.JGroupsSchema"
    echo "- org.infinispan.server.jgroups.subsystem.SubsystemParsingTestCase"
    echo "- org.jboss.as.clustering.infinispan.subsystem.Namespace"
    echo "- org.jboss.as.clustering.infinispan.subsystem.SubsystemParsingTestCase"
    echo "- org.infinispan.server.endpoint.EndpointSubsystemTestCase"
fi



