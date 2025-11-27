#!/bin/bash
PROCESS_SCHEMAS=true

function rewriteXSDs() {
    SEPARATOR=$1
    OLDMAJOR=$2
    OLDMINOR=$3
    NEWMAJOR=$4
    NEWMINOR=$5
    
    SCHEMAS=$(find . -path "*/src/main/resources/*$OLDMAJOR$SEPARATOR$OLDMINOR.xsd" -type f)
    for OLDSCHEMA in $SCHEMAS; do
        # Rewrite the filename
        NEWSCHEMA=${OLDSCHEMA//${OLDMAJOR}${SEPARATOR}${OLDMINOR}/${NEWMAJOR}${SEPARATOR}${NEWMINOR}}
        echo "$OLDSCHEMA ==> $NEWSCHEMA"
        mv "$OLDSCHEMA" "$NEWSCHEMA"
        # We need to do different replacements
        sed -i "s/:${OLDMAJOR}.${OLDMINOR}\"/:${NEWMAJOR}.${NEWMINOR}\"/g" "${NEWSCHEMA}"
        sed -i "s/-$OLDMAJOR$SEPARATOR$OLDMINOR.xsd/-$NEWMAJOR$SEPARATOR$NEWMINOR.xsd/g" "$NEWSCHEMA"
        git rm "$OLDSCHEMA"
        git add "$NEWSCHEMA"
    done
}

function rewriteXML() {
    XML=$1
    OLDMAJOR=$2
    OLDMINOR=$3
    NEWMAJOR=$4
    NEWMINOR=$5

    sed -i "s/:$OLDMAJOR.$OLDMINOR/:$NEWMAJOR.$NEWMINOR/g" "$XML"
    sed -i "s/-$OLDMAJOR.$OLDMINOR.xsd/-$NEWMAJOR.$NEWMINOR.xsd/g" "$XML"
    git add "$XML"
}

function copyXMLs() {
    SEPARATOR=$1
    OLDMAJOR=$2
    OLDMINOR=$3
    NEWMAJOR=$4
    NEWMINOR=$5

    XMLS=$(find . -name "*${OLDMAJOR}${SEPARATOR}${OLDMINOR}.xml" -type f)
    for XML in $XMLS; do
       if ! git check-ignore -q "$XML"; then
          NEWXML=${XML//${OLDMAJOR}${SEPARATOR}${OLDMINOR}/${NEWMAJOR}${SEPARATOR}${NEWMINOR}}
          echo Adding "$NEWXML"
          cp "$XML" "$NEWXML"
          rewriteXML "$NEWXML" "$OLDMAJOR" "$OLDMINOR" "$NEWMAJOR" "$NEWMINOR"
       fi
    done

    PROPERTIES_FILES=$(find . -name "*${OLDMAJOR}${SEPARATOR}${OLDMINOR}.properties" -type f)
    for PROPERTIES in $PROPERTIES_FILES; do
       if ! git check-ignore -q "$PROPERTIES"; then
          NEWPROPERTIES=${PROPERTIES//${OLDMAJOR}${SEPARATOR}${OLDMINOR}/${NEWMAJOR}${SEPARATOR}${NEWMINOR}}
          echo Adding "$NEWPROPERTIES"
          cp "$PROPERTIES" "$NEWPROPERTIES"
          git add "$NEWPROPERTIES"
       fi
    done
}

function addNamespace() {
    OLDMAJOR=$1
    OLDMINOR=$2
    NEWMAJOR=$3
    NEWMINOR=$4

    NAMESPACES=$(find . -name "*.namespaces" -type f)
    for NAMESPACE in $NAMESPACES; do
        OLDNAMESPACE=$(grep "${OLDMAJOR}.${OLDMINOR}" "$NAMESPACE")
        echo "Modifying $NAMESPACE"
        echo "${OLDNAMESPACE/$OLDMAJOR.$OLDMINOR/$NEWMAJOR.$NEWMINOR}" >> "$NAMESPACE"
    done
}

while test $# -gt 0; do
    case "$1" in
            -h|--help)
                    echo "This script changes the versions, updates the schemas and the configuration files"
                    echo " "
                    echo "if no arguments specified - use interactive mode"
                    echo " "
                    echo "options:"
                    echo "-h, --help          Show brief help"
                    echo "--new-version       Specify new revision"
                    echo "--pom-location      Main POM file location"
                    echo "--no-schemas        Do not upgrade the schemas"
                    exit 1
                    ;;
            --new-version*)
                    NEWVERSION="$2"
                    shift
                    ;;
            --pom-location*)
                    POMLOCATION="$2"
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

DIR=$(dirname $0)
BASEDIR=$(readlink -f "$DIR")
if [ -z "$POMLOCATION" ]; then
	POMLOCATION='pom.xml'
fi
VERSION=$(xsltproc "$BASEDIR/extract-gav.xslt" "$POMLOCATION" | cut -d: -f4)

echo "Pom file location: $POMLOCATION"
echo "Current version: $VERSION"
echo -n "New version: "
if [ -z "$NEWVERSION" ]; then
    read -r NEWVERSION
fi

echo "Changing from $VERSION to $NEWVERSION"
while IFS= read -r -d '' POM
do
   sed -i "s/<version>$VERSION<\/version>/<version>$NEWVERSION<\/version>/g" "$POM"
   git add "$POM"
done < <(find . -name 'pom.xml' -type f -print0)

OLDSCHEMAMAJOR=$(echo "$VERSION"|cut -d. -f1)
OLDSCHEMAMINOR=$(echo "$VERSION"|cut -d. -f2)
OLDSCHEMAVERSION=$(echo "$VERSION"|cut -d. -f1,2)
OLDVERSION=$(echo "$VERSION"|cut -d. -f1,2,3)
NEWSCHEMAMAJOR=$(echo "$NEWVERSION"|cut -d. -f1)
NEWSCHEMAMINOR=$(echo "$NEWVERSION"|cut -d. -f2)
NEWSCHEMAVERSION=$(echo "$NEWVERSION"|cut -d. -f1,2)

sed -i "s/<version.infinispan>$VERSION<\/version.infinispan>/<version.infinispan>$NEWVERSION<\/version.infinispan>/g" build/configuration/pom.xml
sed -i "s/<infinispan.codename>.*<\/infinispan.codename>/<infinispan.codename>N\/A<\/infinispan.codename>/g" build/configuration/pom.xml
sed -i "s/<infinispan.base.version>.*<\/infinispan.base.version>/<infinispan.base.version>$NEWMAJOR.$NEWMINOR</infinispan.base.version>/g" build/configuration/pom.xml
sed -i "s/<infinispan.old.version>.*<\/infinispan.old.version>/<infinispan.old.version>$OLDVERSION</infinispan.old.version>/g" build/configuration/pom.xml

if [ "$OLDSCHEMAVERSION" != "$NEWSCHEMAVERSION" ] && [ "$PROCESS_SCHEMAS" = true ] ;  then
    echo "Current schema: $OLDSCHEMAVERSION"
    echo "New schema:     $NEWSCHEMAVERSION"

    # Set master schema version
    sed -i "s/<infinispan.base.version>$OLDSCHEMAVERSION<\/infinispan.base.version>/<infinispan.base.version>$NEWSCHEMAVERSION<\/infinispan.base.version>/g" pom.xml
    # Set the codename to WIP
    sed -E -i "s/<infinispan.codename>[^<]+<\/infinispan.codename>/<infinispan.codename>WIP<\/infinispan.codename>/g" pom.xml
    git add pom.xml

    # Create new test configurations
    copyXMLs . "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"
    copyXMLs _ "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"
    addNamespace "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"

    # Update the server configurations
    CONFIGS=$(find server/runtime/src/main/server/server/conf/ -name '*.xml' -type -f)
    for CONFIG in $CONFIGS; do
         rewriteXML "$CONFIG" "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"
    done

    # Update the server test configurations. Ignore configuration files that start with a number
    CONFIGS=$(find server -regex '.*/src/test/resources/[^0-9].*xml' -type f)
    for CONFIG in $CONFIGS; do
         rewriteXML "$CONFIG" "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"
    done

    # Update the server configurations
    CONFIGS=$(find integrationtests/server-integration/server-integration-commons/ -name '*.xml' -type f)
    for CONFIG in $CONFIGS; do
         rewriteXML "$CONFIG" "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"
    done


    # Rewrite the XSDs
    rewriteXSDs . "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"
    rewriteXSDs _ "$OLDSCHEMAMAJOR" "$OLDSCHEMAMINOR" "$NEWSCHEMAMAJOR" "$NEWSCHEMAMINOR"

    echo "DONE!"
fi

