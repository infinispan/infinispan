#!/bin/bash

ROOTFS="$1"
JDK="$2"
OPENJDK_PACKAGE="$3"
JDK_MAJOR="${4:-25}"
JDK_CHANNEL="${5:-ga}"

ARCHITECTURE=$( uname -m )
case "$ARCHITECTURE" in
  x86_64)
    ARCHITECTURE=x64
    ;;
esac

JVM_PATH="$ROOTFS"/usr/lib/jvm
JAVA_HOME="$JVM_PATH"/default-java
WORK="$(mktemp -d)"
STAGE="$WORK/stage"
mkdir -p "$STAGE"
# Create the standard RPM topdir layout, so that rpmbuild works on all RPM implementations
mkdir -p "$WORK/BUILD" "$WORK/BUILDROOT" "$WORK/RPMS" "$WORK/SOURCES" "$WORK/SPECS" "$WORK/SRPMS"
trap 'rm -rf "$WORK"' EXIT

read -r -d '' JDK_TRIM_FILES << EOF
bin/jar
bin/jarsigner
bin/javac
bin/javadoc
bin/javap
bin/jconsole
bin/jdb
bin/jdepscan
bin/jhsdb
bin/jimage
bin/jinfo
bin/jlink
bin/jmod
bin/jnativescan
bin/jpackage
bin/jrunscript
bin/jshell
bin/jwebserver
bin/serialver
lib/ct.sym
lib/libawt_xawt.so
lib/libjawt.so
lib/libsplashscreen.so
lib/src.zip
include/*
EOF

finalize_tarball_jdk() {
    # $1: the directory of the extracted JDK tree under $STAGE
    TREE="$1"
    # Remove some unnecessary files
    for FILE in $JDK_TRIM_FILES; do
        rm -f "$TREE/$FILE"
    done
    # Wrap the JDK tree in a minimal RPM and install it into the root filesystem.
    # The RPM database survives into the final image, so container scanners
    # (e.g. Trivy) can attribute the JDK and track its CVEs.
    RELEASE_FILE="$TREE/release"
    [ -f "$RELEASE_FILE" ] || { echo "JDK release file not found" >&2; exit 1; }
    # Extract the release metadata as data, without executing the file
    release_field() {
        sed -n "s/^$1=//p" "$RELEASE_FILE" | head -n 1 | sed -e 's/^"//' -e 's/"$//'
    }
    JAVA_VERSION=$(release_field JAVA_VERSION)
    IMPLEMENTOR=$(release_field IMPLEMENTOR)
    IMPLEMENTOR_VERSION=$(release_field IMPLEMENTOR_VERSION)
    SEMANTIC_VERSION=$(release_field SEMANTIC_VERSION)
    SOURCE_URL=$(release_field SOURCE_URL)
    [ -n "$SOURCE_URL" ] || SOURCE_URL=$(release_field SOURCE_REPO)
    [ -n "$JAVA_VERSION" ] || { echo "JDK release file has no JAVA_VERSION" >&2; exit 1; }
    # Derive the package identity from the JDK distribution, so that scanners
    # attribute the package and its vulnerabilities to the right vendor
    if ! [[ "$IMPLEMENTOR" =~ ^[A-Za-z\ ]+$ ]]; then
        IMPLEMENTOR="OpenJDK"
    fi
    VENDOR=$(echo "$IMPLEMENTOR" | tr '[:upper:]' '[:lower:]' | tr -s ' ' '-')
    RPM_NAME="${VENDOR}-openjdk"
    # Derive the build number from the release file (SEMANTIC_VERSION, e.g. 25.0.5.1+2, or
    # IMPLEMENTOR_VERSION, e.g. Temurin-25.0.5.1+2), falling back to the tree name;
    # vendor builds without a recognizable build number default to 1
    VERSION_AND_BUILD="${SEMANTIC_VERSION:-${IMPLEMENTOR_VERSION##*-}}"
    case "$VERSION_AND_BUILD" in
        *+*) JDK_BUILD="${VERSION_AND_BUILD#*\+}" ;;
        *)   JDK_BUILD="$(basename "$TREE")"; JDK_BUILD="${JDK_BUILD#*\+}" ;;
    esac
    if [ -z "$JDK_BUILD" ] || ! [[ "$JDK_BUILD" =~ ^[A-Za-z0-9.]+$ ]]; then
        JDK_BUILD="1"
    fi
    # Map hyphenated early-access versions (e.g. 21-ea) to underscores, which
    # are not valid in an RPM version
    RPM_VERSION="${JAVA_VERSION//-/_}"
    # The spec must live outside the build root, or check-files flags it as
    # an installed-but-unpackaged file
    SPEC_URL=""
    [ -n "$SOURCE_URL" ] && SPEC_URL="URL: ${SOURCE_URL}"
    cat > "$WORK/temurin.spec" << SPEC
Name: ${RPM_NAME}
Version: ${RPM_VERSION}
Release: ${JDK_BUILD}
Summary: ${IMPLEMENTOR} build of OpenJDK ${JAVA_VERSION}
License: GPLv2 with Classpath Exception
${SPEC_URL}

%description
${IMPLEMENTOR} build of OpenJDK ${JAVA_VERSION} (build ${JDK_BUILD}).

%files
/usr/lib/jvm/$(basename "$TREE")
SPEC
    rpmbuild -bb --buildroot "$STAGE" --define "_topdir $WORK" "$WORK/temurin.spec" || { echo "Failed to build the RPM" >&2; exit 1; }
    RPM_FILE=$(find "$WORK/RPMS" -name "${RPM_NAME}-${RPM_VERSION}-${JDK_BUILD}*.rpm" | head -n 1)
    [ -n "$RPM_FILE" ] || { echo "Generated RPM not found" >&2; exit 1; }
    # Install into the root filesystem's own package database, so the package
    # is visible to scanners in the final image
    rpm --root "$ROOTFS" -i --nodeps "$RPM_FILE" || { echo "Failed to install the RPM" >&2; exit 1; }
    # Symlink the default JDK directory
    mkdir -p "$JVM_PATH" || exit 1
    pushd "$JVM_PATH" || exit 1
    ln -s "$(basename "$TREE")" default-java || { echo "Failed to create the default-java link" >&2; exit 1; }
    popd || exit 1
    # Create links to JDK executables in /usr/bin
    [ -d "$JVM_PATH/default-java/bin" ] || { echo "default-java/bin not found" >&2; exit 1; }
    mkdir -p "$ROOTFS"/usr/bin || exit 1
    pushd "$ROOTFS"/usr/bin || exit 1
    ln -sv ../lib/jvm/default-java/bin/* . || { echo "Failed to link the JDK executables" >&2; exit 1; }
    popd || exit 1
}

if [ "$OPENJDK_PACKAGE" != "" ]; then
    # The JDK has been installed by the package manager. We just need to set up a link if necessary
    pushd "$JVM_PATH" || exit 1
    if [ ! -f default-java ]; then
        ln -sv java-*-openjdk default-java || { echo "Failed to create the default-java link" >&2; exit 1; }
    fi
    popd || exit 1
else
    mkdir -p "$STAGE/usr/lib/jvm"
    JDK_MIME_TYPE=$(file --mime-type "$JDK" | cut -f 2 -d ' ')
    case "$JDK_MIME_TYPE" in
        application/gzip)
            echo "Installing JDK from $JDK"
            tar xzvf "$JDK" -C "$STAGE/usr/lib/jvm" || { echo "Failed to extract $JDK" >&2; exit 1; }
            TREE="$(find "$STAGE/usr/lib/jvm" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
            [ -n "$TREE" ] || exit 1
            finalize_tarball_jdk "$TREE"
            ;;
        application/x-rpm)
            echo "Installing JDK from $JDK"
            rpm --root "$ROOTFS" -i --nodeps "$JDK" || { echo "Failed to install $JDK" >&2; exit 1; }
            # Create links to JDK executables in /usr/bin
            mkdir -p "$ROOTFS"/usr/bin || exit 1
            pushd "$ROOTFS"/usr/bin || exit 1
            ln -sv ../lib/jvm/java*/bin/* . || { echo "Failed to link the JDK executables" >&2; exit 1; }
            popd || exit 1
            ;;
        inode/x-empty | empty)
            echo "Downloading Temurin OpenJDK ${JDK_MAJOR} (${JDK_CHANNEL}) for ${ARCHITECTURE}"
            wget -qO "$WORK/jdk.tar.gz" "https://api.adoptium.net/v3/binary/latest/${JDK_MAJOR}/${JDK_CHANNEL}/linux/${ARCHITECTURE}/jdk/hotspot/normal/eclipse" || { echo "Failed to download the JDK" >&2; exit 1; }
            tar xzvf "$WORK/jdk.tar.gz" -C "$STAGE/usr/lib/jvm" || { echo "Failed to extract the downloaded JDK" >&2; exit 1; }
            rm -f "$WORK/jdk.tar.gz"
            TREE="$(find "$STAGE/usr/lib/jvm" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
            [ -n "$TREE" ] || exit 1
            finalize_tarball_jdk "$TREE"
            ;;
        *)
            echo "Don't know how to handle JDK archive of type $JDK_MIME_TYPE"
            exit 1
            ;;
    esac
fi
