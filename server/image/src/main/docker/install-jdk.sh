#!/bin/bash

ARCHITECTURE="$1"
ROOTFS="$2"
JDK="$3"
OPENJDK_PACKAGE="$4"

JVM_PATH="$ROOTFS"/usr/lib/jvm
JAVA_HOME="$JVM_PATH"/default-java
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

if [ "$OPENJDK_PACKAGE" != "" ]; then
  # The JDK has been installed by the package manager. We just need to set up a link if necessary
  pushd "$JVM_PATH" || exit
  if [ ! -f default-java ]; then
    ln -sv java-*-openjdk default-java
  fi
  popd || exit
else
  mkdir -p "$ROOTFS"/usr/lib/jvm
  JDK_MIME_TYPE=$(file --mime-type "$JDK" | cut -f 2 -d ' ')
  case "$JDK_MIME_TYPE" in
    application/gzip)
      echo "Installing JDK from $JDK"
      tar xzvf "$JDK" -C "$JVM_PATH"
      # Symlink the default JDK directory
      pushd "$JVM_PATH" || exit
      ln -s j* default-java
      popd || exit
      # Remove some unnecessary files
      for FILE in $JDK_TRIM_FILES; do
        rm -f "$JAVA_HOME/$FILE"
      done
      # Create links to JDK executables in /usr/bin
      pushd "$ROOTFS"/usr/bin || exit
      ln -sv ../lib/jvm/java/bin/* .
      popd || exit
      ;;
    application/x-rpm)
      echo "Installing JDK from $JDK"
      rpm -i --relocate "/"="$ROOTFS" --badreloc --nodeps "$JDK"
      # Create links to JDK executables in /usr/bin
      pushd "$ROOTFS"/usr/bin || exit
      ln -sv ../lib/jvm/java*/bin/* .
      popd || exit
      ;;
    inode/x-empty | empty)
      case "$ARCHITECTURE" in
        amd64)
          ARCHITECTURE=x64
          ;;
        arm64)
          ARCHITECTURE=aarch64
          ;;
      esac
      echo "Downloading Temurin OpenJDK for ${ARCHITECTURE}"
      wget -qO /tmp/jdk https://api.adoptium.net/v3/binary/latest/25/ga/linux/"${ARCHITECTURE}"/jdk/hotspot/normal/eclipse
      tar xzvf /tmp/jdk -C "$JVM_PATH"
      # Symlink the default JDK directory
      pushd "$JVM_PATH" || exit
      ln -s j* default-java
      popd || exit
      # Remove some unnecessary files
      for FILE in $JDK_TRIM_FILES; do
        rm -f "$JAVA_HOME/$FILE"
      done
      # Create links to JDK executables in /usr/bin
      pushd "$ROOTFS"/usr/bin || exit
      ln -sv ../lib/jvm/java/bin/* .
      popd || exit
      ;;
    *)
      echo "Don't know how to handle JDK archive of type $JDK_MIME_TYPE"
      exit 1
      ;;
  esac
fi
