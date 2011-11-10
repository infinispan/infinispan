#!/bin/bash

add_to_classpath()
{
  DIR=${1}
  for i in ${DIR}/*.jar ; do
    CP=${CP}:${i}
  done
}

# OS specific support.
cygwin=false;
darwin=false;
mingw=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  MINGW*) mingw=true;;
  Darwin*) darwin=true
           if [ -z "$JAVA_VERSION" ] ; then
             JAVA_VERSION="CurrentJDK"
           else
             echo "Using Java version: $JAVA_VERSION"
           fi
           if [ -z "$JAVA_HOME" ] ; then
             JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JAVA_VERSION}/Home
           fi
           ;;
esac

if [ -z "$JAVA_HOME" ] ; then
  if [ -r /etc/gentoo-release ] ; then
    JAVA_HOME=`java-config --jre-home`
  fi
fi

# For Cygwin, ensure paths are in UNIX format before anything is touched
if $cygwin ; then
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
fi

# For Migwn, ensure paths are in UNIX format before anything is touched
if $mingw ; then
  [ -n "$JAVA_HOME" ] &&
    JAVA_HOME="`(cd "$JAVA_HOME"; pwd)`"
fi

if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD="`which java`"
  fi
fi

if [ ! -x "$JAVACMD" ] ; then
  echo "Error: JAVA_HOME is not defined correctly."
  echo "  We cannot execute $JAVACMD"
  exit 1
fi

DIRNAME=`dirname $0`

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/etc:${ISPN_HOME}/etc/config-samples/distexec-demo


add_to_classpath ${ISPN_HOME}
add_to_classpath ${ISPN_HOME}/lib
add_to_classpath ${ISPN_HOME}/modules/demos/distexec
add_to_classpath ${ISPN_HOME}/modules/demos/distexec/lib

if $cygwin; then
   # Turn paths into Windows style for Cygwin
   CP=`cygpath -wp ${CP}`
   LOG4J_CONFIG=`cygpath -w ${ISPN_HOME}/etc/log4j.xml`
else
   LOG4J_CONFIG=${ISPN_HOME}/etc/log4j.xml
fi

JVM_PARAMS="${JVM_PARAMS} -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${LOG4J_CONFIG}"

# RHQ monitoring options
#JVM_PARAMS="$JVM_PARAMS -Dcom.sun.management.jmxremote.ssl=false"
#JVM_PARAMS="$JVM_PARAMS -Dcom.sun.management.jmxremote.authenticate=false"
#JVM_PARAMS="$JVM_PARAMS -Dcom.sun.management.jmxremote.port=6996"

# Sample JPDA settings for remote socket debuging
#JVM_PARAMS="$JVM_PARAMS -Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"
#echo Classpath is $CP

${JAVACMD} -cp ${CP} ${JVM_PARAMS} org.infinispan.demo.distexec.PiApproximationDemo "$@"
