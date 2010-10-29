#!/bin/bash

DIRNAME=`dirname $0`

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

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/etc:${ISPN_HOME}/etc/config-samples/ec2-demo

for i in `find ${ISPN_HOME}/modules -name "*.jar"` ; do
  CP=${CP}:${i}
done

for i in `find ${ISPN_HOME}/lib -name "*.jar"` ; do
  CP=${CP}:${i}
done

CP=${ISPN_HOME}/infinispan-core.jar:$CP

if [ ! -e ${ISPN_HOME}/etc/Amazon-TestData/influenza_aa.dat ]; then
	gunzip ${ISPN_HOME}/etc/Amazon-TestData/influenza_aa.dat.gz > /dev/null
fi

JVM_PARAMS="${JVM_PARAMS} -Xmx512m -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${ISPN_HOME}/etc/log4j.xml"
JVM_PARAMS="${JVM_PARAMS} -DCFGPath=${ISPN_HOME}/etc/config-samples/ec2-demo/"

DEMO_ARGS="${DEMO_ARGS} -c ${ISPN_HOME}/etc/config-samples/ec2-demo/infinispan-ec2-config.xml"
DEMO_ARGS="${DEMO_ARGS} -p ${ISPN_HOME}/etc/Amazon-TestData/influenza_aa.dat"

if $cygwin; then
   # Turn paths into Windows style for Cygwin
   CP=`cygpath -wp ${CP}`
fi

${JAVACMD} -cp ${CP} ${JVM_PARAMS} org.infinispan.ec2demo.InfinispanFluDemo ${DEMO_ARGS}
