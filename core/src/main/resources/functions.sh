#!/bin/bash
# Provides various common functions for the shell scripts provided by
# Infinispan

# Determine ISPN_HOME
if [[ "x${ISPN_HOME}" == "x" ]]; then
  # get the full path (without any relative bits)
  DIRNAME=`dirname "$0"`
  ISPN_HOME=`cd "$DIRNAME"/..; pwd`
  unset DIRNAME
fi

# Determins the OS
CYGWIN=false
DARWIN=false
MINGW=false
case "`uname`" in
  CYGWIN*) CYGWIN=true;;
  MINGW*)  MINGW=true;;
  Darwin*) DARWIN=true;;
esac

# Functions

CLASSPATH=""
function add_classpath() {
  if [[ -z "$ADD_CLASSPATH_CALL_DEPTH" ]]; then
  	ADD_CLASSPATH_CALL_DEPTH=0
  fi

  if [[ $ADD_CLASSPATH_CALL_DEPTH -eq 0 ]]; then
    OLD_IFS=$IFS
    IFS=$'\n'
  fi

  ((ADD_CLASSPATH_CALL_DEPTH ++))

  while [[ $# -gt 0 ]]; do
    E="$1"
    shift
    if [[ ! -e "$E" ]]; then
      echo "Skipping non-existing classpath element: $E"
      continue
    fi

	if [[ -d "$E" ]]; then
      if [[ "$E" =~ /etc($|/) ]]; then
      	# Do not recurse into the configuration directory
        CLASSPATH="$CLASSPATH:$E"
      else
        add_classpath "$E"/*
      fi
    elif [[ -f "$E" && "$E" =~ \.([Jj][Aa][Rr]|[Zz][Ii][Pp])$ ]]; then
      CLASSPATH="$CLASSPATH:$E"
    fi

    if [[ "$CLASSPATH" =~ ^: ]]; then
      CLASSPATH="${CLASSPATH/:/}"
    fi
  done

  ((ADD_CLASSPATH_CALL_DEPTH --))

  if [[ $ADD_CLASSPATH_CALL_DEPTH -eq 0 ]]; then
    IFS=$OLD_IFS
    unset OLD_IFS
    unset ADD_CLASSPATH_CALL_DEPTH
  fi
}

JVM_ARGS=()
function add_jvm_args() {
  while [[ $# -gt 0 ]]; do
	JVM_ARGS=( "${JVM_ARGS[@]}" "$1" )
	shift
  done
}

PROGRAM_ARGS=()
function add_program_args() {
  while [[ $# -gt 0 ]]; do
	PROGRAM_ARGS=( "${PROGRAM_ARGS[@]}" "$1" )
	shift
  done
}

function start() {
  if [[ $# -ne 1 ]]; then
  	echo "Usage: start <main class name>"
  	exit 1
  fi

  # Get the main class and program arguments
  MAIN_CLASS="$1"

  # Find JAVA_HOME and JAVACMD
  # Mac OS X
  if $DARWIN; then
    if [[ -z "$JAVA_VERSION" ]]; then
      JAVA_VERSION="CurrentJDK"
    else
      echo "Using Java version: $JAVA_VERSION"
    fi
    if [[ -z "$JAVA_HOME" ]]; then
      JAVA_HOME=/System/Library/Frameworks/JavaVM.framework/Versions/${JAVA_VERSION}/Home
    fi
  fi

  if [[ -z "$JAVA_HOME" ]]; then
  	# JAVA_HOME not found yet
  	# If Gentoo Linux, we might be able to get it using java-config.
  	if [[ -r /etc/gentoo-release ]]; then
      JAVA_HOME=`java-config --jre-home`
    fi
  else
  	# JAVA_HOME found - convert to the native path if running on Windows.
    if $CYGWIN; then
      JAVA_HOME=`cygpath --unix "$JAVA_HOME"`
    elif $MINGW; then
      JAVA_HOME="`(cd "$JAVA_HOME"; pwd)`"
    fi
  fi

  if [[ -z "$JAVACMD" ]]; then
    if [[ -n "$JAVA_HOME" ]]; then
      if [[ -x "$JAVA_HOME/jre/sh/java" ]]; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVACMD="$JAVA_HOME/jre/sh/java"
      else
        JAVACMD="$JAVA_HOME/bin/java"
      fi
    else
      JAVACMD="`which java`"
    fi
  fi

  if [[ ! -x "$JAVACMD" ]] ; then
    echo "Error: JAVA_HOME is not defined correctly."
    echo "  We cannot execute $JAVACMD"
    exit 1
  fi
  
  # Turn paths into Windows style for Cygwin
  if $CYGWIN; then
    CLASSPATH=`cygpath -wp "${CLASSPATH}"`
  fi

  # Build the command and run:
  START_ARGS=( "$JAVACMD" "-cp" "$CLASSPATH" )
  if [[ ${#JVM_ARGS[@]} -gt 0 ]]; then
  	START_ARGS=( "${START_ARGS[@]}" "${JVM_ARGS[@]}" )
  fi

  # Log4J config path needs path conversion in Cygwin
  if $CYGWIN; then
    LOG4J_CONFIG=`cygpath -w "${ISPN_HOME}/etc/log4j.xml"`
  else
    LOG4J_CONFIG=${ISPN_HOME}/etc/log4j.xml
  fi 
  START_ARGS=( "${START_ARGS[@]}" "-Dlog4j.configuration=file:///$LOG4J_CONFIG" )

  # Main class and its arguments
  START_ARGS=( "${START_ARGS[@]}" "$MAIN_CLASS" )
  if [[ ${#PROGRAM_ARGS[@]} -gt 0 ]]; then
  	START_ARGS=( "${START_ARGS[@]}" "${PROGRAM_ARGS[@]}" )
  fi

  # Uncomment this to check if the arguments were processed correctly.
  #i=0
  #len=${#START_ARGS[@]}
  #while [[ $i -lt $len ]]; do
  #	echo "$i: ${START_ARGS[$i]}"
  #  ((i ++))
  #done

  "${START_ARGS[@]}"
}

