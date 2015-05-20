#############################################################################
#                                                                          ##
#    WildFly Startup Script for starting the standalone server             ##
#                                                                          ##
#############################################################################

$PROGNAME=$MyInvocation.MyCommand.Name
$RESOLVED_JBOSS_HOME = (Get-ChildItem $MyInvocation.MyCommand.Path).Directory.Parent.FullName


# A collection of functions that are used by the other scripts

Function Set-Env {
  $key = $args[0]
  $value = $args[1]
  Set-Content -Path env:$key -Value $value
}

Function Get-Env {
  $key = $args[0]
  if( Test-Path env:$key ) {
    return (Get-ChildItem env:$key).Value
  }
  return $args[1]
}

Function Get-String {
  $value = ''
  foreach($k in $args) {
    $value += $k
  }
  return $value
}

# Setup JBOSS_HOME
if( Test-Path env:JBOSS_HOME) {
  $SANITIZED_JBOSS_HOME = (Get-Item env:JBOSS_HOME).FullName
  if( $SANITIZED_JBOSS_HOME -ne $RESOLVED_JBOSS_HOME) {
    echo "WARNING JBOSS_HOME may be pointing to a different installation - unpredictable results may occur."
    echo ""
  }
  $JBOSS_HOME=$SANITIZED_JBOSS_HOME
} else {
    # get the full path (without any relative bits)
    $JBOSS_HOME=$RESOLVED_JBOSS_HOME
}


# Read an optional running configuration file
$STANDALONE_CONF_FILE = $JBOSS_HOME + '\bin\standalone.conf.ps1'
$STANDALONE_CONF_FILE = Get-Env RUN_CONF $STANDALONE_CONF_FILE
. $STANDALONE_CONF_FILE


# Setup the JVM
if (!(Test-Path env:JAVA)) {
  if( Test-Path env:JAVA_HOME) {
    $JAVA = (Get-ChildItem env:JAVA_HOME).Value + "\bin\java"
  } else {
    $JAVA = 'java'
  }
}

# determine the default module path, if not set
$JBOSS_MODULEPATH = Get-Env JBOSS_MODULEPATH $JBOSS_HOME\modules

# determine the default base dir, if not set
$JBOSS_BASE_DIR = Get-Env JBOSS_BASE_DIR $JBOSS_HOME\standalone

# determine the default log dir, if not set
$JBOSS_LOG_DIR = Get-Env JBOSS_LOG_DIR $JBOSS_BASE_DIR\log

# determine the default configuration dir, if not set
$JBOSS_CONFIG_DIR = Get-Env JBOSS_CONFIG_DIR $JBOSS_BASE_DIR\configuration

# Determine the default JBoss PID file
$JBOSS_PIDFILE = Get-Env JBOSS_PIDFILE ''

$PRESERVE_JAVA_OPTS = Get-Env PRESERVE_JAVA_OPTS false
$PREPEND_JAVA_OPTS = Get-Env PREPEND_JAVA_OPTS false


if($PRESERVE_JAVA_OPTS -ne 'true') {
  $JVM_OPTVERSION = ''

  # Get the Java options as a string
  $JAVA_OPTS_STRING = Get-String $JAVA_OPTS

  if($JAVA_OPTS_STRING.Contains('-d64')) {
    $JVM_OPTVERSION = '-d64'
  } elseif ($JAVA_OPTS_STRING.Contains('-d32')) {
    $JVM_OPTVERSION = '-d32'
  }
}


# Display our environment
echo "================================================================================="
echo ""
echo "  WildFly Bootstrap Environment"
echo ""
echo "  JBOSS_HOME: $JBOSS_HOME"
echo ""
echo "  JBOSS_BASE_DIR: $JBOSS_BASE_DIR"
echo ""
echo "  JAVA: $JAVA"
echo ""
echo "  JAVA_OPTS: $JAVA_OPTS"
echo ""
echo "  JBOSS_MODULEPATH: $JBOSS_MODULEPATH"
echo ""
echo "================================================================================="
echo ""

$backgroundProcess = Get-Env LAUNCH_JBOSS_IN_BACKGROUND 'false'

  $PROG_ARGS = @()
  $PROG_ARGS += $JAVA_OPTS
  $PROG_ARGS += "-Dorg.jboss.boot.log.file=$JBOSS_LOG_DIR/boot.log"
  $PROG_ARGS += "-Dlogging.configuration=file:$JBOSS_CONFIG_DIR/logging.properties"
  $PROG_ARGS += "-Djboss.home.dir=$JBOSS_HOME"
  $PROG_ARGS += "-jar"
  $PROG_ARGS += "$JBOSS_HOME\jboss-modules.jar"
  $PROG_ARGS += "-mp"
  $PROG_ARGS += "$JBOSS_MODULEPATH"
  $PROG_ARGS += "org.jboss.as.standalone"
  $PROG_ARGS += $ARGS

if($JBOSS_PIDFILE -ne '') {
  # Create the file if it doesn't exist
  if(Test-Path $JBOSS_PIDFILE) {
    throw "Looks like a server process is already running. If it isn't then, remove the $JBOSS_PIDFILE and try again"
    return
  }

  New-Item $JBOSS_PIDFILE -type file
}

if($backgroundProcess -eq 'true') {

  # Export the Java program so that the background job can pick that up.
  Set-Env JB_JAVA_EXE $JAVA
  $j = Start-Job -ScriptBlock { & $env:JB_JAVA_EXE $ARGS } -ArgumentList $PROG_ARGS

}
else {
  & $JAVA $PROG_ARGS
}

if($JBOSS_PIDFILE -ne '') {
  if(Test-Path $JBOSS_PIDFILE) {
    Remove-Item $JBOSS_PIDFILE
  }
}
