#!/bin/sh
#
# Infinispan Server standalone control script
#
# chkconfig: - 80 20
# description: Infinispan Server
# processname: standalone
# pidfile: /var/run/infinispan-server/infinispan-server-standalone.pid
# config: /etc/infinispan-server/infinispan-server.conf

# Source function library.
. /etc/init.d/functions

# Load Java configuration.
[ -r /etc/java/java.conf ] && . /etc/java/java.conf
export JAVA_HOME

prog='infinispan-server'

# Load Infinispan Server init.d configuration.
if [ -z "$ISPN_SERVER_CONF" ]; then
  ISPN_SERVER_CONF="/etc/$prog/$prog.conf"
fi

[ -r "$ISPN_SERVER_CONF" ] && . "${ISPN_SERVER_CONF}"

# Set defaults.

if [ -z "$ISPN_SERVER_USER" ]; then
  ISPN_SERVER_USER=root
fi

if [ -z "$ISPN_SERVER_HOME" ]; then
  ISPN_SERVER_HOME="/usr/share/$prog"
fi
export ISPN_SERVER_HOME

if [ -z "$ISPN_SERVER_PIDFILE" ]; then
  ISPN_SERVER_PIDFILE="/var/run/$prog/$prog-standalone.pid"
fi
export ISPN_SERVER_PIDFILE

if [ -z "$ISPN_SERVER_CONSOLE_LOG" ]; then
  ISPN_SERVER_CONSOLE_LOG="/var/log/$prog/console.log"
fi

if [ -z "$STARTUP_WAIT" ]; then
  STARTUP_WAIT=30
fi

if [ -z "$SHUTDOWN_WAIT" ]; then
  SHUTDOWN_WAIT=30
fi

if [ -z "$ISPN_SERVER_CONFIG" ]; then
  ISPN_SERVER_CONFIG=standalone.xml
fi
 
if [ -z "$ISPN_SERVER_RUN_CONF" ]; then
  ISPN_SERVER_RUN_CONF=""
fi

if [ -z "$ISPN_SERVER_SCRIPT" ]; then
  ISPN_SERVER_SCRIPT=standalone.sh
fi
 
if [[ "$ISPN_SERVER_SCRIPT"=~"^[^/]" ]]; then
  ISPN_SERVER_SCRIPT=$ISPN_SERVER_HOME/bin/$ISPN_SERVER_SCRIPT
fi

start() {
  echo -n "Starting $prog: "
  if [ -f $ISPN_SERVER_PIDFILE ]; then
    read ppid < $ISPN_SERVER_PIDFILE
    if [ `ps --pid $ppid 2> /dev/null | grep -c $ppid 2> /dev/null` -eq '1' ]; then
      echo -n "$prog is already running"
      failure
      echo
      return 1 
    else
      rm -f $ISPN_SERVER_PIDFILE
    fi
  fi
  mkdir -p $(dirname $ISPN_SERVER_CONSOLE_LOG)
  cat /dev/null > $ISPN_SERVER_CONSOLE_LOG

  mkdir -p $(dirname $ISPN_SERVER_PIDFILE)
  chown $ISPN_SERVER_USER $(dirname $ISPN_SERVER_PIDFILE) || true

  if [ ! -z "$ISPN_SERVER_USER" ]; then
    if [ -x /etc/rc.d/init.d/functions ]; then
      daemon --user $ISPN_SERVER_USER LAUNCH_JBOSS_IN_BACKGROUND=1 JBOSS_PIDFILE=$ISPN_SERVER_PIDFILE RUN_CONF="$ISPN_SERVER_RUN_CONF" $ISPN_SERVER_SCRIPT -c $ISPN_SERVER_CONFIG 2>&1 > $ISPN_SERVER_CONSOLE_LOG &
    else
      su - $ISPN_SERVER_USER -c "LAUNCH_JBOSS_IN_BACKGROUND=1 JBOSS_PIDFILE=$ISPN_SERVER_PIDFILE RUN_CONF=\"$ISPN_SERVER_RUN_CONF\" $ISPN_SERVER_SCRIPT -c $ISPN_SERVER_CONFIG" 2>&1 > $ISPN_SERVER_CONSOLE_LOG &
    fi
  fi

  count=0
  launched=false

  until [ $count -gt $STARTUP_WAIT ]
  do
    grep 'JBAS015874.*started in' $ISPN_SERVER_CONSOLE_LOG > /dev/null 
    if [ $? -eq 0 ] ; then
      launched=true
      break
    fi 
    sleep 1
    let count=$count+1;
  done
  
  success
  echo
  return 0
}

stop() {
  echo -n $"Stopping $prog: "
  count=0;

  if [ -f $ISPN_SERVER_PIDFILE ]; then
    read kpid < $ISPN_SERVER_PIDFILE
    let kwait=$SHUTDOWN_WAIT

    # Try issuing SIGTERM

    kill -15 $kpid
    until [ `ps --pid $kpid 2> /dev/null | grep -c $kpid 2> /dev/null` -eq '0' ] || [ $count -gt $kwait ]
    do
      sleep 1
      let count=$count+1;
    done

    if [ $count -gt $kwait ]; then
      kill -9 $kpid
    fi
  fi
  rm -f $ISPN_SERVER_PIDFILE
  success
  echo
}

status() {
  if [ -f $ISPN_SERVER_PIDFILE ]; then
    read ppid < $ISPN_SERVER_PIDFILE
    if [ `ps --pid $ppid 2> /dev/null | grep -c $ppid 2> /dev/null` -eq '1' ]; then
      echo "$prog is running (pid $ppid)"
      return 0
    else
      echo "$prog dead but pid file exists"
      return 1
    fi
  fi
  echo "$prog is not running"
  return 3
}

case "$1" in
  start)
      start
      ;;
  stop)
      stop
      ;;
  restart)
      $0 stop
      $0 start
      ;;
  status)
      status
      ;;
  *)
      ## If no parameters are given, print which are avaiable.
      echo "Usage: $0 {start|stop|status|restart|reload}"
      exit 1
      ;;
esac
