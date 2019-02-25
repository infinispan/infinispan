#!/bin/sh
#
# ${infinispan.brand.name} Server control script
#
# chkconfig: - 80 20
# description: ${infinispan.brand.name} Server startup script
# processname: ${infinispan.brand.prefix}-server
# pidfile: /var/run/${infinispan.brand.prefix}-server/${infinispan.brand.prefix}-server.pid
# config: /etc/default/${infinispan.brand.prefix}-server.conf
#

# Source function library.
. /etc/init.d/functions

# Load Java configuration.
[ -r /etc/java/java.conf ] && . /etc/java/java.conf
export JAVA_HOME

if [ -z "$JBOSS_NAME" ]; then
  JBOSS_NAME='${infinispan.brand.prefix}-server'
fi

# Load ${infinispan.brand.name} Server init.d configuration.
if [ -z "$JBOSS_CONF" ]; then
  JBOSS_CONF="/etc/default/$JBOSS_NAME.conf"
fi

if [ -z "$JBOSS_USER" ]; then
  JBOSS_USER='${infinispan.brand.prefix}-server'
fi

[ -r "$JBOSS_CONF" ] && . "${JBOSS_CONF}"

# Set defaults.

if [ -z "$JBOSS_HOME" ]; then
	JBOSS_HOME=/opt/$JBOSS_NAME
fi
export JBOSS_HOME

if [ -z "$JBOSS_PIDFILE" ]; then
	JBOSS_PIDFILE=/var/run/${infinispan.brand.prefix}-server/$JBOSS_NAME.pid
fi
export JBOSS_PIDFILE

if [ -z "$JBOSS_CONSOLE_LOG" ]; then
	JBOSS_CONSOLE_LOG=/var/log/$JBOSS_NAME/console.log
fi

if [ -z "$STARTUP_WAIT" ]; then
	STARTUP_WAIT=30
fi

if [ -z "$SHUTDOWN_WAIT" ]; then
	SHUTDOWN_WAIT=30
fi

if [ -z "$JBOSS_LOCKFILE" ]; then
	JBOSS_LOCKFILE=/var/lock/subsys/$JBOSS_NAME
fi

# Startup mode of ${infinispan.brand.prefix}-server
if [ -z "$JBOSS_MODE" ]; then
	JBOSS_MODE=standalone
fi

if [ -z "$JBOSS_BASE_DIR" ]; then
	JBOSS_BASE_DIR="$JBOSS_HOME/$JBOSS_MODE"
else
	JBOSS_OPTS="$JBOSS_OPTS -Djboss.server.base.dir=$JBOSS_BASE_DIR"
fi

JBOSS_MARKERFILE=$JBOSS_BASE_DIR/tmp/startup-marker

# Startup mode script
if [ "$JBOSS_MODE" = "standalone" ]; then
	JBOSS_SCRIPT=$JBOSS_HOME/bin/standalone.sh
	if [ -z "$JBOSS_CONFIG" ]; then
		JBOSS_CONFIG=standalone.xml
	fi
else
	JBOSS_SCRIPT=$JBOSS_HOME/bin/domain.sh
	if [ -z "$JBOSS_DOMAIN_CONFIG" ]; then
		JBOSS_DOMAIN_CONFIG=domain.xml
	fi
	if [ -z "$JBOSS_HOST_CONFIG" ]; then
		JBOSS_HOST_CONFIG=host.xml
	fi
fi

prog=$JBOSS_NAME
currenttime=$(date +%s%N | cut -b1-13)

start() {
	echo -n "Starting $prog: "
	if ! id "$JBOSS_USER" >/dev/null 2>&1; then
	   echo -n "No user $JBOSS_USER"
   	failure
      echo
   	return 1
	fi
	if [ -f $JBOSS_PIDFILE ]; then
		read ppid < $JBOSS_PIDFILE
		if [ `ps --pid $ppid 2> /dev/null | grep -c $ppid 2> /dev/null` -eq '1' ]; then
			echo -n "$prog is already running"
			failure
      	echo
		   return 1
	   else
		   rm -f $JBOSS_PIDFILE
	   fi
	fi
	mkdir -p $(dirname $JBOSS_CONSOLE_LOG)
	cat /dev/null > $JBOSS_CONSOLE_LOG

	mkdir -p $(dirname $JBOSS_PIDFILE)
	chown $JBOSS_USER $(dirname $JBOSS_PIDFILE) || true

	if [ ! -z "$JBOSS_USER" ]; then
		if [ "$JBOSS_MODE" = "standalone" ]; then
			if [ -r /etc/rc.d/init.d/functions ]; then
			        cd $JBOSS_HOME
				daemon --user $JBOSS_USER LAUNCH_JBOSS_IN_BACKGROUND=1 JBOSS_PIDFILE=$JBOSS_PIDFILE "$JBOSS_SCRIPT -c $JBOSS_CONFIG $JBOSS_OPTS &" >> $JBOSS_CONSOLE_LOG 2>&1
				cd -
			else
				su - $JBOSS_USER -c "cd $JBOSS_HOME; LAUNCH_JBOSS_IN_BACKGROUND=1 JBOSS_PIDFILE=$JBOSS_PIDFILE $JBOSS_SCRIPT -c $JBOSS_CONFIG $JBOSS_OPTS" >> $JBOSS_CONSOLE_LOG 2>&1 &
			fi
		else
			if [ -r /etc/rc.d/init.d/functions ]; then
			        cd $JBOSS_HOME
				daemon --user $JBOSS_USER LAUNCH_JBOSS_IN_BACKGROUND=1 JBOSS_PIDFILE=$JBOSS_PIDFILE "$JBOSS_SCRIPT --domain-config=$JBOSS_DOMAIN_CONFIG --host-config=$JBOSS_HOST_CONFIG $JBOSS_OPTS &" >> $JBOSS_CONSOLE_LOG 2>&1
				cd -
			else
				su - $JBOSS_USER -c "cd $JBOSS_HOME; LAUNCH_JBOSS_IN_BACKGROUND=1 JBOSS_PIDFILE=$JBOSS_PIDFILE $JBOSS_SCRIPT --domain-config=$JBOSS_DOMAIN_CONFIG --host-config=$JBOSS_HOST_CONFIG $JBOSS_OPTS" >> $JBOSS_CONSOLE_LOG 2>&1 &
			fi
		fi
	fi

	count=0
	launched=false

	until [ $count -gt $STARTUP_WAIT ]
	do
		sleep 1
		let count=$count+1;
		if [ -f $JBOSS_MARKERFILE ]; then
			markerfiletimestamp=$(grep -o '[0-9]*' $JBOSS_MARKERFILE) > /dev/null
			if [ "$markerfiletimestamp" -gt "$currenttime" ] ; then
				grep -i 'success:' $JBOSS_MARKERFILE > /dev/null
				if [ $? -eq 0 ] ; then
					launched=true
					break
				fi
			fi
		fi
	done

	if [ "$launched" = "false" ] ; then
		echo "$prog started with errors, please see server log for details"
	fi

	touch $JBOSS_LOCKFILE
	success
	echo
	return 0
}

stop() {
	echo -n $"Stopping $prog: "
	count=0;

	if [ -f $JBOSS_PIDFILE ]; then
		read kpid < $JBOSS_PIDFILE
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
	rm -f $JBOSS_PIDFILE
	rm -f $JBOSS_LOCKFILE
	success
	echo
}

status() {
	if [ -f $JBOSS_PIDFILE ]; then
		read ppid < $JBOSS_PIDFILE
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
		echo "Usage: $0 {start|stop|status|restart}"
		exit 1
		;;
esac
