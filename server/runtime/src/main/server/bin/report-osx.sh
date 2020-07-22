#!/bin/sh

PROGNAME=$(basename "$0")
DIRNAME=$(dirname "$0")

count() { echo $#; }

# Get the PID
if [ "x$1" = "x" ]; then
  # There may be more than one
  SERVER_PPID=$(pgrep server.sh)
  PPID_COUNT=$(count $SERVER_PPID)
  if [ "$PPID_COUNT" -gt "1" ]; then
    printf "Multiple processes detected. Specify one of the following PIDs as the first argument:\n %s" "$SERVER_PPID"
    exit 1
  fi
  SERVER_PID=$(pgrep -P $SERVER_PPID)
else
  SERVER_PID=$1
  shift
fi

TMPDIR=$(mktemp -d "infinispan-server.XXXXX")
# Gather information about the system
system_profiler SPSoftwareDataType > "$TMPDIR/os-release"
ifconfig |grep inet > "$TMPDIR/ip-address"
netstat -rn | grep 'default' > "$TMPDIR/ip-route"
netstat -g > "$TMPDIR/ip-maddress"
sysctl -a | grep machdep.cpu > "$TMPDIR/cpuinfo"
vm_stat | perl -ne '/page size of (\d+)/ and $size=$1; /Pages\s+([^:]+)[^\d]+(\d+)/ and printf("%-16s % 16.2f Mi\n", "$1:", $2 * $size / 1048576);' > "$TMPDIR/meminfo"
uname -a > "$TMPDIR/uname"
df -h > "$TMPDIR/df"
lsof -itcp -P -n | grep "LISTEN"  > "$TMPDIR/ss-tcp"
lsof -iudp -P -n > "$TMPDIR/ss-udp"
lsof / > "$TMPDIR/lsof" 2> /dev/null

# Setup ISPN_HOME
ISPN_HOME=$(cd "$DIRNAME/.." > /dev/null; pwd)

# Setup ISPN_ROOT
if [ "x$1" = "x" ]; then
  ISPN_ROOT=$(cd "$ISPN_HOME/server" > /dev/null; pwd)
else
  ISPN_ROOT=$1
  shift
fi

SERVER_INFO=""

mkdir "$TMPDIR/$SERVER_PID"
jstack "$SERVER_PID" > "$TMPDIR/$SERVER_PID/thread-dump"
# Try and get the ISPN_ROOT from the command-line
SERVER_PID_ISPN_ROOT=$(ps -o args "$SERVER_PID"|sed -n 's/.*-s\s\([^[:space:]]*\).*/\1/p')
if [ "x$SERVER_PID_ISPN_ROOT" = "x" ]; then
  SERVER_PID_ISPN_ROOT="$ISPN_ROOT"
fi

SERVER_INFO="-C $SERVER_PID_ISPN_ROOT conf log data/___global.state"
[ -f "$SERVER_PID_ISPN_ROOT/data/caches.xml" ] && SERVER_INFO="$SERVER_INFO data/caches.xml"

tar czf "$TMPDIR.tar.gz" -C "$TMPDIR" . $SERVER_INFO
rm -rf "$TMPDIR"
echo "$TMPDIR.tar.gz"
