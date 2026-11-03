#!/bin/bash
#
# Runs a Maven command inside Podman's rootless network namespace so that the server test suite can reach
# containers by their bridge IP address. The "podman" Maven profile is added automatically.
#
# Usage: bin/run-in-podman.sh mvn verify -pl server/tests -Dit.test=MyTestIT
#        bin/run-in-podman.sh ./mvnw verify -pl server/rollingupgradetests -Dit.test=ClusteredRollingUpgradeIT ...
#
# See "Running the server tests with Podman" in the Contributing Guide (documentation/src/main/asciidoc/topics/contributing/tests.adoc).
#
set -e

if [ $# -eq 0 ]; then
   echo "Usage: $0 <mvn|mvnw|path/to/mvn> [maven arguments...]" >&2
   exit 1
fi

if ! command -v podman >/dev/null 2>&1; then
   echo "podman was not found on the PATH" >&2
   exit 1
fi

SOCKET="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/podman/podman.sock"
if [ ! -S "$SOCKET" ]; then
   echo "Podman socket $SOCKET not found. Start it with: systemctl --user enable --now podman.socket" >&2
   exit 1
fi

MVN="$1"
shift
ARGS=("$@")

# Inside "podman unshare" we run as uid 0, so Maven would look in /root/.m2 unless told otherwise
EXTRA=("-Duser.home=$HOME")
case " ${ARGS[*]} " in
   *" -Dmaven.repo.local="*) ;;
   *) EXTRA+=("-Dmaven.repo.local=$HOME/.m2/repository") ;;
esac

# Add the podman profile unless the caller already activated it (-Pa,podman / -P a,podman / --activate-profiles=podman)
has_podman_profile=false
profiles=""
prev=""
for arg in "${ARGS[@]}"; do
   case "$arg" in
      -P?*) profiles="$profiles,${arg#-P}" ;;
      --activate-profiles=*) profiles="$profiles,${arg#--activate-profiles=}" ;;
      *) if [ "$prev" = "-P" ] || [ "$prev" = "--activate-profiles" ]; then profiles="$profiles,$arg"; fi ;;
   esac
   prev="$arg"
done
case ",$profiles," in
   *,podman,*) has_podman_profile=true ;;
esac
if [ "$has_podman_profile" = false ]; then
   EXTRA+=("-Ppodman")
fi

exec podman unshare --rootless-netns "$MVN" "${EXTRA[@]}" "${ARGS[@]}"
