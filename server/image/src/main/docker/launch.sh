#!/bin/bash
# ===================================================================================
# Entry point for the image which initiates any pre-launch config required before
# executing the server.
# ===================================================================================

tree() {
  find "$1" | sed -e "s/[^-][^\/]*\//  |/g" -e "s/|\([^ ]\)/|-\1/"
}

tailAll() {
  dir=$1
  for f in "$dir"/*; do
    if [ -d "$f" ]; then
      tailAll "$f"
    else
      print "\n$f\n"
      print ">>>>>>>>>>>\n"
      tail -n +1 "$f"
      printf "\n<<<<<<<<<<<\n"
    fi
  done
}

generate_identities_yaml() {
  # If no identities file provided, then use provided user/pass or generate as required
  if [ -z "${IDENTITIES_BATCH}" ]; then
    BATCH=${ISPN_HOME}/server/conf/generated-identities.batch
    if [ -n "${USER}" ] && [ -n "${PASS}" ]; then
      export ISPN_USERNAME="${USER}"
      export ISPN_PASSWORD="${PASS}"
      echo "user create -g admin" > "${BATCH}"
    else
      cat <<EOF > "${BATCH}"
      echo "################################################################################"
      echo "# USER and/or PASS env variables not specified.                                #"
      echo "# Auto generating user and password:                                           #"
      echo "#                                                                              #"
      user create --random --echo -g admin
      echo "#                                                                              #"
      echo "# These credentials should be passed via environment variables when adding     #"
      echo "# new nodes to the cluster to ensure that clients can access the exposed       #"
      echo "# endpoints, on all nodes, using the same credentials.                         #"
      echo "#                                                                              #"
      echo "# For example:                                                                 #"
      echo "#   docker run -e USER=<username> -e PASS=<password>                           #"
      echo "################################################################################"
EOF
    fi
    BATCHES+=("${BATCH}")
  fi
}

generate_content() {
  if [ "${MANAGED_ENV^^}" != "TRUE" ]; then
    generate_identities_yaml
  fi
}

execute_cli() {
  # Use the native CLI if present
  if [[ -x "${ISPN_HOME}/cli" ]]; then
    "${ISPN_HOME}/cli" "$@"
  else
    "${ISPN_HOME}/bin/cli.sh" "$@"
  fi
  return $?
}

# ===================================================================================
# Script Execution
# ===================================================================================

set -e
if [ -n "${DEBUG}" ]; then
  set -x
fi

# Set the umask otherwise created files are not-writable by the group
umask 0002
declare -a BATCHES

# ===================================================================================
# Configuration Initialization
# ===================================================================================

generate_content

if [[ -n ${SERVER_LIBS} ]]; then
  if [[ -n ${SERVER_LIBS_DIR} ]]; then
    SERVER_ROOT="--server-root=\"${SERVER_LIBS_DIR}\""
  fi
  SERVER_LIBS_BATCH=${ISPN_HOME}/server/conf/generated-server-libs.batch
  echo "install --overwrite --server-home=\"${ISPN_HOME}\" ${SERVER_ROOT} ${SERVER_LIBS}" > "${SERVER_LIBS_BATCH}"
  BATCHES+=("${SERVER_LIBS_BATCH}")
fi

# IDENTITIES_BATCH will not be set if IDENTITIES_PATH provided
if [[ -n ${IDENTITIES_BATCH} ]]; then
  BATCHES+=("${IDENTITIES_BATCH}")
fi

# If INIT_CONTAINER is true, then we just run the CLI scripts and return
if [ "${INIT_CONTAINER^^}" == "TRUE" ]; then
  ARGS=""
  for BATCH in "${BATCHES[@]}"; do
    ARGS="${ARGS} -f ${BATCH}"
  done
  if ! execute_cli "$ARGS"; then
    echo "Batch execution failed. Aborting."
    exit 1
  fi
  exit 0
fi

if [ -n "${DEBUG}" ]; then
  set +x
  tree "${ISPN_HOME}"
  tail -n +1 "${ISPN_HOME}"/server/data/*.state || true
  tail -n +1 "${ISPN_HOME}"/server/data/*.xml || true
  tailAll "${ISPN_HOME}"/server/conf
  set -x
fi

# ===================================================================================
# Server Execution
# ===================================================================================

ARGS=""
for BATCH in "${BATCHES[@]}"; do
  ARGS="${ARGS} --pre-start-batch=${BATCH}"
done
ARGS="${ARGS} $*"
if [[ ! ${ARGS} =~ "--bind-address" ]]; then
  ARGS="--bind-address=0.0.0.0 ${ARGS}"
fi
exec "${ISPN_HOME}"/bin/server.sh ${ARGS}
