#!/bin/bash
# Tests for the "Sync existing repo from server" step of release_cli_packages.yml.
#
# The step script is extracted verbatim from the workflow and executed against a
# local sshd serving a fake remote repository, under two "ls -l" dialects: the
# native OpenSSH one (full paths in the name column) and the one used by
# filemgmt.jboss.org (bare file names). The scenarios cover complete, partial
# and truncated downloads, a pristine server, inconsistent remote states and an
# unreachable server.
#
# Requirements: bash, sshd (openssh-server), python3 with PyYAML, GNU stat.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
WORK=$(mktemp -d)
PORT=$((22000 + RANDOM % 2000))
REMOTE="${WORK}/remote"
SSHD=$(command -v sshd || echo /usr/sbin/sshd)

cleanup() {
  if [ -f "${WORK}/sshd.pid" ]; then
    kill "$(cat "${WORK}/sshd.pid")" 2>/dev/null || true
  fi
  rm -rf "${WORK}"
}
trap cleanup EXIT

# --- extract the step script verbatim from the workflow ---
python3 - "${REPO_ROOT}/.github/workflows/release_cli_packages.yml" > "${WORK}/sync-step.sh" <<'PY'
import sys, yaml
wf = yaml.safe_load(open(sys.argv[1]))
for job in wf["jobs"].values():
    for step in job.get("steps", []):
        if step.get("name") == "Sync existing repo from server":
            print(step["run"])
            sys.exit(0)
sys.exit("step not found")
PY

# --- local sshd serving the fake remote repository ---
ssh-keygen -q -t ed25519 -N '' -f "${WORK}/host_key"
ssh-keygen -q -t ed25519 -N '' -f "${WORK}/id_user"
cp "${WORK}/id_user.pub" "${WORK}/authorized_keys"
cat > "${WORK}/sshd.conf" <<EOF
ListenAddress 127.0.0.1
Port ${PORT}
AddressFamily inet
HostKey ${WORK}/host_key
Subsystem sftp internal-sftp
PubkeyAuthentication yes
PasswordAuthentication no
AuthorizedKeysFile ${WORK}/authorized_keys
StrictModes no
UsePAM no
PidFile ${WORK}/sshd.pid
LogLevel ERROR
EOF
"${SSHD}" -f "${WORK}/sshd.conf"
: > "${WORK}/known_hosts"
echo "[localhost]:${PORT} $(cat "${WORK}/host_key.pub")" >> "${WORK}/known_hosts"

# --- sftp wrapper: connection plumbing, the "bare" ls -l dialect, and sabotage
# of the sync batch (the first invocation) to simulate partial downloads. All
# configuration arrives through the environment: SYNC_TEST_WORK, SYNC_TEST_PORT,
# DIALECT (native|bare), SABOTAGE (drop|truncate), OUT_DIR. ---
mkdir -p "${WORK}/bin"
cat > "${WORK}/bin/sftp" <<'WRAPPER'
#!/bin/bash
set -u
batch=$(cat)
echo x >> "${SYNC_TEST_WORK}/calls"
calls=$(wc -l < "${SYNC_TEST_WORK}/calls")
err_file=$(mktemp)
out=$(printf '%s\n' "${batch}" | /usr/bin/sftp -P "${SYNC_TEST_PORT}" \
  -i "${SYNC_TEST_WORK}/id_user" -F none -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes -o CheckHostIP=no -o AddressFamily=inet \
  -o BatchMode=yes -o UserKnownHostsFile="${SYNC_TEST_WORK}/known_hosts" "$@" \
  2> "${err_file}")
rc=$?
cat "${err_file}" >&2
rm -f "${err_file}"
if [ "${DIALECT:-native}" = bare ]; then
  # filemgmt.jboss.org prints bare file names in the last ls -l column
  printf '%s\n' "${out}" | awk '$1 ~ /^-/ { n = split($9, a, "/"); $9 = a[n] } { print }'
else
  printf '%s\n' "${out}"
fi
if [ "${calls}" -eq 1 ] && [ -n "${SABOTAGE:-}" ] && [ -n "${OUT_DIR:-}" ]; then
  case "${SABOTAGE}" in
    drop)
      victim=$(find "${OUT_DIR}/repo/yum/x86_64" -name '*.rpm' | head -1)
      [ -n "${victim}" ] && rm -f "${victim}"
      ;;
    truncate)
      victim=$(find "${OUT_DIR}/repo/apt/pool/main" -name '*.deb' | head -1)
      [ -n "${victim}" ] && truncate -s 1 "${victim}"
      ;;
  esac
fi
exit "${rc}"
WRAPPER
chmod +x "${WORK}/bin/sftp"

seed_remote() {
  rm -rf "${REMOTE}"
  mkdir -p "${REMOTE}/yum/x86_64/repodata" "${REMOTE}/yum/aarch64/repodata" \
    "${REMOTE}/apt/pool/main"
  for v in 16.2.1 16.2.2 16.3.1; do
    for arch in x86_64 aarch64; do
      printf 'rpm-%s' "${v}" > "${REMOTE}/yum/${arch}/infinispan-cli-${v}-1.${arch}.rpm"
    done
    printf 'deb-%s' "${v}" > "${REMOTE}/apt/pool/main/infinispan-cli_${v}_amd64.deb"
    printf 'deb-%s' "${v}" > "${REMOTE}/apt/pool/main/infinispan-cli_${v}_arm64.deb"
  done
  echo old > "${REMOTE}/yum/x86_64/repodata/repomd.xml"
}

failures=0
run_scenario() {
  name=$1 expect=$2 dialect=$3 sabotage=${4:-}
  out_dir="${WORK}/run"
  rm -rf "${out_dir}"
  mkdir -p "${out_dir}"
  rm -f "${WORK}/calls"
  export SYNC_TEST_WORK="${WORK}" SYNC_TEST_PORT="${PORT}" DIALECT="${dialect}"
  export SABOTAGE="${sabotage}" OUT_DIR="${out_dir}"
  rc=0
  (
    cd "${out_dir}"
    export PATH="${WORK}/bin:${PATH}"
    current_user=$(id -un)
    export SFTP_USER="${current_user}" SFTP_HOST=localhost REMOTE_BASE="${REMOTE}"
    bash -e "${WORK}/sync-step.sh"
  ) > "${WORK}/scenario.log" 2>&1 || rc=$?
  if { [ "${expect}" = pass ] && [ "${rc}" -eq 0 ]; } ||
    { [ "${expect}" = fail ] && [ "${rc}" -ne 0 ]; }; then
    echo "PASS: ${name}"
  else
    echo "FAIL: ${name} (exit ${rc}, expected ${expect})"
    sed 's/^/  | /' "${WORK}/scenario.log"
    failures=$((failures + 1))
  fi
  grep -E '^::(warning|error)::' "${WORK}/scenario.log" | sed 's/^/  > /' || true
}

for dialect in native bare; do
  seed_remote
  run_scenario "complete sync (${dialect})" pass "${dialect}"
  rm -rf "${REMOTE}"
  run_scenario "pristine server (${dialect})" pass "${dialect}"
  seed_remote
  run_scenario "partial download (${dialect})" fail "${dialect}" drop
  seed_remote
  run_scenario "truncated download (${dialect})" fail "${dialect}" truncate
  seed_remote
  rm -f "${REMOTE}"/yum/x86_64/*.rpm
  run_scenario "empty package directory (${dialect})" fail "${dialect}"
  seed_remote
  rm -rf "${REMOTE}/yum"
  run_scenario "missing directory on established repository (${dialect})" fail "${dialect}"
done

kill "$(cat "${WORK}/sshd.pid")" 2>/dev/null || true
rm -f "${WORK}/sshd.pid"
sleep 0.5
run_scenario "unreachable server" fail native

if [ "${failures}" -ne 0 ]; then
  echo "${failures} scenario(s) failed"
  exit 1
fi
echo "all scenarios passed"
