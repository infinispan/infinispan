#!/bin/bash -e
#
# Analyzes jstack thread dump files and produces a Markdown summary.
# Used by CI to post a comment on PRs when the build times out.
#
# Usage: analyze-jstack.sh <directory-with-jstack-files>
# Output: Markdown to stdout

JSTACK_DIR="$1"

if [ -z "$JSTACK_DIR" ] || [ ! -d "$JSTACK_DIR" ]; then
  echo "Usage: $0 <directory-with-jstack-files>" >&2
  exit 1
fi

shopt -s nullglob
JSTACK_FILES=("$JSTACK_DIR"/jstack-*.txt)
shopt -u nullglob

if [ ${#JSTACK_FILES[@]} -eq 0 ]; then
  echo "No jstack files found in $JSTACK_DIR" >&2
  exit 0
fi

# Noise thread name prefixes to exclude from "notable threads" listing
NOISE_PATTERN='^"(Reference Handler|Finalizer|Signal Dispatcher|Service Thread|Monitor Deflation Thread|C[12] CompilerThread|Notification Thread|Common-Cleaner|Attach Listener|InnocuousThread-|GC Thread|G1 |VM Thread|VM Periodic Task|surefire-forkedjvm-|fork-[0-9]+-|ThreadedStreamConsumer|commands-fork-|timeout-check-timer|ping-timer-|process reaper|Log4j2-|JNA Cleaner|Cleaner-|MasterPoller|VirtualThread-unblocker|ForkJoinPool-.*delayScheduler|RMI Scheduler|RxCachedWorkerPoolEvictor|pool-[0-9]+-thread|Timer-)'

TAB=$'\t'

# Extract the main thread stack block from a jstack file
extract_main_thread() {
  local file="$1"
  awk '
    /^"main" / { found=1 }
    found && /^"/ && !/^"main" / { exit }
    found { print }
  ' "$file"
}

# Shorten a fully-qualified name: org.infinispan.foo.Bar.method -> o.i.f.Bar.method
# Shortens all package segments but preserves class name and method name
shorten_class() {
  local fqn="$1"
  local IFS='.'
  local parts
  read -ra parts <<< "$fqn"
  local len=${#parts[@]}
  local result=""
  for (( i=0; i<len; i++ )); do
    if (( i < len - 2 )); then
      result+="${parts[$i]:0:1}."
    elif (( i < len - 1 )); then
      result+="${parts[$i]}."
    else
      result+="${parts[$i]}"
    fi
  done
  echo "$result"
}

# Parse "at org.foo.Bar.method(Bar.java:42)" -> extracts FQN and file location
parse_frame() {
  local line="$1"
  FRAME_FQN=$(echo "$line" | sed -E 's/.*at ([^(]+)\(.*/\1/')
  FRAME_LOC=$(echo "$line" | grep -oP '\([^)]+\)' | tr -d '()')
}

echo "## Thread Dump Analysis (CI Timeout)"
echo ""

EMPTY_PIDS=()

# Classify each file by process type
declare -A FILE_TYPE
for FILE in "${JSTACK_FILES[@]}"; do
  PID=$(basename "$FILE" .txt | sed 's/jstack-//')
  FILE_SIZE=$(stat -c%s "$FILE" 2>/dev/null || echo 0)

  if [ "$FILE_SIZE" -le 1 ]; then
    FILE_TYPE[$FILE]="empty"
    EMPTY_PIDS+=("$PID")
  elif grep -q 'ForkedBooter\.main' "$FILE"; then
    FILE_TYPE[$FILE]="forked"
  elif grep -q 'ForkStarter\.fork\|ForkStarter\.run' "$FILE"; then
    FILE_TYPE[$FILE]="maven"
  else
    FILE_TYPE[$FILE]="other"
  fi
done

# Process forked test JVMs first (most interesting)
for FILE in "${JSTACK_FILES[@]}"; do
  [ "${FILE_TYPE[$FILE]}" != "forked" ] && continue

  PID=$(basename "$FILE" .txt | sed 's/jstack-//')

  echo "### Forked Test JVM (PID $PID)"
  echo ""

  MAIN_BLOCK=$(extract_main_thread "$FILE")

  if [ -n "$MAIN_BLOCK" ]; then
    # Thread state from the main thread
    THREAD_STATE=$(echo "$MAIN_BLOCK" | grep -oP 'java\.lang\.Thread\.State: \K.*' | head -1)

    # All stack frames from the main thread (tab-indented "at" lines)
    STACK_FRAMES=$(echo "$MAIN_BLOCK" | grep -P "^${TAB}at " || true)

    # Stuck test method: the LAST org.infinispan.*Test(s). frame (deepest in stack = actual test method)
    STUCK_TEST_LINE=$(echo "$STACK_FRAMES" | grep 'at org\.infinispan\..*Tests\?\.' | tail -1 || true)
    # Blocked at: the FIRST org.infinispan. frame (shallowest = closest to the blocking call)
    STUCK_AT_LINE=$(echo "$STACK_FRAMES" | grep -m1 'at org\.infinispan\.' || true)

    if [ -n "$STUCK_TEST_LINE" ]; then
      parse_frame "$STUCK_TEST_LINE"
      TEST_SHORT=$(shorten_class "$FRAME_FQN")
      echo "**Stuck test:** \`$TEST_SHORT\`"
    else
      echo "**Stuck test:** (no test method identified in main thread stack)"
    fi

    if [ -n "$STUCK_AT_LINE" ] && [ "$STUCK_AT_LINE" != "$STUCK_TEST_LINE" ]; then
      parse_frame "$STUCK_AT_LINE"
      AT_SHORT=$(shorten_class "$FRAME_FQN")
      echo "**Blocked at:** \`$AT_SHORT\` ($FRAME_LOC)"
    fi

    echo "**Thread state:** $THREAD_STATE"
    echo ""

    # Collapsible main thread stack trace
    echo '<details><summary>Main thread stack trace</summary>'
    echo ''
    echo '```'
    echo "$STACK_FRAMES" | head -25 | sed "s/^${TAB}//"
    echo '```'
    echo ''
    echo '</details>'
    echo ''
  fi

  # Thread state summary (use || true to avoid exit on zero matches)
  TOTAL_THREADS=$(grep -cP '^"[^"]+" #' "$FILE" || true)
  RUNNABLE=$(grep -c 'java.lang.Thread.State: RUNNABLE' "$FILE" || true)
  WAITING=$(grep -cP 'java.lang.Thread.State: WAITING( |$)' "$FILE" || true)
  TIMED_WAITING=$(grep -c 'java.lang.Thread.State: TIMED_WAITING' "$FILE" || true)
  NUM_BLOCKED=$(grep -c 'java.lang.Thread.State: BLOCKED' "$FILE" || true)

  echo "**Thread summary:** $TOTAL_THREADS threads -- $RUNNABLE RUNNABLE, $WAITING WAITING, $TIMED_WAITING TIMED_WAITING, $NUM_BLOCKED BLOCKED"
  echo ""

  # Deadlock detection
  if grep -q 'Found one Java-level deadlock' "$FILE"; then
    echo "**DEADLOCK DETECTED**"
    echo ""
    echo '<details><summary>Deadlock details</summary>'
    echo ''
    echo '```'
    awk '/Found one Java-level deadlock/,/Found [0-9]+ deadlock/' "$FILE"
    echo '```'
    echo ''
    echo '</details>'
    echo ''
  fi

  # Notable threads: exclude noise, exclude main (already shown), group similar names
  NOTABLE_THREADS=$(grep -P '^"' "$FILE" | grep -v '^"main"' | grep -vP "$NOISE_PATTERN" || true)

  if [ -n "$NOTABLE_THREADS" ]; then
    echo "**Notable threads:**"

    # Group by thread name prefix (strip trailing numbers and pool IDs)
    declare -A THREAD_GROUPS
    declare -A THREAD_GROUP_STATES
    declare -a THREAD_GROUP_ORDER=()

    while IFS= read -r line; do
      [ -z "$line" ] && continue
      TNAME=$(echo "$line" | sed -E 's/^"([^"]+)".*/\1/')
      # Normalize name: strip trailing -N, #N, compound -N-N, and trailing dashes
      GROUP=$(echo "$TNAME" | sed -E 's/-[0-9]+(-[0-9]+)*$//' | sed -E 's/ #[0-9]+//' | sed -E 's/-+$//')

      # Get this thread's state from the file using the exact thread name
      TSTATE=$(awk -v name="$TNAME" '
        BEGIN { gsub(/[\\.*+?{}()|^$\[\]]/, "\\\\&", name); pat = "^\"" name "\" " }
        $0 ~ pat { found=1; next }
        found && /java\.lang\.Thread\.State:/ { sub(/.*State: /,""); print; exit }
      ' "$FILE")

      if [ -z "${THREAD_GROUPS[$GROUP]+x}" ]; then
        THREAD_GROUPS[$GROUP]=1
        THREAD_GROUP_STATES[$GROUP]="$TSTATE"
        THREAD_GROUP_ORDER+=("$GROUP")
      else
        THREAD_GROUPS[$GROUP]=$(( ${THREAD_GROUPS[$GROUP]} + 1 ))
      fi
    done <<< "$NOTABLE_THREADS"

    COUNT=0
    for GROUP in "${THREAD_GROUP_ORDER[@]}"; do
      [ $COUNT -ge 10 ] && break
      NUM=${THREAD_GROUPS[$GROUP]}
      STATE=${THREAD_GROUP_STATES[$GROUP]}
      if [ "$NUM" -gt 1 ]; then
        echo "- ${NUM}x \`$GROUP\` ($STATE)"
      else
        echo "- \`$GROUP\` ($STATE)"
      fi
      COUNT=$((COUNT + 1))
    done

    # Always show BLOCKED threads even if over the limit
    BLOCKED_THREADS=$(awk '
      /^"/ { name=$0; next }
      /java\.lang\.Thread\.State: BLOCKED/ {
        gsub(/^"/, "", name)
        gsub(/".*/, "", name)
        print name
      }
    ' "$FILE")

    if [ -n "$BLOCKED_THREADS" ]; then
      echo ""
      echo "**BLOCKED threads:**"
      while IFS= read -r t; do
        [ -z "$t" ] && continue
        echo "- \`$t\`"
      done <<< "$BLOCKED_THREADS"
    fi

    unset THREAD_GROUPS THREAD_GROUP_STATES THREAD_GROUP_ORDER
    echo ""
  fi
done

# Maven parent processes (brief)
for FILE in "${JSTACK_FILES[@]}"; do
  [ "${FILE_TYPE[$FILE]}" != "maven" ] && continue
  PID=$(basename "$FILE" .txt | sed 's/jstack-//')
  TOTAL_THREADS=$(grep -cP '^"[^"]+" #' "$FILE" || true)
  echo "### Maven Parent (PID $PID)"
  echo "Waiting for forked test process. $TOTAL_THREADS threads."
  echo ""
done

# Other (non-empty, non-maven, non-forked) processes
for FILE in "${JSTACK_FILES[@]}"; do
  [ "${FILE_TYPE[$FILE]}" != "other" ] && continue
  PID=$(basename "$FILE" .txt | sed 's/jstack-//')
  TOTAL_THREADS=$(grep -cP '^"[^"]+" #' "$FILE" || true)
  echo "### Other JVM Process (PID $PID)"
  echo "$TOTAL_THREADS threads."
  echo ""
done

# Empty dumps
if [ ${#EMPTY_PIDS[@]} -gt 0 ]; then
  PIDS_STR=$(printf ", %s" "${EMPTY_PIDS[@]}")
  PIDS_STR=${PIDS_STR:2}
  echo "### Empty Dumps"
  echo "PIDs $PIDS_STR -- jstack could not attach (likely Infinispan Server processes)."
  echo ""
fi
