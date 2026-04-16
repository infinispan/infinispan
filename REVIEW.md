# Pull Request Reviewing Instructions

GitHub pull requests should use the .github/pull_request_template.md file which contains user instructions for 
submitting well-formed pull requests. Ensure that the pull request follows the rules, when they are applicable.

## Verify the author checklist

The PR template includes an author checklist. Verify that checklist items are actually satisfied, not just checked off.
Pay particular attention to:
- Test coverage: if tests are missing, is the justification convincing?
- Log messages of level `INFO` and above must be internationalized using JBoss Logging messages.
- Documentation updates: if missing, is the justification convincing?
- Commit messages reference a GitHub issue in the `[#00000] Summary` format.

## What Important means here

Reserve Important for findings that would break behavior, leak data, or block a rollback: incorrect logic, 
PII in logs or error messages, and migrations that aren't backward compatible. Look out for blocking constructs in
non-blocking code paths. Style, naming, and refactoring suggestions are Nit at most.

## Infinispan-specific concerns

Watch for these domain-specific issues that general review might miss:
- **Serialization compatibility:** new or changed fields in classes that are serialized (Protostream, JBoss Marshalling) must be backward-compatible with rolling upgrades. Protostream schema changes require version bumps.
- **Blocking in non-blocking paths:** blocking calls (I/O, locks, `CompletableFuture.get()`) must not appear in Netty or non-blocking event loop threads.
- **Cross-site replication:** changes to cache operations or internal commands may affect cross-site replication behavior.
- **Thread safety:** core data structures are accessed concurrently — verify that new mutable shared state is properly guarded.
- **Configuration changes:** new configuration attributes must have XML/JSON/YAML parsers, schema updates, and documentation.

## Cap the nits

Report at most five Nits per review. If you found more, say "plus N similar items" in the summary instead of posting 
them inline. If everything you found is a Nit, lead the summary with "No blocking issues."

## Do not report

- Anything CI already enforces: lint, formatting, type errors
- Test-only code that intentionally violates production rules

## CI Failures

- If the CI checks have been executed, look at the results and try to determine if any failures are related to the changes.