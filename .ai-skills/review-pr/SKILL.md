---
name: review-pr
description: "Review an Infinispan pull request. Fetches the PR metadata, diff, linked issue and CI results, verifies the author checklist, then hunts and verifies findings in a multi-pass review covering Infinispan-specific concerns (serialization compatibility, blocking in non-blocking paths, cross-site, thread safety, configuration chains, security), triages SpotBugs/CPD annotations, runs targeted tests locally when CI is red or coverage is questionable, drafts a reviewer's-guide walkthrough with typed findings, and posts it to GitHub only after the user approves the draft."
arguments:
  - name: pr
    description: "Pull request number, URL, or branch name (e.g. 15000)"
---

# /review-pr — PR Review Skill

Review a pull request against the Infinispan review rules and post the result to GitHub after the user approves the draft.

## Review persona

Adopt this persona for the entire review:

> You are an experienced, adversarial code reviewer for the Infinispan project. You have NO prior context on this change beyond what you can read. Do not assume anything is correct; verify every claim against the actual files.

Concretely: statements in the PR description, commit messages, and checklist are claims, not evidence — trust only what you can confirm in the diff, the checked-out code, and the CI logs.

Two disciplines follow from this persona:

- **Prove or demote.** A candidate finding you cannot back with `file:line` evidence and a reachable code path is a question for the author, not a finding.
- **Precision over volume.** Three verified findings are worth more than ten speculative ones. Noise trains authors to ignore reviews.

## Inputs

- `$pr` — the pull request number, URL, or branch name

## Procedure

### Step 0: Load the governing instructions

Read these files before looking at the diff. They are the source of truth; this skill only sequences the work:

- `REVIEW.md` — always. Severity definitions, nit cap, what not to report.
- `AI-CODE.md` — always. Architecture, clustered-mode and concurrency checklist, development standards.
- `AI-TEST.md` — when the PR touches `src/test/`. Test frameworks and mandatory coverage levels.
- `AI-CONFIG.md` — when the PR touches configuration classes or XSD schemas.
- `documentation/AI-CODE.md` — when the PR touches `documentation/`.
- Module-level `AI-CODE.md` — for each module the PR touches, if one exists in the module root.

### Step 1: Gather PR data

```
gh pr view $pr --json number,title,body,author,isDraft,baseRefName,headRefName,commits,files,reviews,comments
gh pr diff $pr
gh pr checks $pr
```

Record the PR number from the first command as `$NUMBER` — every later command uses it. If `gh pr checks` fails or shows pending checks, proceed with the review anyway — incomplete CI is not a blocker. Re-check just before posting and include whatever results have arrived; whatever is still missing goes into the deferred-judgment note (Steps 5 and 8).

A large diff is not a blocker — hosted review tools cap out on diff size, this workflow does not. The shell tool saves oversized `gh pr diff` output to a file; read it in chunks from there. Ignore comments authored by `github-actions` (coverage reports, flaky-test digests) except when triaging coverage — they are not review signal, and flaky tests with existing tracking issues are unrelated failures by definition.

From the PR body, extract the linked issue (`Closes #N` / `Fixes #N`) and fetch it: `gh issue view N`. If there is no linked issue, that is a checklist finding.

Detect the review mode:

- **First review** — no earlier review exists, or no commits were pushed since the last one. Proceed with the full procedure.
- **Re-review** — a previous review exists (from this skill, a teammate, or a bot) and newer commits were pushed after it. Review incrementally: compute the delta since the last reviewed commit (`git diff <reviewed-sha>..<head>`), review only the delta, and re-check each earlier finding against the new code, reporting which ones are resolved. Do not re-post findings that still stand verbatim; summarize them as unresolved.

Special cases (adapt, don't skip):

- **Draft PRs**: review the code, but frame findings as guidance for work in progress and do not request changes.
- **Bot PRs** (dependabot and similar): skip the author checklist and the issue link; focus on what changed between versions, breaking changes, and whether the new version is compatible with the rest of the build.

Bucket the changed files:

- **Implementation**: `src/main/`
- **Tests**: `src/test/`
- **Documentation**: `documentation/`
- **Configuration**: XSDs under `core/src/main/resources/schema/` or `server/runtime/src/main/resources/schema/`, and classes under `org.infinispan.configuration`

Also record the modules involved (the path before `src/`, e.g. `server/tests`).

### Step 2: Understand the change (first pass)

Before judging anything, build an accurate mental model:

- Read the full diff and enough surrounding code (whole methods, callers, related classes) to explain what each file's change does and why.
- Read the linked issue and assess whether the change actually addresses it. A PR that claims to fix an issue without touching the code path described in the issue is a finding.
- Prepare the walkthrough material used in Step 8: for each file, one line on what changed and why it matters, plus how a human could verify the change (which test to run, what to check).

### Step 3: Verify the author checklist

The PR template (`.github/pull_request_template.md`) includes an author checklist. Verify items are actually satisfied, not just checked:

- **Commit format**: every commit's first line matches `[#00000] Summary` (see `.gitmessage`).
- **Squashed commits**: no `WIP`- or `Implements feedback`-style messages.
- **Tests**: the PR adds or modifies tests exercising the changes, **or** the PR body contains a convincing justification. "Tested manually" without instructions is not convincing.
- **Documentation**: docs updated, or a convincing justification for why none is needed.
- **Log i18n**: scan the diff for new log calls in `src/main` at level INFO or above (`.info(`, `.warn(`, `.error(`, `.fatal(`). They must go through JBoss Logging (`org.infinispan.util.logging.Log` with `@Message`/`@LogMessage` ids) — hardcoded strings or `System.out` are findings.
- **Benchmarks / output samples**: only when the PR claims to affect performance or user-visible output (logs, CLI, Console). For benchmarks, check the sibling `../infinispan-benchmarks` repository directly (see `AI-CODE.md` for the JMH module layout) before asking the author for evidence — the benchmark may already exist there.

Unmet checklist items are Important findings, except items that don't apply to the change. Skip this step entirely for bot PRs.

### Step 4: Hunt for findings (second pass)

Sweep every hunk three ways:

**General** (from the reviewer checklist in the PR template): unconsidered inputs, use cases, or integrations; understandability (KISS, comments); better alternatives worth suggesting (Nit at most).

**Infinispan-specific** — check whether the diff makes each concern applicable before evaluating it:

- **Serialization compatibility**: new or changed fields in classes serialized via Protostream or JBoss Marshalling must be backward-compatible with rolling upgrades. Protostream schema changes require version bumps.
- **Blocking in non-blocking paths**: no I/O, locks, or `CompletableFuture.get()` on Netty or non-blocking event loop threads.
- **Cross-site replication**: changes to cache operations or internal commands may affect cross-site behavior.
- **Thread safety**: new mutable shared state must be properly guarded.
- **Configuration**: a new or changed attribute needs the full chain from `AI-CONFIG.md` — `Attribute`/`Element` enum values, `AttributeDefinition`, builder method, parser case, serializer write, XSD update, docs. A missing serializer or XSD entry is Important (silent config loss or validation failure).
- **Rolling upgrade compatibility**: new Hot Rod commands, REST endpoints, or protocol changes require updating `server/rollingupgradetests/src/test/resources/compatibility.json`.
- **Clustered mode and marshalling** (see `AI-CODE.md`): lambdas passed to `cache.compute()`/`cache.merge()` must be marshallable; prefer `compute()`/CAS over `get()` + `put()` for atomicity.
- **Test coverage levels** (see `AI-TEST.md`): clustered tests for cache operations sent across nodes; marshalling tests for new `@ProtoTypeId` classes; server integration tests for features exposed via Hot Rod or REST.
- **Javadocs**: required on new/changed public API classes and methods.

**Security** — mark these findings with 🚨:

- User-controlled data (keys, values, principal names, IPs) written to logs or error messages — PII leaks are Important per `REVIEW.md`.
- New parsing of network input (RESP, Hot Rod, Memcached, REST handlers, configuration parsers): does malformed or hostile input stay bounded, or can it throw uncaught exceptions, loop, or blow up memory?
- Hardcoded credentials, tokens, or secrets in the diff.
- New server-side operations reachable without an authorization check.
- Deserialization of untrusted data.

### Step 5: Check CI results

From `gh pr checks $NUMBER`:

- All green → state that in the summary.
- **Skipping** → the job's path or branch filter did not apply; treat it as not-applicable, not pending — it does not trigger the deferred-judgment note.
- **Pending or incomplete** → proceed with the review; this is not a blocker. Note which checks are still running, and state in the draft that final judgment is deferred until all checks are available. Re-run `gh pr checks` immediately before posting and fold in anything that has since finished.
- Red → inspect the failing job (`gh run view <job-id> --log-failed`) and determine whether the failure is plausibly caused by the changes. Only related failures are findings; infrastructure flakes and unrelated pre-existing failures are not.
- **Triage the static-analysis annotations**: Infinispan CI posts advisory (non-blocking) SpotBugs findings for changed classes and PMD copy-paste-detection comments, alongside checkstyle. These are candidate findings, not verdicts — read each annotation on changed lines, decide whether it is a real problem in context, and include the confirmed ones (citing the job). Dismiss noise rather than repeating it; do not re-derive what the scanners already caught.
- **Documentation is not built by PR CI** — the docs build runs only in the distribution profile and the push workflow. For PRs touching `documentation/`, verify the changed AsciiDoc statically: every `link:#anchor` must have a matching `[id='...']` in the same file or a reachable one. A local `mvn -Pdistribution -pl documentation package` is possible but its OpenAPI-generation step needs full-distribution artifacts and often fails for reasons unrelated to the diff — static xref checking is the pragmatic default.

### Step 6: Investigate and verify (third pass)

Every candidate finding from Steps 2–5 — your own and the scanners' — must survive this pass before it reaches the draft. If the user's working tree is not on the PR head, create the Step 7 worktree now and use it for all code inspection in this pass, not only for test runs.

- **Multi-hop the codebase.** For each changed or new public method/class, search for callers across modules (`grep -rn "methodName(" --include=*.java`) and check they handle the new behavior, signature, or exceptions. Follow cross-module impact (`api` → `core` → `server/*` → tests).
- **Check history before judging.** Before flagging odd or redundant-looking code for removal, read why it exists: `git log -p <file>` or `git blame` on the lines. Code guarded by a workaround usually has a reason.
- **Check convention before reporting.** A behavior that looks wrong in isolation may be project convention: before reporting "method X throws `UnsupportedOperationException`" (or similar) as a finding, grep the sibling implementations. Behavior consistent across the codebase is deliberate — report a deviation from convention, not the convention itself.
- **Verify reachability.** Confirm the offending line is on a path that can actually execute, with the inputs the PR allows. Unreachable or hypothetical issues are dropped or demoted to questions.
- **Re-read the exact lines.** Quote them in your notes. A finding you cannot cite as `path:line` with the code that proves it gets dropped or rephrased as a question for the author.

### Step 7: Run targeted tests locally

Run tests locally when CI is red and the failure might be related, or when it is unclear whether the PR's tests actually exercise the changes. Skip this step when the diff is trivially reviewable and CI is green.

Do not touch the user's working tree. Check out the PR head into a separate worktree:

```
git fetch origin pull/$NUMBER/head
git worktree add /tmp/opencode/review-pr-$NUMBER FETCH_HEAD
```

Derive modules and test classes from the changed test files: the module is the path before `src/test/`; the test class is the Java class name without package; class names ending in `IT` are failsafe integration tests run with `-Dit.test=`, all others use `-Dtest=`. Then, from inside the worktree:

```
mvn clean install -pl <module> -am -DskipTests -Dcheckstyle.skip
mvn verify -pl <module> -Dtest=TestClass1,TestClass2 -Dcheckstyle.skip
mvn verify -pl <module> -Dit.test=ITClass1 -Dcheckstyle.skip
```

Record the results for the summary. Always clean up, even if a run fails midway:

```
git worktree remove --force /tmp/opencode/review-pr-$NUMBER
```

To verify that tests actually fail without the implementation (deeper than a review needs), suggest the `verify-commit` skill to the user rather than replicating it here.

### Step 8: Compose the draft review

Apply the rules from `REVIEW.md`:

- **Important** = would break behavior, leak data, or block a rollback: incorrect logic, PII in logs or error messages, non-backward-compatible migrations, blocking constructs in non-blocking paths. Unmet author-checklist items from Step 3 are Important.
- **Nit** = style, naming, refactoring suggestions. Report at most five; beyond that write "plus N similar items" in the summary.
- If every finding is a Nit, the summary must lead with **"No blocking issues."**
- Do not report anything CI already enforces (lint, formatting, type errors) or test-only code that intentionally violates production rules.
- Anything that needs the author's input but is not a proven finding is a **Question**, asked separately from the findings — never inflated to Important.

Structure the draft as a reviewer's guide:

1. **Walkthrough** — a short overview of the change, then a file-by-file table: `File | Change | Why it matters`. End with **How to verify**: the concrete steps a human reviewer can follow (tests to run, commands, what to check by hand).
2. **Verdict summary** — what the PR does, whether it addresses the linked issue, overall verdict, CI status (including triaged SpotBugs/CPD annotations), local test results if run. If any checks were still pending when the review ran, the summary must say so explicitly, e.g. **"Final judgment deferred: N checks still pending"** — the verdict is provisional until they complete.
3. **Findings** — each formatted as:
   - `**Important (category)** 🚨 if security — `path:line``
   - One-paragraph explanation citing the exact code.
   - A concrete suggested fix — a fenced code block the author can apply, or a precise description when a snippet doesn't fit.
   - Categories: `bug_risk`, `compatibility`, `security`, `performance`, `maintainability`, `test_coverage`, `documentation`, `process`.
4. **Questions** — for the author, clearly separated from findings.
5. **Checklist violations** — from Step 3, if any.
6. A short closing note to the author.

Present the draft to the user and ask for approval. Do not post anything before the user approves the text.

### Step 9: Post the review

With the approved draft saved to a file:

```
gh pr review $NUMBER --comment --body-file <draft>
```

- Approve only when there are no Important findings **and** the change needs no human judgment beyond this review → `gh pr review $NUMBER --approve --body-file <draft>`. When in doubt, comment and leave the approval to a human.
- If CI checks were still pending, post as `--comment` with the deferred-judgment note — do not approve or request changes on a provisional verdict. Once the checks finish, a follow-up is cheap: re-run `gh pr checks`, post the final judgment, and only review the delta if something failed.
- Important findings that the user wants blocking → `gh pr review $NUMBER --request-changes --body-file <draft>`.
- If a `--request-changes` or `--approve` call fails (e.g. missing permission), fall back to `--comment`.

After posting, offer the natural follow-ups rather than stopping at the review: drafting resolution replies for the findings when the author pushes fixes, and — if the user is the author — author-side work such as fixing the nits or opening follow-up issues (per `AI-ISSUES.md`) for the remaining process findings.

## Important notes

- The review is read-only with respect to the PR: never push to, rebase, or edit the PR branch.
- Never post to GitHub without explicit user approval of the draft text.
- Always remove the worktree from Step 7, even if tests failed or were interrupted.
- Findings reference `path:line` from the diff's new-file side so the author can jump straight to them.
- If a checklist item does not apply to the change (e.g. benchmarks for a pure refactor), do not demand it.
- On a re-review (Step 1), report the delta: new findings, resolved findings, and unresolved ones in one summary — do not repeat the full first review.
