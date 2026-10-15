---
name: verify-commit
description: "Verify that commits for a GitHub issue include tests and that those tests fail without the implementation changes. Given an issue ID, finds commits, checks for test files, then reverts src/main changes and runs the tests to confirm they fail."
arguments:
  - name: issue
    description: "GitHub issue number (e.g. 18300)"
  - name: branch
    description: "Branch to search and base the verification on (e.g. origin/16.0.x). Defaults to the current branch."
    default: "HEAD"
---

# /verify-commit — Commit Verification Skill

Verify that the commits for a GitHub issue include proper tests and that the tests actually detect the bug/feature they claim to cover.

## Inputs

- `$issue` — the GitHub issue number (digits only, no `#` prefix)
- `$branch` — the branch to search for commits and base the verification on (e.g. `origin/16.0.x`, `main`). Defaults to `HEAD` (current branch).

## Procedure

### Step 0: Resolve the branch

If `$branch` is a remote-tracking branch (e.g. `origin/16.0.x`), fetch it first:

```
git fetch origin 16.0.x
```

### Step 1: Find commits

Find all commits on `$branch` whose first line matches `[#$issue]`:

```
git log --oneline $branch --grep='\[#$issue\]'
```

Do **not** use `--all` — only search the specified branch.

If no commits are found, report that and stop.

### Step 2: Identify changed files

For each commit, collect the list of changed files:

```
git show --stat --name-only <commit>
```

Separate them into two buckets:

- **Implementation files**: paths containing `src/main/`
- **Test files**: paths containing `src/test/`

If there are **no test files** across all the commits, **abort immediately** with a clear message:

> ❌ No test files found in commits for issue #$issue. Verification requires tests.

### Step 3: Identify modules and test classes

First, separate test files into two categories:

- **Test source files**: paths under `src/test/java/` — these are runnable test classes.
- **Test resource files**: paths under `src/test/resources/` — these are data files consumed by tests, not runnable on their own. Note their module for the build, but do not try to derive test class names from them.

For each **test source file**, derive:

1. **The Maven module path**: everything before `src/test/`. For example, `server/tests/src/test/java/...` → module is `server/tests`.
2. **The test class name**: the Java class name without package (e.g. `MurmurHash3CompatTest`).
3. **Whether it's a failsafe integration test**: the class name ends with `IT` (e.g. `ClusteredRollingUpgradeIT`). These use `-Dit.test=`. All others use `-Dtest=`.

Collect the unique set of modules and their associated test classes. If test resources were changed but no test source files exist in any commit, identify which test classes consume those resources (e.g. by grepping for the resource filenames) and include those in the test run.

### Step 4: Stash current state and revert implementation

Before modifying the working tree, ensure it's clean (stash if needed).

Create a temporary verification branch from `$branch`:

```
git checkout -b verify-$issue $branch
```

Now **revert only the implementation files** (src/main) from the identified commits, keeping test changes intact:

```
git show <commit> -- <src/main files...> | git apply --reverse
```

Do this for each commit, applying all src/main reversions. If a revert fails due to conflicts, note which files conflicted and report them — do not force through.

Commit the reversion so the build has a clean tree. Use `--no-verify` to bypass any commit-message hooks, since this is a temporary commit that will be deleted in cleanup:

```
git commit -am "temporary: revert implementation for verification" --no-verify
```

### Step 5: Build dependencies

Build the project up to (but not including the test phase of) the involved modules. Use `-Dmaven.test.skip=true` (not `-DskipTests`) because `-DskipTests` still compiles tests — when tests reference new APIs added by the implementation, test compilation will fail prematurely:

```
mvn -am -pl <module1>,<module2>,... -Dmaven.test.skip=true clean package
```

If the build fails, report the failure and skip to cleanup.

### Step 6: Run the tests (expecting failure)

For each module, run its tests. Separate surefire and failsafe tests:

**For surefire tests** (class names NOT ending in `IT`):
```
mvn verify -pl <module> -Dtest=TestClass1,TestClass2
```

**For failsafe integration tests** (class names ending in `IT`):
```
mvn verify -pl <module> -Dit.test=ITClass1,ITClass2
```

Record the results. The **expected outcome is test failure** — this proves the tests actually cover the implementation.

### Step 7: Report results

Produce a summary table:

| Test Class | Module | Type | Result | Verdict |
|---|---|---|---|---|
| FooTest | core | surefire | COMPILE ERROR | ✅ Test structurally depends on implementation |
| BazTest | core | surefire | FAILED | ✅ Test catches the bug |
| BarIT | server/tests | failsafe | PASSED | ⚠️ Test passes without implementation |

- **COMPILE ERROR** → ✅ Strong: the test won't even compile without the implementation — it's structurally dependent on the new code (e.g. references a method or class added by the implementation).
- **FAILED** → ✅ Good: the test properly detects the absence of the implementation.
- **PASSED** → ⚠️ Suspicious: the test does not fail without the implementation, meaning it may not be testing what the commit claims.
- **BUILD ERROR** → Report the error details (distinct from COMPILE ERROR — this covers non-compilation build failures like missing resources or plugin errors).

### Step 8: Cleanup

Always clean up, even if earlier steps failed:

```
git checkout -
git branch -D verify-$issue
```

If you stashed changes in Step 4, restore them:

```
git stash pop
```

## Important notes

- Never modify the original branch. All reversion work happens on the temporary `verify-$issue` branch.
- If multiple commits exist for the same issue, process all of them together — revert all implementation changes, then run all tests.
- If a commit only touches test files (no src/main changes), note it but don't try to revert anything for that commit.
- If a commit only touches implementation files (no src/test changes), it contributes to the implementation bucket but doesn't add tests — the check in Step 2 covers the aggregate.
