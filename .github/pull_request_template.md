<!--- If this is a non-code change, this template is not required; reference any issues or top-level descriptions as needed -->
<!--- All code change PRs should relate to an issue, reference it here; see example below -->
<!--- Closes: #00000 -->

Author Checklist (all must be checked):
- [ ] Commit message includes a reference to the corresponding [GitHub issue](https://github.com/infinispan/infinispan/issues) (e.g. commit message: "[#00000] Issue title...") and is formatted according to our [git message template](https://github.com/infinispan/infinispan/blob/main/.gitmessage).
- [ ] Commits have been squashed into self-contained units of work (e.g. 'WIP'- and 'Implements feedback'-style messages have been removed).
- [ ] The PR includes new/modified unit and integration tests that exercise the changes. Include additional manual testing instructions if necessary. If the PR does not include tests, clear justification **MUST** be provided.
- [ ] The PR includes new/modified documentation. If the PR does not require documentation changes, clear justification **MUST** be provided.
- [ ] Log messages of level `INFO` and above have been internationalized.
- [ ] If the PR affects performance, include before/after benchmarks.
- [ ] If the PR affects output (such as logs, CLI, Console), provide an example (text snippet/screenshot).
- [ ] If the PR requires a followup or is part of a larger body of work, ensure that relevant [GitHub issues](https://github.com/infinispan/infinispan/issues) have been created as part of a larger Epic to track the additional work.

Reviewer Checklist (all must be checked):
- [ ] Code review
- [ ] Test review
- [ ] Documentation review
