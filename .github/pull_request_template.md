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
- [ ] If the PR requires a followup or is part of a larger body of work, ensure that relevant [GitHub issues](https://github.com/infinispan/infinispan/issues) have been created to track the additional work.

Reviewer Checklist (all must be checked):
- [ ] Code review
  - Are the changes reasonable?
  - Are there any use cases, integrations, inputs that haven't been considered in the implementation?
  - Is the code understandable? (KISS, comments)
  - Is there a better way to implement/write the functionality, code pieces?
  - Is formatting in line with the rest of the project? (checkstyle and spotless)
- [ ] Test review
  - Is the new test coverage rich enough to cover all the changes and considerations above?
  - Does the base functionality works as expected in the build project?
  - Does it behave as expected when given various inputs? (Valid and invalid, missing, partial)
  - Are there integrations to be considered? (Does the new functionality work well with the rest of the project, environment)
  -  Even if all above works as expected, are there weird behaviors to be improved on?
- [ ] Documentation review
  - Does the new/changed documentation cover the PR in sufficient matter?
  - For public API, are JavaDocs included for every new/changed class/method ?
  - For new features, does the documentation add/amend any usage examples ?
  - Grammar and syntax checks 
  - Does AsciiDoc generation report any WARN messages ?
