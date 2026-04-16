# Issue Creation Instructions

## Issue Types

Use GitHub issue types (not labels) for classification. Each type has a template in `.github/ISSUE_TEMPLATE/`:

| Type        | Template              | When to Use                                                                              |
|-------------|-----------------------|------------------------------------------------------------------------------------------|
| **Bug**     | `bug_report.yml`      | Something is broken or behaving incorrectly                                              |
| **Feature** | `feature_request.yml` | New functionality or enhancement to existing functionality                               |
| **Epic**    | `epic.yml`            | A large feature broken into multiple linked issues — requires a linked GitHub Discussion |
| **Task**    | `housekeeping.yml`    | Cleanup, refactoring, dependency updates, CI changes — not a bug or feature              |

If none of these types apply, blank issues are also allowed.

## Required Fields by Type

### Bug Report
- **Description** (required) — what is broken
- **Expected behavior** (required) — what should happen
- **Actual behavior** (required) — what happens instead
- **How to reproduce** (required) — steps or link to a reproducer
- **Environment** — version, JDK, OS

Note: security vulnerabilities should be reported privately to security@infinispan.org, not as public issues.

### Feature Request
- **Description** (required) — what the feature does and why it's needed
- **Implementation ideas** (optional) — technical approach, link to infinispan-designs if applicable

### Epic
- **Description** (required) — high-level description of the feature
- **Discussion link** (required) — link to a GitHub Discussion where the feature was discussed
- **Related issues** (optional) — list of sub-issues (`#1`, `#2`, etc.)
- **Motivation** (optional) — why this feature should be added

### Task
- **Description** (required) — what needs to be done
- **Implementation ideas** (optional) — technical approach

## Conventions

- Issue titles should be concise and descriptive
- Reference related issues and PRs using `#number` format
- When creating issues from code investigation, include file paths and line numbers where relevant
- Commit messages reference issues using `[#00000] Summary` format

## External Resources

- Questions about usage: [StackOverflow (tag: infinispan)](https://stackoverflow.com/questions/ask?tags=infinispan)
- Community discussions: [GitHub Discussions](https://github.com/infinispan/infinispan/discussions)
- Live chat: [Infinispan Zulip](https://infinispan.zulipchat.com/)
