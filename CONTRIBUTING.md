Guidelines for contributing to Infinispan
====

Contributions from the community are essential in keeping Infinispan strong and successful.

This guide focuses on how to contribute back to Infinispan using GitHub pull requests.
If you need help with cloning, compiling or setting the project up in an IDE please refer to
our [Contributing Guide](https://infinispan.org/docs/dev/titles/contributing/contributing.html).

## Legal

All original contributions to Infinispan are licensed under the
[ASL - Apache License](https://www.apache.org/licenses/LICENSE-2.0),
version 2.0 or later, or, if another license is specified as governing the file or directory being
modified, such other license.

All contributions are subject to the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).
The DCO text is also included verbatim in the [dco.txt](dco.txt) file in the root directory of the repository.

## Getting Started

If you are just getting started with Git, GitHub and/or contributing to Infinispan there are a
few prerequisite steps:

* Make sure you have a [GitHub account](https://github.com/signup/free)
* [Fork](https://help.github.com/articles/fork-a-repo/) the
  Infinispan [repository](https://github.com/infinispan/infinispan).
  As discussed in the linked page, this also includes:
    * [Setting](https://help.github.com/articles/set-up-git) up your local git install
    * Cloning your fork

## Create a test case

If you have opened an [issue](https://github.com/infinispan/infinispan/issues) but are not comfortable enough to
contribute code directly, creating a self-contained test case is a good first step towards contributing.

Just fork the repository, build your test case and attach it as an archive to
an [issue](https://github.com/infinispan/infinispan/issues)

## Create a topic branch

Create a "topic" branch on which you will work. The convention is to name the branch
using the issue key. If there is not already an issue covering the work you
want to do, create one. Assuming you will be working from the main branch and working
on issue 12345:

```shell
git checkout -b 12345 origin/main
```

## Code

Code away...

## Formatting rules and style conventions

The Infinispan family projects share the same style conventions. Please refer to
our [Contributing Guide](https://infinispan.org/docs/dev/titles/contributing/contributing.html) for more details.

## Debugging

If you need to debug a test running inside a container, such as the tests in `server/tests`, you will need to start a
debugger in listening/server mode on port 5005. Set any breakpoints you need and then launch the test with the
`org.infinispan.test.server.container.debug` system property set to the index of the container you want to debug
(usually `0` for the first container):

```
mvn verify -pl server/tests -Dit.test=RequestTracingIT -Dorg.infinispan.test.server.container.debug=0
```

## Commit

* Make commits of logical units.
* Be sure to start the commit messages with the issue key you are working on. This allows easy cross-referencing on
  GitHub.
* Avoid formatting changes to existing code as much as possible: they make the intent of your patch less clear.
* Make sure you have added the necessary tests for your changes.
* Run _all_ the tests to assure nothing else was accidentally broken:

```shell
mvn verify
```

_Prior to committing, if you want to pull in the latest upstream changes (highly
appreciated by the way), please use rebasing rather than merging (see instructions below). Merging creates
"merge commits" that really muck up the project timeline._

Add the original Infinispan repository as a remote repository called upstream:

```shell
git remote add upstream git@github.com:infinispan/infinispan.git
```

If you want to rebase your branch on top of the main branch, you can use the following git command:

```shell
git pull --rebase upstream main
```

## Use of Generative AI

Generative AI tools may be used to assist in writing code, tests, or documentation, provided that you fully understand every change you submit. The goal is to keep Infinispan's code consistent and high-quality while respecting reviewers' limited time.

If you use generative AI to assist with your contribution, all the following are required:

* You understand the change. You must be able to explain what your code does and why. Submitting AI-generated code you do not understand is not acceptable.
* You engage with review feedback. You are expected to respond to questions and comments from reviewers. If you use AI to help draft responses, you must edit and proofread them to ensure they are accurate and address the reviewer's point.
* You can revise the code yourself. If a reviewer requests changes, you are responsible for addressing them, even if your AI tool is unable to produce a suitable fix.
* You disclose AI agents usage. Include a note in the PR description indicating the usage of AI agents for generating complete solutions from a prompt (i.e. you do not need to mention a simple AI autocomplete). This helps reviewers calibrate their review.
* You ensure licensing compliance. All generated code must be released under the Apache License, Version 2.0, the same license as Infinispan. You are responsible for verifying that the tools you use do not introduce additional licensing restrictions.

## Submit

* Push your changes to a topic branch in your fork of the repository.
* Initiate a [pull request](http://help.github.com/send-pull-requests/).
* Cross-reference the issue key in the pull request description.
