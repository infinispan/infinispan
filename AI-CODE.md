## Tech Stack
* **Java Version:** 25, use JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64/ (compiler release target is 17 for embedded modules, 25 for server modules — see `maven.compiler.release` in the root pom.xml or in the module pom for overrides)
* **Build Tool:** Maven (Multi-module)
* **Key Frameworks:** JUnit 5, AssertJ

## Project Architecture

* **build/bom:** BOM to be used by other projects to set versions for Infinispan-specific dependencies
* **build/configuration:** dependency versions, configuration and branding properties.
* **api:** Common API. Depended on by almost every other module.
* **commons/all:** Shared utilities. Low-level helpers used across modules.
* **core:** Core implementation for embedded caches. Most modules depend on this.
* **counter:** Clustered counters implementation. Depends on core.
* **lock:** Clustered lock implementation. Depends on core.
* **multimap:** Multimap implementation. Depends on core.
* **query:** Embedded query implementation. Depends on core and Hibernate Search.
* **client/hotrod-client:** Hot Rod client. Independent of server modules.
* **server/core:** Base server module. Foundation for all protocol modules.
* **server/resp:** Server module implementing the Redis protocol. Depends on server/core.
* **server/memcached:** Server module implementing the Memcached protocol. Depends on server/core.
* **server/hotrod:** Server module implementing the Hot Rod protocol. Depends on server/core.
* **server/router:** Server module implementing the single-port router. Depends on protocol modules.
* **server/runtime:** Server integration module that aggregates all protocol modules — build this to test end-to-end server changes.
* **server/tests:** Server integration testsuite. Used for verifying that the modules work together as well as ensuring codepaths work when authorization is applied.

## Common Build Commands
* **Full build (skip tests):** `mvn install -DskipTests -Dcheckstyle.skip`
* **Build a single module:** `mvn install -pl core -DskipTests -Dcheckstyle.skip`
* **Build a module and its dependencies:** `mvn install -pl server/runtime -am -DskipTests -Dcheckstyle.skip`
* **Run a single test class:** `mvn verify -pl core -Dtest=SomeTest`
* **Run a single integration test:** `mvn verify -pl server/tests -Dit.test=SomeTestIT`
* **Run a single test method:** `mvn verify -pl core -Dtest=SomeTest#testMethod`
* **Format code before committing:** `mvn spotless:apply`

## Module-specific instructions
When working in a specific module, check for an AI-CODE.md in that module's directory for additional guidelines.

## Development Standards
* **Style:** Functional programming patterns where possible; use Records for DTOs.
* **Javadocs:** Required for public API classes and methods. Optional for internal code (but encouraged where they help developers). Unnecessary for test classes and methods.
* **Testing:** Aim for 75% coverage as a guideline, not a hard target. Focus test effort on meaningful logic and edge cases — do not work too hard to cover boilerplate exception handlers and trivial branches. Enable coverage collection with Jacoco using the `-Pcoverage` maven profile.
* **Coding style:** Avoid checkstyle checks using `-Dcheckstyle.skip` and focus on implementation. Use `mvn spotless:apply` to format the code before committing.
* **Commit logs:** commit logs must always start with a line at the top formatted as `[#00000] Summary`.
* **PRs:** Use the pull request template in `.github/pull_request_template.md` for PR descriptions.
* **Git branches:** branches should always be named `issueid/issue_summary` and use `origin/main` as the upstream

## Development Platform
* **Issues:** Use GitHub issue types (Bug, Feature, Task, Epic) instead of labels for classification. Each type has a template in `.github/ISSUE_TEMPLATE/` that must be filled in when opening issues.

## Related projects

* **Operator:** The Infinispan Operator source code is in ../infinispan-operator
* **Website:** The Infinispan website source code is in ../infinispan.github.io
* **ProtoStream:** The ProtoStream source code is in ../protostream
* **Console:** The Infinispan Console source code is in ../infinispan-console
