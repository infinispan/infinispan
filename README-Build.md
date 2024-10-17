You can read more about how to build Infinispan in the documentation: [Infinispan Contributor's Guide](https://infinispan.org/docs/dev/titles/contributing/contributing.html)

Provided you already have the correct versions of Java and Maven installed, you can get started right away.

  ./mvnw clean install -DskipTests

Available profiles
==================

* *distribution* Builds the full distribution
* *java-alt-test* Runs tests using an older JDK compared to the one required to build. Requires setting the `JAVA_ALT_HOME` environment variable. 

