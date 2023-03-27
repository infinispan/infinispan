You can read more about how to build Infinispan in the documentation:

link:https://infinispan.org/docs/dev/titles/contributing/contributing.html[Infinispan Contributor's Guide]

Provided you already have the correct versions of Java and Maven installed, you can get started right away.
For convenience you can use the provided maven-settings.xml file which enables all additional repositories required for
building Infinispan:

  ./mvnw -s maven-settings.xml clean install -DskipTests

Available profiles
==================

* *distribution* Builds the full distribution
* *java-alt-test* Runs tests using an older JDK compared to the one required to build. Requires setting the `JAVA_ALT_HOME` environment variable. 

