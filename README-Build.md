You can read more about how to build Infinispan in the documentation:

https://infinispan.org/docs/dev/titles/contributing/contributing.html

Provided you already have the correct versions of Java and Maven installed, you can get started right away.
For convenience you can use the provided maven-settings.xml file which enables all additional repositories required for
building Infinispan:

  `mvn -DskipTests -s maven-settings.xml clean install`

or use the provided build.sh or build.bat depending on your platform of choice.

Available profiles
==================

* *distribution* Builds the full distribution
* *java8-test*   Runs the testsuite using a Java 8 installation (the JAVA8_HOME environment variable must point to it)
