You can read more about how to build Infinispan in the
documentation: [Infinispan Contributor's Guide](https://infinispan.org/docs/dev/titles/contributing/contributing.html)

Provided you already have the correct versions of Java and Maven installed, you can get started right away.

`./mvnw clean install -DskipTests`

Available profiles
==================

* *distribution* Builds the full distribution
* *java-alt-test* Runs tests using an older JDK compared to the one required to build. Requires setting the
  `JAVA_ALT_HOME` environment variable.
* *image* Builds a container image

Building an image
=================

You can build a server image using a locally built server. Use the following command:

`./mvnw -Pimage -am -pl server/image`

The image is built with the latest OpenJDK LTS distribution from https://adoptium.net/
You can use a specific release of Infinispan Server by adding the `-Dserver.dist=<url>` property to the above Maven
invocation. You can also use a specific JDK release via the `-Djdk.dist=<url>` property.

Building natives
================

In order to build the native CLI you need to install a version of [GraalVM](https://www.graalvm.org/downloads/) that
matches the JDK version required to build Infinispan. Set the `GRAALVM_HOME` environment variable to the GraalVM 
installation directory and use the following command:

`./mvnw -Pnative -am -pl quarkus/cli`
