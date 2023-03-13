# How to build native

1. Download and extract the Java 17 version of GraalVM (>= 22.3) from
https://github.com/graalvm/graalvm-ce-builds/releases
for your distribution.
2. Install `native-image` in the `bin` directory of the extracted graal
runtime
   * `gu install native-image`
3. Set the `GRAALVM_HOME` environment variable to the extracted
graal runtime
4. Build the project
   * `mvn clean install -P native -am -pl quarkus`
