# Running in the container:

The test in the module should match one of the following rules:

* Classloading 1: see if all the modules can be correctly initialized
* Classloading 2: see if extension points work with user classes (marshalling, listeners, CDI annotations)
* Security: see if things still work when running the container with a security manager

## Module separation

### server-integration-commons
It contains tests that can be running inside a container.

### third-party-server
It run the tests from `server-integration-commons` and could have specific tests for a container.

### wildfly-modules
It run the tests from `server-integration-commons` and could have specific tests for a wildfly-modules.

## Debug
To export shrinkwrap archive to a file use the following
```java
war.as(ZipExporter.class).exportTo(new File("/path/to/my.war"), true);
```
