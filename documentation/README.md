# Infinispan Documentation

Tips to get started with Infinispan documentation.

## Documentation Guidelines

Start by reading the [Documentation Guidelines](https://infinispan.org/docs/stable/titles/contributing/contributing.html#documentation_guidelines) in the _Contributor's Guide_.

## Requirements

No additional toolchain is required beyond a JDK and Maven. The build uses the [asciidoctor-maven-plugin](https://github.com/asciidoctor/maven-plugins), which runs Asciidoctor in-process via JRuby, so there is no need to install Ruby or AsciiDoctor separately.

## Building Documentation

From the repository root, run:

```bash
$ mvn install -Pdistribution -pl documentation
```

The other Infinispan modules must already be installed in your local Maven repository (for example by running `mvn install -Pdistribution` once). The generated HTML is written to `documentation/target/generated/<version>/html`. To preview it locally, see the _Serving Generated Documentation_ section.

## Generated Content

Besides converting AsciiDoc sources, the build aggregates information from other parts of the codebase into AsciiDoc (under `target/generated-asciidoc/`) before rendering:

* **Metrics** — `Metrics2Asciidoc` discovers all metrics registered through the `MetricInfo` service and generates a reference table (`metrics.adoc`).
* **Version compatibility** — the `Compatibility` class in _server/testdriver/core_ loads `compatibility.json` and renders the supported server version matrix for rolling upgrades (`compatibility.adoc`).
* **RESP commands** — `RespCommands2Asciidoc` lists every registered RESP command, grouped by family with ACL information, combined with author notes from `src/main/resources/resp-command-notes.properties` (`ref_redis_commands.adoc`).
* **REST API reference** — the _openapi-generator-maven-plugin_ converts the OpenAPI specification built in _server/rest_ into AsciiDoc under `generated-asciidoc/openapi`.
* **Configuration defaults** — the _infinispan-defaults-maven-plugin_ extracts default attribute values from the core and cache store jars (`target/default-attributes.adoc`), which topics include via the `{defaults}` attribute.

When building with `-Pdistribution`, two more reports are added: `XSDoc` renders the module XML schemas as HTML under `<version>/html/configuration-schema`, and a Saxon transformation of collected log messages produces the logging report described below.

## Serving Generated Documentation

After building, serve the generated HTML locally with the bundled JDK web server (`jwebserver`, requires JDK 18+). From the repository root:

```bash
$ mvn exec:exec@serve-docs -f documentation/pom.xml
```

Then open `http://127.0.0.1:8080/` in a browser and press Ctrl+C to stop the server. Use `-Ddocs.port=<port>` to change the port.

## Generating log reports

Use `import org.infinispan.logging.annotations.Description;` annotations to provide additional detail about Infinispan log messages.
Descriptions should help users identify and resolve errors.

1. Add the logging dependency to the respective `pom.xml`:
```xml
<dependency>
  <groupId>org.infinispan</groupId>
  <artifactId>infinispan-logging-processor</artifactId>
</dependency>
```

2. Include `@Description` annotations in `Log.java` files.
```
@Description("Provide a more detailed description of the causes and conditions that lead to the error as well as an actionable resolution.")
```

3. Build the distribution.
```
mvn clean install -Pdistribution -DskipTests
```

4. Check the generated HTML at:
```
documentation/target/generated/$version/html/logging/logs.html
```


## Publishing Documentation

The [Infinispan Website](https://github.com/infinispan/infinispan.github.io)
hosts public documentation for each release. Documentation source files are
pulled from this repository and included in the website build process.
