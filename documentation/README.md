# Infinispan Documentation

Tips to get started with Infinispan documentation.

## Documentation Guidelines

Start by reading the [Documentation Guidelines](http://infinispan.org/docs/stable/contributing/contributing.html#documentation_guidelines) in the _Contributer's Guide_.

## Tooling

Install the complete AsciiDoctor toolchain. See the following:
* [Installing AsciiDoctor](http://asciidoctor.org/docs/install-toolchain/#installing-or-updating-asciidoctor)
  * [Mac OS](http://asciidoctor.org/docs/install-asciidoctor-macosx/)
* [Text editors](http://asciidoctor.org/docs/install-toolchain/#text-editors-and-syntax-highlighting)

## Building Documentation

Use _live previews_ to review your changes while editing or contributing
content. See [Editing AsciiDoc with Live Preview](http://asciidoctor.org/docs/editing-asciidoc-with-live-preview/).

Run the _asciidoctor_ command against the main book file to build HTML locally.
For example, to build the _User Guide_ locally, run:

```bash
$ asciidoctor user_guide.asciidoc
```

**Tips:**

- Use the `Guardfile` in the documentation repository to apply stylesheets correctly if you use Guard to monitor changes and regenerate HTML as you edit.
- The `pom.xml` for documentation is _*experimental*_ and is not
currently functional.

## Publishing Documentation

The [Infinispan Website](https://github.com/infinispan/infinispan.github.io)
hosts public documentation for each release. Documentation source files are
pulled from this repository and included in the website build process.
