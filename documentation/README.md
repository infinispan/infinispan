# Instructions on the documentation process

This file is not fleshed out, it's just a collection of tips.
A lot of inspiration has been gained from the 
[Hibernate-OGM](https://github.com/hibernate/hibernate-ogm/tree/master/hibernate-ogm-documentation) 
project documentation, as well as the [AsciiDoctor website sources](https://github.com/asciidoctor/asciidoctor.org),
which also uses AsciiDoc for documentation as well as 
[Awestruct](http://www.awestruct.org) to build and publish
the website.

## Authoring documents
Read the _Contributing To Infinispan Guide's_ section on [Writing Documentation](http://www.infinispan.org/docs/6.0.x/contributing/contributing.html#_writing_documentation_and_faqs) for more details.

### Editing
You will want to install the entire AsciiDoctor toolchain on your computer.
* [Installing AsciiDoctor](http://asciidoctor.org/docs/install-toolchain/#installing-or-updating-asciidoctor)
  * [On a Mac](http://asciidoctor.org/docs/install-asciidoctor-macosx/)
* [Text editors](http://asciidoctor.org/docs/install-toolchain/#text-editors-and-syntax-highlighting)

## Building and rendering documents
A `pom.xml` file is included here, but this is _*experimental*_ and probably will
not work.

*TIP:* Don't bother with it.

### Publishing
There are scripts in the 
[Infinispan Website](https://github.com/infinispan/infinispan.github.io) repository
which are capable of grabbing docs from here and building/integrating the docs as a
part of the Infinispan.org website.

*NOTE:* This is the preferred method of rendering the documentation.

### Live editing
Naturally, while editing the docs, you don't want to have to build the entire website to see your changes.
A good way to do this is to set up _live previews_ as described
[here](http://asciidoctor.org/docs/editing-asciidoc-with-live-preview/).

#### Guardfile
A `Guardfile` is included here in this repository which should be used instead of the `Guardfile` as specified in the link above.
This will ensure proper application of stylesheets, etc.

*NOTE:* Aggregate files (i.e., using AsciiDoc's `include` directive) does _not_ work with live previews.
However, these will be rendered correctly when the site is properly built.
