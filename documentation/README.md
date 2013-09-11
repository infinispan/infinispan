# Instructions on the documentation process

This file is not fleshed out, it's just a collection of tips.
A lot of inspiration has been gained from the 
[Hibernate-OGM](https://github.com/hibernate/hibernate-ogm/tree/master/hibernate-ogm-documentation) 
project documentation, as well as the [AsciiDoctor website sources](https://github.com/asciidoctor/asciidoctor.org),
which also uses AsciiDoc for documentation as well as 
[Awestruct](http://www.awestruct.org) to build and publish
the website.

## Authoring documents
The docs make use of [AsciiDoc](http://en.wikipedia.org/wiki/AsciiDoc) as a markup language.
It is lightweight, easy to use, intuitive, and (thanks to [AsciiDoctor](http://asciidoctor.org/) 
has powerful tooling to transform the docs to HTML, PDF, DocBook and other formats.

### Style guide
[AsciiDoctor](http://asciidoctor.org/) has some excellent resources on authoring
documentation effectively, including:

* [Syntax Quick Reference](http://asciidoctor.org/docs/asciidoc-syntax-quick-reference/)
* [Writers' Guide](http://asciidoctor.org/docs/asciidoc-writers-guide/)
* [Style Guide] (http://asciidoctor.org/docs/asciidoc-recommended-practices/)

### Editing
You will want to install the entire AsciiDoctor toolchain on your computer.
* [Installing AsciiDoctor](http://asciidoctor.org/docs/install-toolchain/#installing-or-updating-asciidoctor)
  * [On a Mac](http://asciidoctor.org/docs/install-asciidoctor-macosx/)
* [Text editors](http://asciidoctor.org/docs/install-toolchain/#text-editors-and-syntax-highlighting)

### Linefeed
A soft limit of 80 characters is recommended.
At the end of each sentence, go to the next line.
Consider going to the next line for each new clause,
in particular if the sentence would go beyond 80 characters.
But do not obsess: if a multi-clause sentence is below 80 characters,
don't split it to limit the _verticality_ of the document.
For long links, tend to go to the next line.

The 80 characters limit is used because GitHub diffs are around 90 chars long.

For more information, read
[this blog post](http://emmanuelbernard.com/blog/2013/08/08/one-line-per-idea/)

### Diagrams
Diagrams are done in OmniGraffle and stored as XML files in `src/main/omnigraffle`.
Export the omnigraffle files as `png` with a dot per inch of 72. This will create
a file of the right size for the web.

Binary images should be stored under `documentation/src/main/asciidoc/images`

## Building and rendering documents
A `pom.xml` file is included here, but this is _*experimental*_ and probably will
not work.

*TIP:* Don't bother with it.

There are scripts in the 
[Infinispan Website](https://github.com/infinispan/infinispan.github.io) repository
which are capable of grabbing docs from here and building/integrating the docs as a
part of the Infinispan.org website.
This is the preferred method of rendering the documentation.

### Live editing
Naturally, while editing the docs, you don't want to have to build the entire website to see your changes.
A good way to do this is to set up _live previews_ as described
[here](http://asciidoctor.org/docs/editing-asciidoc-with-live-preview/).

#### Guardfile
A `Guardfile` is included here in this repository which should be used instead of the `Guardfile` as specified in the link above.
This will ensure proper application of stylesheets, etc.

*NOTE:* Aggregate files (i.e., using AsciiDoc's `include` directive) does _not_ work with live previews.
However, these will be rendered correctly when the site is properly built.
