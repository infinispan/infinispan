# Instructions on the documentation process

This file is not fleshed out, it's just a collection of tips.
A lot of inspiration has been gained from the 
[Hibernate-OGM](https://github.com/hibernate/hibernate-ogm/tree/master/hibernate-ogm-documentation) 
project documentation, as well as the [AsciiDoctor website sources](https://github.com/asciidoctor/asciidoctor.org),
which also uses AsciiDoc for documentation as well as 
[Awestruct](http://www.awestruct.org) to build and publish
the website.

## AsciiDoc

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

## Diagrams

Diagrams are done in OmniGraffle and stored as XML files in `src/main/omnigraffle`.
Export the omnigraffle files as `png` with a dot per inch of 72. This will create
a file of the right size for the web.

Binary images should be stored under `documentation/src/main/asciidoc/images`

## Building and rendering documents
A `pom.xml` file is included here, but this is _*experimental*_ and probably will
not work.

TIP: Don't bother with it.

There are scripts in the `https://github.com/infinispan/infinispan.github.io`
repository which is capable of grabbing docs from here and building/integrating
the docs as a part of the Infinispan.org website.  This is the preferred method
of rendering the documentation.

### Live editing
Naturally, while editing the docs, you don't want to have to build the entire
website to see your changes.  A good way to do this is to set up _live previews_
as described [here](http://asciidoctor.org/docs/editing-asciidoc-with-live-preview/).
