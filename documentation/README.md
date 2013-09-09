# Read me on the documentation process

This file is not fleshed out, it's just a collection of tips.
A lot of inspiration has been gained from the 
[Hibernate-OGM](https://github.com/hibernate/hibernate-ogm/tree/master/hibernate-ogm-documentation) 
project documentation.

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

