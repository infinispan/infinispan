package org.infinispan.sample;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.api.annotations.indexing.*;

// Annotate values with @Indexed to add them to indexes
// Annotate each fields according to how you want to index it
@Indexed
public class Book {
   @Keyword
   String title;

   @Text
   String description;

   @Keyword
   String isbn;

   @Basic
   LocalDate publicationDate;

   @Embedded
   Set<Author> authors = new HashSet<Author>();
}
