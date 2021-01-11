package org.infinispan.sample;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.*;

// Annotate values with @Indexed to add them to indexes
// Annotate each fields according to how you want to index it
@Indexed
public class Book {
   @FullTextField
   String title;

   @FullTextField
   String description;

   @KeywordField
   String isbn;

   @GenericField
   LocalDate publicationDate;

   @IndexedEmbedded
   Set<Author> authors = new HashSet<Author>();
}
