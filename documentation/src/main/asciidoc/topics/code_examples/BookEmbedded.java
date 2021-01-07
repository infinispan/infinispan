package org.infinispan.sample;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.search.mapper.pojo.mapping.definition.annotation.*;

//Values you want to index need to be annotated with @Indexed, then you pick which fields and how they are to be indexed:
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