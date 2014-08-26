package org.infinispan.test.integration.as.query;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

@Indexed
public final class Book implements Serializable {

   public Book(String title, String publisher) {
      this.title = title;
      this.publisher = publisher;
   }

   @Field(analyze = Analyze.NO)
   final String publisher;

   @Field
   final String title;

}
