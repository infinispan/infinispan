package org.infinispan.test.integration.as.query;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

@Indexed
public final class Book implements Serializable {

   public Book(String title) {
      this.title = title;
   }

   @Field
   final String title;

}
