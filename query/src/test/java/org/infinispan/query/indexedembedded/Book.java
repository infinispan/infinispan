package org.infinispan.query.indexedembedded;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

@Indexed
public class Book implements Serializable {

   @Field String title;
   @Field String author;
   @Field String editor;

   public Book(String title, String author, String editor) {
      this.title = title;
      this.author = author;
      this.editor = editor;
   }

}
