package org.infinispan.query.indexedembedded;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

@Indexed
public class Book implements Serializable {

   String title;
   String author;
   String editor;

   public Book(String title, String author, String editor) {
      this.title = title;
      this.author = author;
      this.editor = editor;
   }

   @Field
   public String getTitle() {
      return title;
   }

   @Field
   public String getAuthor() {
      return author;
   }

   @Field
   public String getEditor() {
      return editor;
   }
}
