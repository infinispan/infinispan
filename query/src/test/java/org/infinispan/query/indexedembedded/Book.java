package org.infinispan.query.indexedembedded;

import java.io.Serializable;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;

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

   @Text
   public String getTitle() {
      return title;
   }

   @Basic
   public String getAuthor() {
      return author;
   }

   @Basic
   public String getEditor() {
      return editor;
   }
}
