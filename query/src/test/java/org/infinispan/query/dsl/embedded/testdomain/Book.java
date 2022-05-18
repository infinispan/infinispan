package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
public class Book implements Serializable {

   private String title;

   private String publisher;

   private String isbn;

   private String preface;

   private Author author;

   public Book(String title, String publisher, Author author, String preface) {
      this.title = title;
      this.publisher = publisher;
      this.author = author;
      this.preface = preface;
   }

   @Basic
   public String getTitle() {
      return title;
   }

   public void setTitle(String title) {
      this.title = title;
   }

   @Text
   public String getPublisher() {
      return publisher;
   }

   public void setPublisher(String publisher) {
      this.publisher = publisher;
   }

   @Basic
   public String getIsbn() {
      return isbn;
   }

   public void setIsbn(String isbn) {
      this.isbn = isbn;
   }

   @Text
   public String getPreface() {
      return preface;
   }

   public void setPreface(String preface) {
      this.preface = preface;
   }

   @Embedded
   public Author getAuthor() {
      return author;
   }

   public void setAuthor(Author author) {
      this.author = author;
   }

   @Override
   public String toString() {
      return "Book{" +
            "title='" + title + '\'' +
            ", publisher='" + publisher + '\'' +
            ", isbn='" + isbn + '\'' +
            ", author=" + author +
            ", preface='" + preface + '\'' +
            '}';
   }
}
