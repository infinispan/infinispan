package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@Indexed
public class Book implements Serializable {

   @Field
   private String title;

   @Field
   private String publisher;

   @Field(analyze = Analyze.NO)
   private String isbn;

   @Field
   private String preface;

   @IndexedEmbedded
   private Author author;

   public Book(String title, String publisher, Author author, String preface) {
      this.title = title;
      this.publisher = publisher;
      this.author = author;
      this.preface = preface;
   }

   public String getTitle() {
      return title;
   }

   public void setTitle(String title) {
      this.title = title;
   }

   public String getPublisher() {
      return publisher;
   }

   public void setPublisher(String publisher) {
      this.publisher = publisher;
   }

   public String getIsbn() {
      return isbn;
   }

   public void setIsbn(String isbn) {
      this.isbn = isbn;
   }

   public String getPreface() {
      return preface;
   }

   public void setPreface(String preface) {
      this.preface = preface;
   }

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
