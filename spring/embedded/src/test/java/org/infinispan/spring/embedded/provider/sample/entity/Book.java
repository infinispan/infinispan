package org.infinispan.spring.embedded.provider.sample.entity;

import java.io.Serializable;

/**
 * Book.
 *
 * @author Olaf Bergner
 * @since 5.1
 */
public class Book implements Serializable {

   private Integer id;

   private String isbn;

   private String author;

   private String title;

   public Book() {
   }

   public Book(String isbn, String author, String title) {
      this(null, isbn, author, title);
   }

   public Book(Integer id, String isbn, String author, String title) {
      this.id = id;
      this.isbn = isbn;
      this.author = author;
      this.title = title;
   }

   public Integer getId() {
      return id;
   }

   public void setId(Integer id) {
      this.id = id;
   }

   public String getIsbn() {
      return isbn;
   }

   public void setIsbn(String isbn) {
      this.isbn = isbn;
   }

   public String getAuthor() {
      return author;
   }

   public void setAuthor(String author) {
      this.author = author;
   }

   public String getTitle() {
      return title;
   }

   public void setTitle(String title) {
      this.title = title;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((author == null) ? 0 : author.hashCode());
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      result = prime * result + ((isbn == null) ? 0 : isbn.hashCode());
      result = prime * result + ((title == null) ? 0 : title.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      Book other = (Book) obj;
      if (author == null) {
         if (other.author != null) return false;
      } else if (!author.equals(other.author)) return false;
      if (id == null) {
         if (other.id != null) return false;
      } else if (!id.equals(other.id)) return false;
      if (isbn == null) {
         if (other.isbn != null) return false;
      } else if (!isbn.equals(other.isbn)) return false;
      if (title == null) {
         if (other.title != null) return false;
      } else if (!title.equals(other.title)) return false;
      return true;
   }

   @Override
   public String toString() {
      return "Book [id=" + id + ", isbn=" + isbn + ", author=" + author + ", title=" + title + "]";
   }
}
