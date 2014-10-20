package org.infinispan.spring.provider.sample;

/**
 * Book.
 *
 * @author <a href="mailto:olaf DOT bergner AT gmx DOT de">Olaf Bergner</a>
 * @since 5.1
 */
public class Book {

   private Integer id;

   private String isbn;

   private String author;

   private String title;

   public Book() {
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
