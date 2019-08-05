package org.infinispan.query.test;

import java.util.Objects;
import java.util.Set;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;

@Indexed
public class Book {

   @Field
   private String title;

   @Field
   private String description;

   @IndexedEmbedded
   private Set<Author> authors;

   public Book(String title, String description, Set<Author> authors) {
      this.title = title;
      this.description = description;
      this.authors = authors;
   }

   public String getTitle() {
      return title;
   }

   public void setTitle(String title) {
      this.title = title;
   }

   public String getDescription() {
      return description;
   }

   public void setDescription(String description) {
      this.description = description;
   }

   public Set<Author> getAuthors() {
      return authors;
   }

   public void setAuthors(Set<Author> authors) {
      this.authors = authors;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Book)) return false;
      Book other = (Book) o;
      return Objects.equals(title, other.title) &&
            Objects.equals(description, other.description) &&
            Objects.equals(authors, other.authors);
   }

   @Override
   public int hashCode() {
      return Objects.hash(title, description, authors);
   }
}
