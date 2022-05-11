package org.infinispan.query.test;

import java.util.Objects;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.api.annotations.indexing.Text;

@Indexed
public class Author {

   private String name;

   private String surname;

   public Author(String name, String surname) {
      this.name = name;
      this.surname = surname;
   }

   @Text
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Basic
   public String getSurname() {
      return surname;
   }

   public void setSurname(String surname) {
      this.surname = surname;
   }

   @Override
   public int hashCode() {
      return Objects.hash(name, surname);
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null || getClass() != obj.getClass())
         return false;
      Author other = (Author) obj;
      return Objects.equals(name, other.name) && Objects.equals(surname, other.surname);
   }
}
