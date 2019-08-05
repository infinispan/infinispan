package org.infinispan.query.test;

import java.util.Objects;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;

@Indexed
public class Author {

   @Field
   private String name;

   @Field
   private String surname;

   public Author(String name, String surname) {
      this.name = name;
      this.surname = surname;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

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
