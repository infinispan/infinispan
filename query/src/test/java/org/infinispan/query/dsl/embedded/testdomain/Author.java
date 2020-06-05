package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Store;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
public class Author implements Serializable {

   private String name;

   private String surname;

   public Author(String name, String surname) {
      this.name = name;
      this.surname = surname;
   }

   @Field(store = Store.YES)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Field
   public String getSurname() {
      return surname;
   }

   public void setSurname(String surname) {
      this.surname = surname;
   }

   @Override
   public String toString() {
      return "Author{" +
            "name='" + name + '\'' +
            ", surname='" + surname + '\'' +
            '}';
   }
}
