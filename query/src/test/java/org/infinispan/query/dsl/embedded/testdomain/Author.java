package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;

import org.infinispan.api.annotations.indexing.Basic;

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

   @Basic(projectable = true)
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
   public String toString() {
      return "Author{" +
            "name='" + name + '\'' +
            ", surname='" + surname + '\'' +
            '}';
   }
}
