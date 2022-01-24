package org.infinispan.server.test.client.hotrod;

import java.io.Serializable;

/**
 * @author Pedro Ruivo
 * @since 9.4
 */
public class Person implements Serializable {

   final String name;

   public Person(String name) {
      this.name = name;
   }

   public String getName() {
      return name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Person person = (Person) o;

      if (!name.equals(person.name)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return name.hashCode();
   }
}
