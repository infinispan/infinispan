package org.infinispan.persistence.jdbc.stringbased;

import java.io.Serializable;

/**
 * Pojo used for testing jdbc caches stores.
 *
 * @author Mircea.Markus@jboss.com
 */
public class Person implements Serializable {
   
   /** The serialVersionUID */
   private static final long serialVersionUID = -835015913569270262L;
   
   private String name;
   private String surname;
   private int age;
   private int hashCode = -1;

   public Person(String name, String surname, int age) {
      this.name = name;
      this.surname = surname;
      this.age = age;
   }

   public String getName() {
      return name;
   }

   public String getSurname() {
      return surname;
   }

   public int getAge() {
      return age;
   }

   public void setHashCode(int hashCode) {
      this.hashCode = hashCode;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Person)) return false;

      Person person = (Person) o;

      if (age != person.age) return false;
      if (name != null ? !name.equals(person.name) : person.name != null) return false;
      if (surname != null ? !surname.equals(person.surname) : person.surname != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      if (hashCode != -1) {
         return hashCode;
      }
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (surname != null ? surname.hashCode() : 0);
      result = 31 * result + age;
      return result;
   }
}
