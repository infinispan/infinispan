package org.infinispan.test.data;

import java.io.Serializable;

import org.infinispan.marshall.core.ExternalPojo;

public class Person implements Serializable, ExternalPojo {

   private static final long serialVersionUID = -885384294556845285L;

   String name = null;
   Address address;

   public Person() {
      // Needed for serialization
   }

   public Person(String name) {
      this.name = name;
   }

   public Person(String name, Address address) {
      this.name = name;
      this.address = address;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public void setName(Object obj) {
      this.name = (String) obj;
   }

   public Address getAddress() {
      return address;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("name=").append(getName()).append(" Address= ").append(address);
      return sb.toString();
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final Person person = (Person) o;

      if (address != null ? !address.equals(person.address) : person.address != null) return false;
      if (name != null ? !name.equals(person.name) : person.name != null) return false;

      return true;
   }

   public int hashCode() {
      int result;
      result = (name != null ? name.hashCode() : 0);
      result = 29 * result + (address != null ? address.hashCode() : 0);
      return result;
   }
}
