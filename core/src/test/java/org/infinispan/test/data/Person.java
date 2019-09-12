package org.infinispan.test.data;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class Person implements Serializable {

   String name;
   Address address;

   public Person() {
      // Needed for serialization
   }

   public Person(String name) {
      this(name, null);
   }

   @ProtoFactory
   public Person(String name, Address address) {
      this.name = name;
      this.address = address;
   }

   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @ProtoField(number = 2)
   public Address getAddress() {
      return address;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   @Override
   public String toString() {
      return "Person{" +
            "name='" + name + '\'' +
            ", address=" + address +
            '}';
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
