package org.infinispan.api.client.impl;

import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public final class Person {

   @ProtoDoc("@Field")
   @ProtoField(number = 1, required = true)
   String firstName;

   @ProtoDoc("@Field")
   @ProtoField(number = 2, required = true)
   String lastName;

   @ProtoDoc("@Field")
   @ProtoField(number = 3, required = true)
   int bornYear;

   @ProtoDoc("@Field")
   @ProtoField(number = 4, required = true)
   String bornIn;

   @ProtoDoc("@Field")
   @ProtoField(number = 5, required = true)
   Address address;

   public Person() {

   }

   public Person(String firstName, String lastName, int bornYear, String bornIn) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.bornYear = bornYear;
      this.bornIn = bornIn;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   @Override
   public String toString() {
      return "Person{" +
            "firstName='" + firstName + '\'' +
            ", lastName='" + lastName + '\'' +
            ", bornYear='" + bornYear + '\'' +
            ", bornIn='" + bornIn + '\'' +
            ", address='" + address + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Person person = (Person) o;
      return bornYear == person.bornYear &&
            Objects.equals(firstName, person.firstName) &&
            Objects.equals(lastName, person.lastName) &&
            Objects.equals(bornIn, person.bornIn) &&
            Objects.equals(address, person.address);
   }

   @Override
   public int hashCode() {
      return Objects.hash(firstName, lastName, bornYear, bornIn, address);
   }

   public String getFirstName() {
      return firstName;
   }

   public String getLastName() {
      return lastName;
   }

   public int getBornYear() {
      return bornYear;
   }

   public String getBornIn() {
      return bornIn;
   }

   public Address getAddress() {
      return address;
   }
}
