package org.infinispan.api.client.impl;

import java.util.Objects;

import org.infinispan.protostream.annotations.ProtoDoc;
import org.infinispan.protostream.annotations.ProtoField;

@ProtoDoc("@Indexed")
public final class Person {

   @ProtoDoc("@Field")
   @ProtoField(number = 1, required = true)
   String id;

   @ProtoDoc("@Field")
   @ProtoField(number = 2, required = true)
   String firstName;

   @ProtoDoc("@Field")
   @ProtoField(number = 3, required = true)
   String lastName;

   @ProtoDoc("@Field")
   @ProtoField(number = 4, required = true)
   int bornYear;

   @ProtoDoc("@Field")
   @ProtoField(number = 5, required = true)
   String bornIn;

   @ProtoDoc("@Field")
   @ProtoField(number = 6, required = true)
   Address address;

   public Person() {

   }

   public Person(String firstName, String lastName, int bornYear, String bornIn) {
      this.id = SearchUtil.id();
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
            "id='" + id + '\'' +
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
            Objects.equals(id, person.id) &&
            Objects.equals(firstName, person.firstName) &&
            Objects.equals(lastName, person.lastName) &&
            Objects.equals(bornIn, person.bornIn) &&
            Objects.equals(address, person.address);
   }

   @Override
   public int hashCode() {
      return Objects.hash(firstName, lastName, bornYear, bornIn, address);
   }

   public String getId() {
      return id;
   }

   public void setId(String id) {
      this.id = id;
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

   public Person copy() {
      Person person = new Person(this.firstName, this.lastName, this.bornYear, this.bornIn);
      person.id = this.id;
      person.address = this.address;
      return person;
   }
}
