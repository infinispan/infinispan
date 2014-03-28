package org.infinispan.objectfilter.test.model;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class Person {

   private String name;

   private String surname;

   private Address address;

   private List<PhoneNumber> phoneNumbers;

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

   public Address getAddress() {
      return address;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   public List<PhoneNumber> getPhoneNumbers() {
      return phoneNumbers;
   }

   public void setPhoneNumbers(List<PhoneNumber> phoneNumbers) {
      this.phoneNumbers = phoneNumbers;
   }

   @Override
   public String toString() {
      return "Person{" +
            "name='" + name + '\'' +
            ", surname='" + surname + '\'' +
            ", phoneNumbers=" + phoneNumbers +
            ", address=" + address +
            '}';
   }
}
