package org.infinispan.objectfilter.test.model;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class Person {

   public enum Gender {
      MALE, FEMALE
   }

   // fields start with underscore to demonstrate that property getter access is used instead of field access
   private String _name;

   private String _surname;

   private Address _address;

   private int _age;

   private List<PhoneNumber> _phoneNumbers;

   private String _license;

   private Gender _gender;

   public String getName() {
      return _name;
   }

   public void setName(String name) {
      this._name = name;
   }

   public String getSurname() {
      return _surname;
   }

   public void setSurname(String surname) {
      this._surname = surname;
   }

   public Address getAddress() {
      return _address;
   }

   public void setAddress(Address address) {
      this._address = address;
   }

   public int getAge() {
      return _age;
   }

   public void setAge(int age) {
      this._age = age;
   }

   public List<PhoneNumber> getPhoneNumbers() {
      return _phoneNumbers;
   }

   public void setPhoneNumbers(List<PhoneNumber> phoneNumbers) {
      this._phoneNumbers = phoneNumbers;
   }

   public String getLicense() {
      return _license;
   }

   public void setLicense(String license) {
      this._license = license;
   }


   public Gender getGender() {
      return _gender;
   }

   public void setGender(Gender gender) {
      this._gender = gender;
   }

   @Override
   public String toString() {
      return "Person{" +
            "name='" + _name + '\'' +
            ", surname='" + _surname + '\'' +
            ", phoneNumbers=" + _phoneNumbers +
            ", address=" + _address +
            ", age=" + _age +
            ", license=" + _license +
            ", gender=" + _gender +
            '}';
   }
}
