package org.infinispan.objectfilter.test.model;

import java.util.Date;
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

   private int _id;

   private String _surname;

   private Address _address;

   private int _age;

   private List<Integer> _favouriteNumbers;

   private List<PhoneNumber> _phoneNumbers;

   private String _license;

   private Gender _gender;

   private Date _lastUpdate;

   private boolean _deleted;

   public int getId() {
      return _id;
   }

   public void setId(int id) {
      this._id = id;
   }

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

   public List<Integer> getFavouriteNumbers() {
      return _favouriteNumbers;
   }

   public void setFavouriteNumbers(List<Integer> favouriteNumbers) {
      this._favouriteNumbers = favouriteNumbers;
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

   public Date getLastUpdate() {
      return _lastUpdate;
   }

   public void setLastUpdate(Date lastUpdate) {
      this._lastUpdate = lastUpdate;
   }

   public boolean isDeleted() {
      return _deleted;
   }

   public void setDeleted(boolean deleted) {
      this._deleted = deleted;
   }

   @Override
   public String toString() {
      return "Person{" +
            "id='" + _id + '\'' +
            ", name='" + _name + '\'' +
            ", surname='" + _surname + '\'' +
            ", phoneNumbers=" + _phoneNumbers +
            ", address=" + _address +
            ", age=" + _age +
            ", favouriteNumbers=" + _favouriteNumbers +
            ", license=" + _license +
            ", gender=" + _gender +
            ", lastUpdate=" + _lastUpdate +
            ", deleted=" + _deleted +
            '}';
   }
}
