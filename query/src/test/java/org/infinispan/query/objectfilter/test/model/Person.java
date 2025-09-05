package org.infinispan.query.objectfilter.test.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.infinispan.protostream.annotations.ProtoComment;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class Person {

   public enum Gender {
      @ProtoEnumValue
      MALE,
      @ProtoEnumValue(1)
      FEMALE
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

   private Integer _location;

   @ProtoField(value = 1, defaultValue = "0")
   public int getId() {
      return _id;
   }

   public void setId(int id) {
      this._id = id;
   }

   @ProtoField(2)
   public String getName() {
      return _name;
   }

   public void setName(String name) {
      this._name = name;
   }

   @ProtoField(3)
   public String getSurname() {
      return _surname;
   }

   public void setSurname(String surname) {
      this._surname = surname;
   }

   @ProtoField(4)
   public Address getAddress() {
      return _address;
   }

   public void setAddress(Address address) {
      this._address = address;
   }

   @ProtoField(value = 5, collectionImplementation = ArrayList.class)
   public List<PhoneNumber> getPhoneNumbers() {
      return _phoneNumbers;
   }

   public void setPhoneNumbers(List<PhoneNumber> phoneNumbers) {
      this._phoneNumbers = phoneNumbers;
   }


   @ProtoField(value = 6, defaultValue = "0")
   public int getAge() {
      return _age;
   }

   public void setAge(int age) {
      this._age = age;
   }

   @ProtoField(value = 7, collectionImplementation = ArrayList.class)
   public List<Integer> getFavouriteNumbers() {
      return _favouriteNumbers;
   }

   public void setFavouriteNumbers(List<Integer> favouriteNumbers) {
      this._favouriteNumbers = favouriteNumbers;
   }

   @ProtoField(8)
   public String getLicense() {
      return _license;
   }

   public void setLicense(String license) {
      this._license = license;
   }

   @ProtoField(9)
   public Gender getGender() {
      return _gender;
   }

   public void setGender(Gender gender) {
      this._gender = gender;
   }

   @ProtoField(10)
   public Date getLastUpdate() {
      return _lastUpdate;
   }

   public void setLastUpdate(Date lastUpdate) {
      this._lastUpdate = lastUpdate;
   }

   @ProtoField(value = 11, defaultValue = "false")
   public boolean isDeleted() {
      return _deleted;
   }

   public void setDeleted(boolean deleted) {
      this._deleted = deleted;
   }

   @ProtoField(value = 12)
   @ProtoComment("Not an actual spatial property! Just makes the test fail with a meaningful message.")
   public Integer getLocation() {
      return _location;
   }

   public void setLocation(Integer location) {
      this._location = location;
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
            ", location=" + _location +
            '}';
   }
}
