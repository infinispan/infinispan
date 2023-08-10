package org.infinispan.rest.search.entity;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.descriptors.Type;

/**
 * @since 9.2
 */
@Indexed
public class Person implements Serializable {

   private Integer id;

   private String name;

   private String surname;

   @Basic
   private Integer age;

   private Address address;

   private Gender gender;

   private Set<PhoneNumber> phoneNumbers;

   public Person() {
   }

   public Person(Integer id, String name, String surname, Integer age, Address address, Gender gender, Set<PhoneNumber> phoneNumbers) {
      this.id = id;
      this.name = name;
      this.surname = surname;
      this.age = age;
      this.address = address;
      this.gender = gender;
      this.phoneNumbers = phoneNumbers;
   }

   @ProtoField(number = 1)
   public Integer getId() {
      return id;
   }

   public void setId(Integer id) {
      this.id = id;
   }

   @ProtoField(number = 2)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @ProtoField(number = 3)
   public String getSurname() {
      return surname;
   }

   public void setSurname(String surname) {
      this.surname = surname;
   }

   @ProtoField(number = 4)
   public Gender getGender() {
      return gender;
   }

   public void setGender(Gender gender) {
      this.gender = gender;
   }

   @ProtoField(number = 5)
   public Address getAddress() {
      return address;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   @ProtoField(number = 6, collectionImplementation = HashSet.class)
   public Set<PhoneNumber> getPhoneNumbers() {
      return phoneNumbers;
   }

   public void setPhoneNumbers(Set<PhoneNumber> phoneNumbers) {
      this.phoneNumbers = phoneNumbers;
   }

   @ProtoField(number = 7, type = Type.UINT32)
   public Integer getAge() {
      return age;
   }

   public void setAge(Integer age) {
      this.age = age;
   }
}
