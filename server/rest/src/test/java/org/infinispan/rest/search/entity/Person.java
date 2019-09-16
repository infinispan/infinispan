package org.infinispan.rest.search.entity;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.NumericField;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @since 9.2
 */
@Indexed
@SuppressWarnings("unused")
public class Person implements Serializable {

   @Field
   private Integer id;

   @Field
   private String name;

   @Field
   private String surname;

   @Field
   @NumericField
   private Integer age;

   @Field
   private Address address;

   @Field
   private Gender gender;

   @IndexedEmbedded
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

   @ProtoField(number = 7)
   public Integer getAge() {
      return age;
   }

   public void setAge(Integer age) {
      this.age = age;
   }
}
