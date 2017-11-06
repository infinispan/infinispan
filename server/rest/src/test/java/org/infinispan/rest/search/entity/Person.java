package org.infinispan.rest.search.entity;

import java.util.Set;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.NumericField;

/**
 * @since 9.2
 */
@Indexed
@SuppressWarnings("unused")
public class Person {

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

   public Integer getId() {
      return id;
   }

   public void setId(Integer id) {
      this.id = id;
   }

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

   public Gender getGender() {
      return gender;
   }

   public void setGender(Gender gender) {
      this.gender = gender;
   }

   public Address getAddress() {
      return address;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   public Set<PhoneNumber> getPhoneNumbers() {
      return phoneNumbers;
   }

   public void setPhoneNumbers(Set<PhoneNumber> phoneNumbers) {
      this.phoneNumbers = phoneNumbers;
   }

   public Integer getAge() {
      return age;
   }

   public void setAge(Integer age) {
      this.age = age;
   }
}
