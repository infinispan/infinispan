package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.Gender;
import org.infinispan.query.dsl.embedded.testdomain.User;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class UserPB implements User {

   private int id;
   private String name;
   private String surname;
   private String salutation;
   private Set<Integer> accountIds;
   private List<Address> addresses;
   private Integer age;
   private Gender gender;
   private String notes;
   private Instant creationDate;
   private Instant passwordExpirationDate;

   @ProtoField(number = 1)
   public String getName() {
      return name;
   }

   @Override
   public void setName(String name) {
      this.name = name;
   }

   @ProtoField(number = 2, defaultValue = "0")
   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   @ProtoField(number = 3, collectionImplementation = HashSet.class)
   public Set<Integer> getAccountIds() {
      return accountIds;
   }

   public void setAccountIds(Set<Integer> accountIds) {
      this.accountIds = accountIds;
   }

   @ProtoField(number = 4)
   public String getSurname() {
      return surname;
   }

   public void setSurname(String surname) {
      this.surname = surname;
   }

   @ProtoField(number = 5)
   public String getSalutation() {
      return salutation;
   }

   public void setSalutation(String salutation) {
      this.salutation = salutation;
   }

   @ProtoField(number = 6)
   public Integer getAge() {
      return age;
   }

   public void setAge(Integer age) {
      this.age = age;
   }

   @ProtoField(number = 7)
   public Gender getGender() {
      return gender;
   }

   public void setGender(Gender gender) {
      this.gender = gender;
   }

   @ProtoField(number = 8, collectionImplementation = ArrayList.class)
   public List<Address> getAddresses() {
      return addresses;
   }

   public void setAddresses(List<Address> addresses) {
      this.addresses = addresses;
   }

   @ProtoField(number = 9)
   public String getNotes() {
      return notes;
   }

   public void setNotes(String notes) {
      this.notes = notes;
   }

   @ProtoField(number = 10)
   public Instant getCreationDate() {
      return creationDate;
   }

   public void setCreationDate(Instant creationDate) {
      this.creationDate = creationDate;
   }

   @ProtoField(number = 11)
   public Instant getPasswordExpirationDate() {
      return passwordExpirationDate;
   }

   public void setPasswordExpirationDate(Instant passwordExpirationDate) {
      this.passwordExpirationDate = passwordExpirationDate;
   }

   @Override
   public String toString() {
      return "UserPB{" +
            "id=" + id +
            ", name='" + name + '\'' +
            ", surname='" + surname + '\'' +
            ", salutation='" + salutation + '\'' +
            ", accountIds=" + accountIds +
            ", addresses=" + addresses +
            ", age=" + age +
            ", gender=" + gender +
            ", notes=" + notes +
            ", creationDate='" + creationDate + '\'' +
            ", passwordExpirationDate=" + passwordExpirationDate +
            '}';
   }
}
