package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.infinispan.query.dsl.embedded.testdomain.Address;
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

   @Override
   public int getId() {
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   public Set<Integer> getAccountIds() {
      return accountIds;
   }

   @Override
   public void setAccountIds(Set<Integer> accountIds) {
      this.accountIds = accountIds;
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public void setName(String name) {
      this.name = name;
   }

   @Override
   public String getSurname() {
      return surname;
   }

   @Override
   public void setSurname(String surname) {
      this.surname = surname;
   }

   @Override
   public String getSalutation() {
      return salutation;
   }

   @Override
   public void setSalutation(String salutation) {
      this.salutation = salutation;
   }

   @Override
   public List<Address> getAddresses() {
      return addresses;
   }

   @Override
   public void setAddresses(List<Address> addresses) {
      this.addresses = addresses;
   }

   @Override
   public Integer getAge() {
      return age;
   }

   @Override
   public void setAge(Integer age) {
      this.age = age;
   }

   @Override
   public Gender getGender() {
      return gender;
   }

   @Override
   public void setGender(Gender gender) {
      this.gender = gender;
   }

   @Override
   public String getNotes() {
      return notes;
   }

   @Override
   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Override
   public Instant getCreationDate() {
      return creationDate;
   }

   @Override
   public void setCreationDate(Instant creationDate) {
      this.creationDate = creationDate;
   }

   @Override
   public Instant getPasswordExpirationDate() {
      return passwordExpirationDate;
   }

   @Override
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
            ", creationDate=" + creationDate +
            ", passwordExpirationDate=" + passwordExpirationDate +
            '}';
   }
}
