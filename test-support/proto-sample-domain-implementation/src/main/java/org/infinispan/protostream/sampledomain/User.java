package org.infinispan.protostream.sampledomain;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.infinispan.protostream.UnknownFieldSet;

/**
 * @author anistor@redhat.com
 */
public class User {

   public enum Gender {
      MALE, FEMALE
   }

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

   private UnknownFieldSet unknownFieldSet;

   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   public Set<Integer> getAccountIds() {
      return accountIds;
   }

   public void setAccountIds(Set<Integer> accountIds) {
      this.accountIds = accountIds;
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

   public String getSalutation() {
      return salutation;
   }

   public void setSalutation(String salutation) {
      this.salutation = salutation;
   }

   public List<Address> getAddresses() {
      return addresses;
   }

   public void setAddresses(List<Address> addresses) {
      this.addresses = addresses;
   }

   public Integer getAge() {
      return age;
   }

   public void setAge(Integer age) {
      this.age = age;
   }

   public Gender getGender() {
      return gender;
   }

   public void setGender(Gender gender) {
      this.gender = gender;
   }

   public String getNotes() {
      return notes;
   }

   public void setNotes(String notes) {
      this.notes = notes;
   }

   public Instant getCreationDate() {
      return creationDate;
   }

   public void setCreationDate(Instant creationDate) {
      this.creationDate = creationDate;
   }

   public Instant getPasswordExpirationDate() {
      return passwordExpirationDate;
   }

   public void setPasswordExpirationDate(Instant passwordExpirationDate) {
      this.passwordExpirationDate = passwordExpirationDate;
   }

   public UnknownFieldSet getUnknownFieldSet() {
      return unknownFieldSet;
   }

   public void setUnknownFieldSet(UnknownFieldSet unknownFieldSet) {
      this.unknownFieldSet = unknownFieldSet;
   }

   @Override
   public String toString() {
      return "User{" +
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
            ", passwordExpirationDate='" + passwordExpirationDate + '\'' +
            ", unknownFieldSet=" + unknownFieldSet +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      User user = (User) o;
      return id == user.id &&
            Objects.equals(name, user.name) &&
            Objects.equals(surname, user.surname) &&
            Objects.equals(salutation, user.salutation) &&
            Objects.equals(accountIds, user.accountIds) &&
            Objects.equals(addresses, user.addresses) &&
            Objects.equals(age, user.age) &&
            gender == user.gender &&
            Objects.equals(notes, user.notes) &&
            Objects.equals(creationDate, user.creationDate) &&
            Objects.equals(passwordExpirationDate, user.passwordExpirationDate);
   }

   @Override
   public int hashCode() {
      return Objects.hash(id, name, surname, salutation, accountIds, addresses, age, gender, notes, creationDate, passwordExpirationDate);
   }
}
