package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.query.dsl.embedded.testdomain.Address;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
@ProtoName("User")
public class UserHS extends UserBase {

   private int id;

   private Set<Integer> accountIds;

   private String surname;

   private String salutation;

   private Integer age;  // yes, not the birth date :)

   private Gender gender;

   private List<Address> addresses;

   private Instant creationDate;

   private Instant passwordExpirationDate;

   /**
    * This is not indexed!
    */
   private String notes;

   @Override
   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 1, defaultValue = "0")
   public int getId() {
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(number = 2, collectionImplementation = HashSet.class)
   public Set<Integer> getAccountIds() {
      return accountIds;
   }

   @Override
   public void setAccountIds(Set<Integer> accountIds) {
      this.accountIds = accountIds;
   }

   @Override
   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 4)
   public String getSurname() {
      return surname;
   }

   @Override
   public void setSurname(String surname) {
      this.surname = surname;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(number = 5)
   public String getSalutation() {
      return salutation;
   }

   @Override
   public void setSalutation(String salutation) {
      this.salutation = salutation;
   }

   @Override
   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 6)
   public Integer getAge() {
      return age;
   }

   @Override
   public void setAge(Integer age) {
      this.age = age;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(number = 7)
   public Gender getGender() {
      return gender;
   }

   @Override
   public void setGender(Gender gender) {
      this.gender = gender;
   }

   @Override
   public List<Address> getAddresses() {
      return addresses;
   }

   @Override
   public void setAddresses(List<Address> addresses) {
      this.addresses = addresses;
   }

   @Embedded(name = "addresses")
   @ProtoField(number = 8, collectionImplementation = ArrayList.class)
   public List<AddressHS> getHSAddresses() {
      return addresses == null ? null : addresses.stream().map(AddressHS.class::cast).collect(Collectors.toList());
   }

   void setHSAddresses(List<AddressHS> addresses) {
      // IPROTO-120 means that an empty list is always returned, so we need to force a null value
      this.addresses = addresses == null || addresses.isEmpty() ? null : new ArrayList<>(addresses);
   }

   @Override
   @ProtoField(number = 9)
   public String getNotes() {
      return notes;
   }

   @Override
   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Override
   @ProtoField(number = 10)
   @Basic(projectable = true)
   public Instant getCreationDate() {
      return creationDate;
   }

   @Override
   public void setCreationDate(Instant creationDate) {
      this.creationDate = creationDate;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      UserHS userHS = (UserHS) o;
      return id == userHS.id && Objects.equals(accountIds, userHS.accountIds) && Objects.equals(surname, userHS.surname) && Objects.equals(salutation, userHS.salutation) && Objects.equals(age, userHS.age) && gender == userHS.gender && Objects.equals(addresses, userHS.addresses) && Objects.equals(creationDate, userHS.creationDate) && Objects.equals(passwordExpirationDate, userHS.passwordExpirationDate) && Objects.equals(notes, userHS.notes);
   }

   @Override
   public int hashCode() {
      return Objects.hash(id, accountIds, surname, salutation, age, gender, addresses, creationDate, passwordExpirationDate, notes);
   }

   @Override
   @ProtoField(number = 11)
   public Instant getPasswordExpirationDate() {
      return passwordExpirationDate;
   }

   @Override
   public void setPasswordExpirationDate(Instant passwordExpirationDate) {
      this.passwordExpirationDate = passwordExpirationDate;
   }

   @Override
   public String toString() {
      return "UserHS{" + "id=" + id + ", name='" + name + '\'' + ", surname='" + surname + '\'' + ", salutation='" + salutation + '\'' + ", accountIds=" + accountIds + ", addresses=" + addresses + ", age=" + age + ", gender=" + gender + ", notes=" + notes + ", creationDate='" + creationDate + '\'' + ", passwordExpirationDate=" + passwordExpirationDate + '}';
   }
}
