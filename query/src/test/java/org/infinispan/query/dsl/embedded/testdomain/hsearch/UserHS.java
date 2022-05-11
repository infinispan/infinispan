package org.infinispan.query.dsl.embedded.testdomain.hsearch;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.query.dsl.embedded.testdomain.Address;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
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
   @ProtoField(number = 2, defaultValue = "0")
   public int getId() {
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(number = 3, collectionImplementation = HashSet.class)
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
   @ProtoField(number = 11)
   public Instant getPasswordExpirationDate() {
      return passwordExpirationDate;
   }

   @Override
   public void setPasswordExpirationDate(Instant passwordExpirationDate) {
      this.passwordExpirationDate = passwordExpirationDate;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      UserHS other = (UserHS) o;

      if (age != null ? !age.equals(other.age) : other.age != null) return false;
      if (id != other.id) return false;
      if (accountIds != null ? !accountIds.equals(other.accountIds) : other.accountIds != null) return false;
      if (addresses != null ? !addresses.equals(other.addresses) : other.addresses != null) return false;
      if (gender != other.gender) return false;
      if (name != null ? !name.equals(other.name) : other.name != null) return false;
      if (surname != null ? !surname.equals(other.surname) : other.surname != null) return false;
      if (salutation != null ? !salutation.equals(other.salutation) : other.salutation != null) return false;
      if (notes != null ? !notes.equals(other.notes) : other.notes != null) return false;
      if (creationDate != null ? !creationDate.equals(other.creationDate) : other.creationDate != null) return false;
      if (passwordExpirationDate != null ? !passwordExpirationDate.equals(other.passwordExpirationDate) : other.passwordExpirationDate != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = id;
      result = 31 * result + (accountIds != null ? accountIds.hashCode() : 0);
      result = 31 * result + (name != null ? name.hashCode() : 0);
      result = 31 * result + (surname != null ? surname.hashCode() : 0);
      result = 31 * result + (salutation != null ? salutation.hashCode() : 0);
      result = 31 * result + (age != null ? age : 0);
      result = 31 * result + (gender != null ? gender.hashCode() : 0);
      result = 31 * result + (addresses != null ? addresses.hashCode() : 0);
      result = 31 * result + (notes != null ? notes.hashCode() : 0);
      result = 31 * result + (creationDate != null ? creationDate.hashCode() : 0);
      result = 31 * result + (passwordExpirationDate != null ? passwordExpirationDate.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "UserHS{" +
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
