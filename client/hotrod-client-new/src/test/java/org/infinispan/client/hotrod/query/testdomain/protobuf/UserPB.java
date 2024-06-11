package org.infinispan.client.hotrod.query.testdomain.protobuf;

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
import org.infinispan.protostream.annotations.ProtoName;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
@ProtoName("User")
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
   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 1, defaultValue = "-1")
   public int getId() {
      return id;
   }

   @Override
   public void setId(int id) {
      this.id = id;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(value = 2, collectionImplementation = HashSet.class)
   public Set<Integer> getAccountIds() {
      return accountIds;
   }

   @Override
   public void setAccountIds(Set<Integer> accountIds) {
      this.accountIds = accountIds;
   }

   @Override
   @Basic(projectable = true, sortable = true)
   @ProtoField(3)
   public String getName() {
      return name;
   }

   @Override
   public void setName(String name) {
      this.name = name;
   }

   @Override
   @Basic(projectable = true, sortable = true, indexNullAs = "_null_")
   @ProtoField(4)
   public String getSurname() {
      return surname;
   }

   @Override
   public void setSurname(String surname) {
      this.surname = surname;
   }

   @Override
   @Basic(projectable = true, indexNullAs = "_null_")
   @ProtoField(5)
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

   @Embedded
   @ProtoField(value = 6, name = "addresses", collectionImplementation = ArrayList.class)
   List<AddressPB> getWrappedAddresses() {
      return addresses == null ? null :
            addresses.stream().map(AddressPB.class::cast).collect(Collectors.toList());
   }

   void setWrappedAddresses(List<AddressPB> wrappedAddresses) {
      this.addresses = wrappedAddresses == null ? null :
            wrappedAddresses.stream().map(Address.class::cast).collect(Collectors.toList());
   }

   @Override
   @Basic(sortable = true, indexNullAs = "-1")
   @ProtoField(7)
   public Integer getAge() {
      return age;
   }

   @Override
   public void setAge(Integer age) {
      this.age = age;
   }

   @Override
   @Basic(projectable = true)
   @ProtoField(8)
   public Gender getGender() {
      return gender;
   }

   @Override
   public void setGender(Gender gender) {
      this.gender = gender;
   }

   @Override
   @ProtoField(9)
   public String getNotes() {
      return notes;
   }

   @Override
   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Override
   @Basic(projectable = true, sortable = true, indexNullAs = "-1")
   @ProtoField(10)
   public Instant getCreationDate() {
      return creationDate;
   }

   @Override
   public void setCreationDate(Instant creationDate) {
      this.creationDate = creationDate;
   }

   @Override
   @ProtoField(11)
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
            ", creationDate='" + creationDate + '\'' +
            ", passwordExpirationDate=" + passwordExpirationDate +
            '}';
   }
}
