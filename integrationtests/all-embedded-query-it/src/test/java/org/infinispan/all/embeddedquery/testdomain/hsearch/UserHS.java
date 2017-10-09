package org.infinispan.all.embeddedquery.testdomain.hsearch;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.IndexedEmbedded;
import org.hibernate.search.annotations.NumericField;
import org.hibernate.search.annotations.SortableField;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.impl.BuiltinIterableBridge;
import org.infinispan.all.embeddedquery.testdomain.Address;
import org.infinispan.all.embeddedquery.testdomain.User;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Indexed
public class UserHS extends UserBase {

   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   private int id;

   @Field(store = Store.YES, analyze = Analyze.NO)
   @FieldBridge(impl = BuiltinIterableBridge.class)
   private Set<Integer> accountIds;

   @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   @SortableField
   private String surname;

   @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   private String salutation;

   @Field(store = Store.YES, analyze = Analyze.NO, indexNullAs = "-1")
   @NumericField
   @SortableField
   private Integer age;  // yes, not the birth date :)

   @Field(store = Store.YES, analyze = Analyze.NO)
   private Gender gender;

   @IndexedEmbedded(targetElement = AddressHS.class, indexNullAs = Field.DEFAULT_NULL_TOKEN)
   private List<Address> addresses;

   @Field(analyze = Analyze.NO, store = Store.YES, index = Index.YES)
   private Instant creationDate;

   //@Field(analyze = Analyze.NO, store = Store.YES, index = Index.NO)
   private Instant passwordExpirationDate;

   /**
    * This is not indexed!
    */
   private String notes;

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
   public List<Address> getAddresses() {
      return addresses;
   }

   @Override
   public void setAddresses(List<Address> addresses) {
      this.addresses = addresses;
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
            ", creationDate=" + creationDate +
            ", passwordExpirationDate=" + passwordExpirationDate +
            '}';
   }
}

/**
 * Parent class for UserHS to demonstrate inheritance of indexed attributes.
 */
abstract class UserBase implements User, Serializable {

   @Field(store = Store.YES, analyze = Analyze.NO)
   @SortableField
   protected String name;

   @Override
   public String getName() {
      return name;
   }

   @Override
   public void setName(String name) {
      this.name = name;
   }
}
