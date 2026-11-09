package org.infinispan.protostream.sampledomain.bank;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.infinispan.api.annotations.indexing.Basic;
import org.infinispan.api.annotations.indexing.Embedded;
import org.infinispan.api.annotations.indexing.Indexed;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * @author anistor@redhat.com
 */
@Indexed
public class User implements Serializable {

   public static final String ENTITY_NAME = "sample_domain.User";

   public enum Gender {
      @ProtoEnumValue(0)
      MALE,
      @ProtoEnumValue(1)
      FEMALE
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

   @Basic(projectable = true, sortable = true)
   @ProtoField(number = 1, defaultValue = "0")
   public int getId() {
      return id;
   }

   public void setId(int id) {
      this.id = id;
   }

   @Basic(projectable = true)
   @ProtoField(value = 2, collectionImplementation = HashSet.class)
   public Set<Integer> getAccountIds() {
      return accountIds;
   }

   public void setAccountIds(Set<Integer> accountIds) {
      this.accountIds = accountIds;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(3)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(4)
   public String getSurname() {
      return surname;
   }

   public void setSurname(String surname) {
      this.surname = surname;
   }

   @Basic(projectable = true)
   @ProtoField(5)
   public String getSalutation() {
      return salutation;
   }

   public void setSalutation(String salutation) {
      this.salutation = salutation;
   }

   @Embedded
   @ProtoField(value = 6, collectionImplementation = ArrayList.class)
   public List<Address> getAddresses() {
      return addresses;
   }

   public void setAddresses(List<Address> addresses) {
      this.addresses = addresses;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(7)
   public Integer getAge() {
      return age;
   }

   public void setAge(Integer age) {
      this.age = age;
   }

   @Basic(projectable = true)
   @ProtoField(8)
   public Gender getGender() {
      return gender;
   }

   public void setGender(Gender gender) {
      this.gender = gender;
   }

   @ProtoField(9)
   public String getNotes() {
      return notes;
   }

   public void setNotes(String notes) {
      this.notes = notes;
   }

   @Basic(projectable = true, sortable = true)
   @ProtoField(10)
   public Instant getCreationDate() {
      return creationDate;
   }

   public void setCreationDate(Instant creationDate) {
      this.creationDate = creationDate;
   }

   @ProtoField(11)
   public Instant getPasswordExpirationDate() {
      return passwordExpirationDate;
   }

    public void setPasswordExpirationDate(Instant passwordExpirationDate) {
       this.passwordExpirationDate = passwordExpirationDate;
    }

    public User(){}

    public User(int id, String name, String surname, Gender gender, int age, String notes, List<Address> addresses, Set<Integer> accountIds) {
      this.id = id;
      this.name = name;
      this.surname = surname;
      this.gender = gender;
      this.age = age;
      this.notes = notes;
      this.addresses = addresses;
      this.accountIds = accountIds;
    }

    public static Map<String, User> data() {
       Map<String, User> data = new LinkedHashMap<>();
       int id = 1;
       data.put("John Doe", new User(id++, "John", "Doe", Gender.MALE, 22, "Lorem ispum dolor sit amet", List.of(new Address("Main Street", "X1234", 0)), null));
       data.put("Spider Man", new User(id++, "Spider", "Man", Gender.MALE, 32, null, List.of(new Address("Old Street", "Y12", 0), new Address("Bond Street", "ZZ", 0)), Collections.singleton(3)));
       data.put("Spider Woman", new User(id++, "Spider", "Woman", Gender.FEMALE, 31, null, null, null));
       data.put("Tom Cat", new User(id++, "Tom Cat", "Cat", Gender.MALE, 40, null, List.of(new Address("Dark Alley", "1234", 0)), Collections.singleton(12)));
       data.put("Adrian Nistor", new User(id++, "Adrian", "Nistor", Gender.MALE, 50, null, List.of(new Address("Old Street", "XYZ", 0)), null));
       data.put("Jane Doe", new User(id++, "Jane", "Doe", Gender.FEMALE, 60, null, null, null));
       return data;
    }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      User user = (User) o;
      return id == user.id && Objects.equals(name, user.name) && Objects.equals(surname, user.surname) && Objects.equals(salutation, user.salutation) && Objects.equals(accountIds, user.accountIds) && Objects.equals(addresses, user.addresses) && Objects.equals(age, user.age) && gender == user.gender && Objects.equals(notes, user.notes) && Objects.equals(creationDate, user.creationDate) && Objects.equals(passwordExpirationDate, user.passwordExpirationDate);
   }

   @Override
   public int hashCode() {
      return Objects.hash(id, name, surname, salutation, accountIds, addresses, age, gender, notes, creationDate, passwordExpirationDate);
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
            '}';
   }

}
