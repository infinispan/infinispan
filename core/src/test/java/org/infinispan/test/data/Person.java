package org.infinispan.test.data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class Person implements Serializable, JsonSerialization {

   String name;
   Address address;
   byte[] picture;
   Sex sex;
   Date birthDate;

   public Person() {
      // Needed for serialization
      birthDate = new Date(0);
   }

   public Person(String name) {
      this(name, null, null, null, null);
   }

   @ProtoFactory
   public Person(String name, Address address, byte[] picture, Sex sex, Date birthDate) {
      this.name = name;
      this.address = address;
      this.picture = picture;
      this.sex = sex;
      // This is only required until we can remove the default 0
      this.birthDate = birthDate == null ? new Date(0) : birthDate;
   }

   @ProtoField(1)
   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   @ProtoField(2)
   public Address getAddress() {
      return address;
   }

   public void setAddress(Address address) {
      this.address = address;
   }

   @ProtoField(3)
   public byte[] getPicture() {
      return picture;
   }

   public void setPicture(byte[] picture) {
      this.picture = picture;
   }

   @ProtoField(4)
   public Sex getSex() {
      return sex;
   }

   public void setSex(Sex sex) {
      this.sex = sex;
   }

   @ProtoField(value = 5, defaultValue = "0")
   public Date getBirthDate() {
      return birthDate;
   }

   public void setBirthDate(Date birthDate) {
      this.birthDate = birthDate;
   }

   @Override
   public String toString() {
      return "Person{" +
            "name='" + name + '\'' +
            ", address=" + address +
            ", picture=" + Util.toHexString(picture) +
            ", sex=" + sex +
            ", birthDate=" + birthDate +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final Person person = (Person) o;

      if (address != null ? !address.equals(person.address) : person.address != null) return false;
      if (name != null ? !name.equals(person.name) : person.name != null) return false;
      if (picture != null ? !Arrays.equals(picture, person.picture) : person.picture != null) return false;
      if (sex != null ? !sex.equals(person.sex) : person.sex != null) return false;
      if (birthDate != null ? !birthDate.equals(person.birthDate) : person.birthDate != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      result = (name != null ? name.hashCode() : 0);
      result = 29 * result + (address != null ? address.hashCode() : 0);
      result = 29 * result + (picture != null ? Arrays.hashCode(picture) : 0);
      result = 29 * result + (sex != null ? sex.hashCode() : 0);
      result = 29 * result + (birthDate != null ? birthDate.hashCode() : 0);
      return result;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("name", name)
            .set("address", Json.make(address))
            .set("picture", picture)
            .set("sex", sex)
            .set("birthDate", birthDate == null ? 0 : birthDate.getTime());
   }
}
