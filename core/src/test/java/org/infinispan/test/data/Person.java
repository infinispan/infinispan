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
   boolean acceptedToS;
   double moneyOwned;
   float moneyOwed;
   double decimalField;
   float realField;

   public Person() {
      // Needed for serialization
   }

   public Person(String name) {
      this(name, null);
   }

   public Person(String name, Address address) {
      this(name, address, null, null, null, false, 1.1, 0.4f, 10.3, 4.7f);
   }

   @ProtoFactory
   public Person(String name, Address address, byte[] picture, Sex sex, Date birthDate, boolean acceptedToS, double moneyOwned,
         float moneyOwed, double decimalField, float realField) {
      this.name = name;
      this.address = address;
      this.picture = picture;
      this.sex = sex;
      this.birthDate = birthDate;
      this.acceptedToS = acceptedToS;
      this.moneyOwned = moneyOwned;
      this.moneyOwed = moneyOwed;
      this.decimalField = decimalField;
      this.realField = realField;
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

   @ProtoField(value = 5)
   public Date getBirthDate() {
      return birthDate;
   }

   public void setBirthDate(Date birthDate) {
      this.birthDate = birthDate;
   }

   @ProtoField(value = 6, defaultValue = "false", name = "accepted_tos")
   public boolean isAcceptedToS() {
      return acceptedToS;
   }

   public void setAcceptedToS(boolean acceptedToS) {
      this.acceptedToS = acceptedToS;
   }

   @ProtoField(value = 7, defaultValue = "1.1")
   public double getMoneyOwned() {
      return moneyOwned;
   }

   public void setMoneyOwned(double moneyOwned) {
      this.moneyOwned = moneyOwned;
   }

   @ProtoField(value = 8, defaultValue = "0.4")
   public float getMoneyOwed() {
      return moneyOwed;
   }

   public void setMoneyOwed(float moneyOwed) {
      this.moneyOwed = moneyOwed;
   }

   @ProtoField(value = 9, defaultValue = "10.3")
   public double getDecimalField() {
      return decimalField;
   }

   public void setDecimalField(double decimalField) {
      this.decimalField = decimalField;
   }

   @ProtoField(value = 10, defaultValue = "4.7")
   public float getRealField() {
      return realField;
   }

   public void setRealField(float realField) {
      this.realField = realField;
   }

   @Override
   public String toString() {
      return "Person{" +
            "name='" + name + '\'' +
            ", address=" + address +
            ", picture=" + Util.toHexString(picture) +
            ", sex=" + sex +
            ", birthDate=" + birthDate +
            ", acceptedToS=" + acceptedToS +
            ", moneyOwned=" + moneyOwned +
            ", moneyOwed=" + moneyOwed +
            ", decimalField=" + decimalField +
            ", realField=" + realField +
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
      if (acceptedToS != person.acceptedToS) return false;
      if (moneyOwned != person.moneyOwned) return false;
      if (moneyOwed != person.moneyOwed) return false;
      if (decimalField != person.decimalField) return false;
      if (realField != person.realField) return false;

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
      result = 29 * result + Boolean.hashCode(acceptedToS);
      result = 29 * result + Double.hashCode(moneyOwned);
      result = 29 * result + Float.hashCode(moneyOwed);
      result = 29 * result + Double.hashCode(decimalField);
      result = 29 * result + Float.hashCode(realField);
      return result;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("name", name)
            .set("address", Json.make(address))
            .set("picture", picture)
            .set("sex", sex)
            .set("birthDate", birthDate == null ? 0 : birthDate.getTime())
            .set("acceptedToS", acceptedToS)
            .set("moneyOwned", moneyOwned)
            .set("moneyOwed", moneyOwed)
            .set("decimalField", decimalField)
            .set("realField", realField);
   }
}
