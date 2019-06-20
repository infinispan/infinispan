package org.infinispan.test.data;

import java.io.Serializable;

import org.infinispan.protostream.annotations.ProtoField;

public class Address implements Serializable {
   private static final long serialVersionUID = 5943073369866339615L;

   @ProtoField(number = 1)
   String street = null;

   @ProtoField(number = 2)
   String city = "San Jose";

   @ProtoField(number = 3, defaultValue = "0")
   int zip = 0;

   public Address() {}

   public Address(String street, String city, int zip) {
      this.street = street;
      this.city = city;
      this.zip = zip;
   }

   public String getStreet() {
      return street;
   }

   public void setStreet(String street) {
      this.street = street;
   }

   public String getCity() {
      return city;
   }

   public void setCity(String city) {
      this.city = city;
   }

   public int getZip() {
      return zip;
   }

   public void setZip(int zip) {
      this.zip = zip;
   }

   public String toString() {
      return "street=" + getStreet() + ", city=" + getCity() + ", zip=" + getZip();
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final Address address = (Address) o;

      if (zip != address.zip) return false;
      if (city != null ? !city.equals(address.city) : address.city != null) return false;
      if (street != null ? !street.equals(address.street) : address.street != null) return false;

      return true;
   }

   public int hashCode() {
      int result;
      result = (street != null ? street.hashCode() : 0);
      result = 29 * result + (city != null ? city.hashCode() : 0);
      result = 29 * result + zip;
      return result;
   }
}
