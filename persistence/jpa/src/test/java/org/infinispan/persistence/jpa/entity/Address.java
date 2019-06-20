package org.infinispan.persistence.jpa.entity;

import javax.persistence.Embeddable;

import org.infinispan.protostream.annotations.ProtoField;

@Embeddable
public class Address {

   private String city;
   private String street;
   private int zipCode;

   @ProtoField(number = 1)
   public String getCity() {
      return city;
   }

   public void setCity(String city) {
      this.city = city;
   }

   @ProtoField(number = 2)
   public String getStreet() {
      return street;
   }

   public void setStreet(String street) {
      this.street = street;
   }

   @ProtoField(number = 3, defaultValue = "0")
   public int getZipCode() {
      return zipCode;
   }

   public void setZipCode(int zipCode) {
      this.zipCode = zipCode;
   }

   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;

      final Address address = (Address) o;

      if (city != null ? !city.equals(address.getCity()) : address.getCity() != null)
         return false;
      if (street != null ? !street.equals(address.getStreet()) : address.getStreet() != null)
         return false;
      if (zipCode != address.getZipCode())
         return false;

      return true;
   }

   public int hashCode() {
      final int prime = 31;
      int result;
      result = (city != null ? city.hashCode() : 0);
      result = prime * result + (street != null ? street.hashCode() : 0);
      result = prime * result + zipCode;
      return result;
   }

   @Override
   public String toString() {
      return "Address{" +
            "city='" + city + '\'' +
            ", street='" + street + '\'' +
            ", zipCode=" + zipCode +
            '}';
   }
}
