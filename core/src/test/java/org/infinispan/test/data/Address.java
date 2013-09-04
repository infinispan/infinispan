package org.infinispan.test.data;

import java.io.Serializable;

public class Address implements Serializable {
   private static final long serialVersionUID = 5943073369866339615L;

   String street = null;
   String city = "San Jose";
   int zip = 0;

   public String getStreet() {
      return street;
   }

   public Address setStreet(String street) {
      this.street = street;
      return this;
   }

   public String getCity() {
      return city;
   }

   public Address setCity(String city) {
      this.city = city;
      return this;
   }

   public int getZip() {
      return zip;
   }

   public Address setZip(int zip) {
      this.zip = zip;
      return this;
   }

   public String toString() {
      return "street=" + getStreet() + ", city=" + getCity() + ", zip=" + getZip();
   }

//    public Object writeReplace() {
//	return this;
//    }

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
