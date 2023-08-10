package org.infinispan.protostream.sampledomain;

import java.util.Objects;

/**
 * @author anistor@redhat.com
 */
public class Address {

   private String street;
   private String postCode;
   private int number;
   private boolean isCommercial;

   public Address() {
   }

   public Address(String street, String postCode, int number) {
      this(street, postCode, number, false);
   }

   public Address(String street, String postCode, int number, boolean isCommercial) {
      this.street = street;
      this.postCode = postCode;
      this.number = number;
      this.isCommercial = isCommercial;
   }

   public String getStreet() {
      return street;
   }

   public void setStreet(String street) {
      this.street = street;
   }

   public String getPostCode() {
      return postCode;
   }

   public void setPostCode(String postCode) {
      this.postCode = postCode;
   }

   public int getNumber() {
      return number;
   }

   public void setNumber(int number) {
      this.number = number;
   }

   public boolean isCommercial() {
      return isCommercial;
   }

   public void setCommercial(boolean isCommercial) {
      this.isCommercial = isCommercial;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Address address = (Address) o;
      return number == address.number &&
            isCommercial == address.isCommercial &&
            Objects.equals(street, address.street) &&
            Objects.equals(postCode, address.postCode);
   }

   @Override
   public int hashCode() {
      return Objects.hash(street, postCode, number, isCommercial);
   }

   @Override
   public String toString() {
      return "Address{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            ", number=" + number +
            ", isCommercial=" + isCommercial +
            '}';
   }
}
