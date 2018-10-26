package org.infinispan.client.hotrod.query.testdomain.protobuf;

import java.util.Objects;

import org.infinispan.query.dsl.embedded.testdomain.Address;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AddressPB implements Address {

   private String street;

   private String postCode;

   private int number;

   private boolean isCommercial;

   @Override
   public String getStreet() {
      return street;
   }

   @Override
   public void setStreet(String street) {
      this.street = street;
   }

   @Override
   public String getPostCode() {
      return postCode;
   }

   @Override
   public void setPostCode(String postCode) {
      this.postCode = postCode;
   }

   @Override
   public int getNumber() {
      return number;
   }

   @Override
   public void setNumber(int number) {
      this.number = number;
   }

   @Override
   public boolean isCommercial() {
      return isCommercial;
   }

   @Override
   public void setCommercial(boolean isCommercial) {
      this.isCommercial = isCommercial;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AddressPB address = (AddressPB) o;
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
      return "AddressPB{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            ", number=" + number +
            ", isCommercial=" + isCommercial +
            '}';
   }
}
