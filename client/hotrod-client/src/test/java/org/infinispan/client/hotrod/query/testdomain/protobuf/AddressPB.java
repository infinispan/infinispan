package org.infinispan.client.hotrod.query.testdomain.protobuf;

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

   public boolean isCommercial() {
      return isCommercial;
   }

   public void setCommercial(boolean commercial) {
      isCommercial = commercial;
   }
   @Override
   public String toString() {
      return "AddressPB{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            ", number='" + number + '\'' +
            ", isCommercial=" + isCommercial +
            '}';
   }
}
