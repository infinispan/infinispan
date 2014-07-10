package org.infinispan.client.hotrod.query.testdomain.protobuf;

import org.infinispan.query.dsl.embedded.testdomain.Address;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class AddressPB implements Address {

   private String street;
   private String postCode;

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

   @Override
   public String toString() {
      return "AddressPB{" +
            "street='" + street + '\'' +
            ", postCode='" + postCode + '\'' +
            '}';
   }
}
