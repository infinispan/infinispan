package org.infinispan.rest.search.entity;

/**
 * @since 9.2
 */
@SuppressWarnings("unused")
public class Address {

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
}
