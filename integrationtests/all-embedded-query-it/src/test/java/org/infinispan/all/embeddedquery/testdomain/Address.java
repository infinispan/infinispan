package org.infinispan.all.embeddedquery.testdomain;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Address {

   String getStreet();

   void setStreet(String street);

   String getPostCode();

   void setPostCode(String postCode);

   int getNumber();

   void setNumber(int number);

   boolean isCommercial();

   void setCommercial(boolean isCommercial);
}
