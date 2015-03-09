package org.infinispan.all.remote.sample.testdomain;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Address {

   String getStreet();

   void setStreet(String street);

   String getPostCode();

   void setPostCode(String postCode);
}
