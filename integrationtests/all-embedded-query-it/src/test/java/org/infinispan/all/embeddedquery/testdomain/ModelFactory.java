package org.infinispan.all.embeddedquery.testdomain;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface ModelFactory {

   Account makeAccount();

   Limits makeLimits();

   Class<?> getAccountImplClass();

   String getAccountTypeName();

   User makeUser();

   Class<?> getUserImplClass();

   String getUserTypeName();

   Address makeAddress();

   Class<?> getAddressImplClass();

   String getAddressTypeName();

   Transaction makeTransaction();

   Class<?> getTransactionImplClass();

   String getTransactionTypeName();
}
