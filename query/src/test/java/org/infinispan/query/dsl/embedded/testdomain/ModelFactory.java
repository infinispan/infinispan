package org.infinispan.query.dsl.embedded.testdomain;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface ModelFactory {

   Account makeAccount();

   Class<?> getAccountImplClass();

   User makeUser();

   Class<?> getUserImplClass();

   Address makeAddress();

   Class<?> getAddressImplClass();

   Transaction makeTransaction();

   Class<?> getTransactionImplClass();
}
