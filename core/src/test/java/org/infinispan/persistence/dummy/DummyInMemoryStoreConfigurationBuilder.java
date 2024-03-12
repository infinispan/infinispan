package org.infinispan.persistence.dummy;

import static org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration.ASYNC_OPERATION;
import static org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration.SLOW;
import static org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration.START_FAILURES;
import static org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration.STORE_NAME;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class DummyInMemoryStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<DummyInMemoryStoreConfiguration, DummyInMemoryStoreConfigurationBuilder> {

   public DummyInMemoryStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, DummyInMemoryStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public DummyInMemoryStoreConfigurationBuilder self() {
      return this;
   }

   /**
    * If true, then writes to this store are artificially slowed by {@value DummyInMemoryStore#SLOW_STORE_WAIT} milliseconds.
    * @deprecated Use {@link org.infinispan.persistence.support.DelayStore} instead.
    */
   @Deprecated(forRemoval=true)
   public DummyInMemoryStoreConfigurationBuilder slow(boolean slow) {
      attributes.attribute(SLOW).set(slow);
      return this;
   }

   /**
    * If true, then operations will be performed "asynchronously" on the non blocking executor.
    * @param asyncOperation whether the store should perform the operations on the non blocking executor.
    * @return this builder
    */
   public DummyInMemoryStoreConfigurationBuilder asyncOperation(boolean asyncOperation) {
      attributes.attribute(ASYNC_OPERATION).set(asyncOperation);
      return this;
   }

   /**
    * A storeName can be utilised to lookup existing DummyInMemoryStore instances associated with the provided String. If
    * an instance is already mapped to the provided string, then that instance is utilised.  Otherwise a new instance is
    * created and associated with the given string. This can be useful for testing shared stores, across multiple CacheManager instances.
    */
   public DummyInMemoryStoreConfigurationBuilder storeName(String storeName) {
      attributes.attribute(STORE_NAME).set(storeName);
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder startFailures(int failures) {
      attributes.attribute(START_FAILURES).set(failures);
      return this;
   }

   @Override
   public DummyInMemoryStoreConfiguration create() {
      return new DummyInMemoryStoreConfiguration(attributes.protect(), async.create());
   }

   @Override
   public DummyInMemoryStoreConfigurationBuilder read(DummyInMemoryStoreConfiguration template, Combine combine) {
      super.read(template, combine);
      return this;
   }

}
