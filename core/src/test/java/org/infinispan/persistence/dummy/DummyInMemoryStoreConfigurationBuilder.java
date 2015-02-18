package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

import static org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration.*;

public class DummyInMemoryStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<DummyInMemoryStoreConfiguration, DummyInMemoryStoreConfigurationBuilder> {

   public DummyInMemoryStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, DummyInMemoryStoreConfiguration.attributeDefinitionSet());
   }

   @Override
   public DummyInMemoryStoreConfigurationBuilder self() {
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder debug(boolean debug) {
      attributes.attribute(DEBUG).set(debug);
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder slow(boolean slow) {
      attributes.attribute(SLOW).set(slow);
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder storeName(String storeName) {
      attributes.attribute(STORE_NAME).set(storeName);
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder failKey(Object failKey) {
      attributes.attribute(FAIL_KEY).set(failKey);
      return this;
   }

   @Override
   public DummyInMemoryStoreConfiguration create() {
      return new DummyInMemoryStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
   }

   @Override
   public DummyInMemoryStoreConfigurationBuilder read(DummyInMemoryStoreConfiguration template) {
      super.read(template);
      return this;
   }

}
