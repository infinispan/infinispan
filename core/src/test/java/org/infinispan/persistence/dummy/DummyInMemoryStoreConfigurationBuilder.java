package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;

public class DummyInMemoryStoreConfigurationBuilder extends
      AbstractStoreConfigurationBuilder<DummyInMemoryStoreConfiguration, DummyInMemoryStoreConfigurationBuilder> {

   protected boolean debug;
   protected boolean slow;
   protected String storeName;
   protected Object failKey;

   public DummyInMemoryStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public DummyInMemoryStoreConfigurationBuilder self() {
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder debug(boolean debug) {
      this.debug = debug;
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder slow(boolean slow) {
      this.slow = slow;
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder storeName(String storeName) {
      this.storeName = storeName;
      return this;
   }

   public DummyInMemoryStoreConfigurationBuilder failKey(Object failKey) {
      this.failKey = failKey;
      return this;
   }

   @Override
   public DummyInMemoryStoreConfiguration create() {
      return new DummyInMemoryStoreConfiguration(purgeOnStartup, fetchPersistentState, ignoreModifications,
                                                            async.create(), singletonStore.create(), preload, shared, properties,
                                                             debug, slow, storeName, failKey);
   }

   @Override
   public DummyInMemoryStoreConfigurationBuilder read(DummyInMemoryStoreConfiguration template) {
      super.read(template);

      debug = template.debug();
      slow = template.slow();
      storeName = template.storeName();
      failKey = template.failKey();
      shared =template.shared();

      return this;
   }

}
