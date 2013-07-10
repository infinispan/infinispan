package org.infinispan.loaders.dummy;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;

public class DummyInMemoryCacheStoreConfigurationBuilder
      extends
      AbstractStoreConfigurationBuilder<DummyInMemoryCacheStoreConfiguration, DummyInMemoryCacheStoreConfigurationBuilder> {

   protected boolean debug;
   protected boolean slow;
   protected String storeName;
   protected Object failKey;

   public DummyInMemoryCacheStoreConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   @Override
   public DummyInMemoryCacheStoreConfigurationBuilder self() {
      return this;
   }

   public DummyInMemoryCacheStoreConfigurationBuilder debug(boolean debug) {
      this.debug = debug;
      return this;
   }

   public DummyInMemoryCacheStoreConfigurationBuilder slow(boolean slow) {
      this.slow = slow;
      return this;
   }

   public DummyInMemoryCacheStoreConfigurationBuilder storeName(String storeName) {
      this.storeName = storeName;
      return this;
   }

   public DummyInMemoryCacheStoreConfigurationBuilder failKey(Object failKey) {
      this.failKey = failKey;
      return this;
   }

   @Override
   public DummyInMemoryCacheStoreConfiguration create() {
      return new DummyInMemoryCacheStoreConfiguration(debug, slow, storeName, failKey, purgeOnStartup, purgeSynchronously, purgerThreads,
            fetchPersistentState, ignoreModifications, TypedProperties.toTypedProperties(properties), async.create(),
            singletonStore.create());
   }

   @Override
   public DummyInMemoryCacheStoreConfigurationBuilder read(DummyInMemoryCacheStoreConfiguration template) {
      debug = template.debug();
      slow = template.slow();
      storeName = template.storeName();
      failKey = template.failKey();

      // AbstractStore-specific configuration
      fetchPersistentState = template.fetchPersistentState();
      ignoreModifications = template.ignoreModifications();
      properties = template.properties();
      purgeOnStartup = template.purgeOnStartup();
      purgeSynchronously = template.purgeSynchronously();
      async.read(template.async());
      singletonStore.read(template.singletonStore());

      return this;
   }

}
