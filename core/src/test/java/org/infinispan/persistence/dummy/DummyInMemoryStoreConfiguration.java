package org.infinispan.persistence.dummy;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

import java.util.Properties;

@BuiltBy(DummyInMemoryStoreConfigurationBuilder.class)
@ConfigurationFor(DummyInMemoryStore.class)
public class DummyInMemoryStoreConfiguration extends AbstractStoreConfiguration {

   private final boolean debug;
   private final boolean slow;
   private final String storeName;
   private final Object failKey;

   public DummyInMemoryStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties, boolean debug, boolean slow, String storeName, Object failKey) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
      this.debug = debug;
      this.slow = slow;
      this.storeName = storeName;
      this.failKey = failKey;
   }

   public boolean debug() {
      return debug;
   }

   public boolean slow() {
      return slow;
   }

   public String storeName() {
      return storeName;
   }

   public Object failKey() {
      return failKey;
   }


}
