package org.infinispan.nearcache.jms;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

import java.util.Properties;

@BuiltBy(RemoteEventStoreConfigurationBuilder.class)
@ConfigurationFor(RemoteEventStore.class)
public class RemoteEventStoreConfiguration extends AbstractStoreConfiguration {

   public RemoteEventStoreConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications,
                                        AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore,
                                        boolean preload, boolean shared, Properties properties) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
   }
}
