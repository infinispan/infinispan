package org.infinispan.jcache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;

import java.util.Properties;

@BuiltBy(JCacheWriterAdapterConfigurationBuilder.class)
@ConfigurationFor(JCacheWriterAdapter.class)
public class JCacheWriterAdapterConfiguration extends AbstractStoreConfiguration {

   public JCacheWriterAdapterConfiguration(boolean purgeOnStartup, boolean fetchPersistentState, boolean ignoreModifications, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore, boolean preload, boolean shared, Properties properties) {
      super(purgeOnStartup, fetchPersistentState, ignoreModifications, async, singletonStore, preload, shared, properties);
   }
}
