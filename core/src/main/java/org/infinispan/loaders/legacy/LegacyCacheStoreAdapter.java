package org.infinispan.loaders.legacy;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.config.parsing.XmlConfigHelper;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.LegacyStoreConfiguration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheLoaderConfig;
import org.infinispan.loaders.AbstractCacheStoreConfig;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.spi.AbstractCacheLoader;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.marshall.StreamingMarshallerAdapter;
import org.infinispan.transaction.xa.GlobalTransaction;

@Deprecated
public class LegacyCacheStoreAdapter extends AbstractCacheLoader implements CacheStore {
   org.infinispan.loaders.CacheStore delegate;
   LegacyStoreConfiguration configuration;

   @Override
   public void init(CacheLoaderConfiguration config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      this.configuration = validateConfigurationClass(config, LegacyStoreConfiguration.class);
      super.init(config, cache, m);

      CacheLoaderConfig legacy = Util.getInstance(configuration.cacheStore().getClass().getAnnotation(CacheLoaderMetadata.class).configurationClass());
      if (legacy instanceof AbstractCacheLoaderConfig) {
         AbstractCacheLoaderConfig aclc = (AbstractCacheLoaderConfig) legacy;
         aclc.setProperties(configuration.properties());
      }
      XmlConfigHelper.setValues(legacy, configuration.properties(), false, true);
      if (legacy instanceof AbstractCacheStoreConfig) {
         AbstractCacheStoreConfig acsc = (AbstractCacheStoreConfig) legacy;
         acsc.fetchPersistentState(configuration.fetchPersistentState());
         acsc.ignoreModifications(configuration.ignoreModifications());
         acsc.purgeOnStartup(configuration.purgeOnStartup());
         acsc.purgeSynchronously(configuration.purgeSynchronously());
         acsc.purgerThreads(configuration.purgerThreads());

         acsc.getAsyncStoreConfig().setEnabled(configuration.async().enabled());
         acsc.getAsyncStoreConfig().flushLockTimeout(configuration.async().flushLockTimeout());
         acsc.getAsyncStoreConfig().modificationQueueSize(configuration.async().modificationQueueSize());
         acsc.getAsyncStoreConfig().shutdownTimeout(configuration.async().shutdownTimeout());
         acsc.getAsyncStoreConfig().threadPoolSize(configuration.async().threadPoolSize());
         acsc.getSingletonStoreConfig().enabled(configuration.singletonStore().enabled());
         acsc.getSingletonStoreConfig().pushStateTimeout(configuration.singletonStore().pushStateTimeout());
         acsc.getSingletonStoreConfig().pushStateWhenCoordinator(configuration.singletonStore().pushStateWhenCoordinator());
      }

      delegate = (org.infinispan.loaders.CacheStore) Util.getInstance(configuration.cacheStore().getClass());
      delegate.init(legacy, cache, m != null ? new StreamingMarshallerAdapter(m) : null);
   }

   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      delegate.store(entry);
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      delegate.fromStream(inputStream);
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return delegate.load(key);
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return delegate.loadAll();
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      delegate.toStream(outputStream);
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      return delegate.load(numEntries);
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      return delegate.loadAllKeys(keysToExclude);
   }

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      return delegate.containsKey(key);
   }

   @Override
   public void clear() throws CacheLoaderException {
      delegate.clear();
   }

   @Override
   public void start() throws CacheLoaderException {
      delegate.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      delegate.stop();
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      return delegate.remove(key);
   }

   @Override
   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      delegate.removeAll(keys);
   }

   @Override
   public void purgeExpired() throws CacheLoaderException {
      delegate.purgeExpired();
   }

   @Override
   public void prepare(List<? extends Modification> modifications, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
      delegate.prepare(modifications, tx, isOnePhase);
   }

   @Override
   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      delegate.commit(tx);
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      delegate.rollback(tx);
   }

   @Override
   public CacheLoaderConfiguration getConfiguration() {
      return configuration;
   }
}
