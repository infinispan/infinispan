package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore;
import org.infinispan.marshall.Marshaller;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Cache store that combines functionality of {@link JdbcBinaryCacheStore} and {@link JdbcStringBasedCacheStore}. It
 * aggregates an instance of JdbcBinaryCacheStore and JdbcStringBasedCacheStore, delegating work to one of them
 * (sometimes both, see below) based on the passed in key. In order to determine which store to use it will rely on the
 * configured {@link org.infinispan.loaders.jdbc.stringbased.Key2StringMapper} )(see configuration).
 * <p/>
 * The advantage it brings is the possibility of effeciently storing string(able) keyd {@link
 * org.infinispan.container.entries.InternalCacheEntry}s, and at the same time being able to store any other keys, a la
 * {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}.
 * <p/>
 * There will only be a performance cost for the aggregate operations: loadAll, fromStream, toStream and clear. For
 * these operations there will be two distinct database call, one for each JdbcStore implementation. Most of application
 * are only using these operations at lifecycles changes (e.g. fromStram and toStream at cluster join time, loadAll at
 * startup for warm caches), so performance drawback shouldn't be significant (again, most of the cases).
 * <p/>
 * Resource sharing - both aggregated cache loaders have locks and connection pools. The locking is not shared, each
 * loader keeping its own {@link org.infinispan.util.concurrent.locks.StripedLock} instace. Also the tables (even though
 * similar as definition) are different in order to avoid key collision. On the other hand, the connection pooling is a
 * shared resource.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.jdbc.mixed.JdbcMixedCacheStoreConfig
 * @see org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore
 * @see org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore
 */
public class JdbcMixedCacheStore extends AbstractCacheStore {

   private JdbcMixedCacheStoreConfig config;
   private JdbcBinaryCacheStore binaryCacheStore = new JdbcBinaryCacheStore();
   private JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
   private ConnectionFactory sharedConnectionFactory;

   @Override
   public void init(CacheLoaderConfig config, Cache cache, Marshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      this.config = (JdbcMixedCacheStoreConfig) config;
      binaryCacheStore.init(this.config.getBinaryCacheStoreConfig(), cache, m);
      stringBasedCacheStore.init(this.config.getStringCacheStoreConfig(), cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      ConnectionFactoryConfig factoryConfig = config.getConnectionFactoryConfig();
      sharedConnectionFactory = ConnectionFactory.getConnectionFactory(factoryConfig.getConnectionFactoryClass());
      sharedConnectionFactory.start(factoryConfig);
      binaryCacheStore.doConnectionFactoryInitialization(sharedConnectionFactory);
      binaryCacheStore.start();
      stringBasedCacheStore.doConnectionFactoryInitialization(sharedConnectionFactory);
      stringBasedCacheStore.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();
      binaryCacheStore.stop();
      stringBasedCacheStore.stop();
      sharedConnectionFactory.stop();
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      binaryCacheStore.purgeInternal();
      stringBasedCacheStore.purgeInternal();
   }

   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return getCacheStore(key).load(key);
   }

   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      Set<InternalCacheEntry> fromBuckets = binaryCacheStore.loadAll();
      Set<InternalCacheEntry> fromStrings = stringBasedCacheStore.loadAll();
      fromBuckets.addAll(fromStrings);
      return fromBuckets;
   }

   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      getCacheStore(ed.getKey()).store(ed);
   }

   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      binaryCacheStore.fromStream(inputStream);
      stringBasedCacheStore.fromStream(inputStream);
   }

   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      binaryCacheStore.toStream(outputStream);
      stringBasedCacheStore.toStream(outputStream);
   }

   public boolean remove(Object key) throws CacheLoaderException {
      return getCacheStore(key).remove(key);
   }

   public void clear() throws CacheLoaderException {
      binaryCacheStore.clear();
      stringBasedCacheStore.clear();
   }

   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return JdbcMixedCacheStoreConfig.class;
   }

   private CacheStore getCacheStore(Object key) {
      return stringBasedCacheStore.supportsKey(key.getClass()) ? stringBasedCacheStore : binaryCacheStore;
   }

   public ConnectionFactory getConnectionFactory() {
      return sharedConnectionFactory;
   }

   public JdbcBinaryCacheStore getBinaryCacheStore() {
      return binaryCacheStore;
   }

   public JdbcStringBasedCacheStore getStringBasedCacheStore() {
      return stringBasedCacheStore;
   }
}
