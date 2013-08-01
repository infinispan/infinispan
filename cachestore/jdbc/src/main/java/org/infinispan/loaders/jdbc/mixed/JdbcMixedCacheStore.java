package org.infinispan.loaders.jdbc.mixed;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheLoaderConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore;
import org.infinispan.loaders.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcBinaryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.configuration.JdbcMixedCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfiguration;
import org.infinispan.loaders.jdbc.configuration.JdbcStringBasedCacheStoreConfigurationBuilder;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore;
import org.infinispan.loaders.spi.AbstractCacheStore;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Cache store that combines functionality of {@link JdbcBinaryCacheStore} and {@link JdbcStringBasedCacheStore}. It
 * aggregates an instance of JdbcBinaryCacheStore and JdbcStringBasedCacheStore, delegating work to one of them
 * (sometimes both, see below) based on the passed in key. In order to determine which store to use it will rely on the
 * configured {@link org.infinispan.loaders.keymappers.Key2StringMapper} )(see configuration).
 * <p/>
 * The advantage it brings is the possibility of efficiently storing string(able) keyed {@link
 * org.infinispan.container.entries.InternalCacheEntry}s, and at the same time being able to store any other keys, a la
 * {@link org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore}.
 * <p/>
 * There will only be a performance cost for the aggregate operations: loadAll, fromStream, toStream and clear. For
 * these operations there will be two distinct database call, one for each JdbcStore implementation. Most of application
 * are only using these operations at lifecycles changes (e.g. fromStream and toStream at cluster join time, loadAll at
 * startup for warm caches), so performance drawback shouldn't be significant (again, most of the cases).
 * <p/>
 * Resource sharing - both aggregated cache loaders have locks and connection pools. The locking is not shared, each
 * loader keeping its own {@link org.infinispan.util.concurrent.locks.StripedLock} instance. Also the tables (even though
 * similar as definition) are different in order to avoid key collision. On the other hand, the connection pooling is a
 * shared resource.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.jdbc.binary.JdbcBinaryCacheStore
 * @see org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore
 */
public class JdbcMixedCacheStore extends AbstractCacheStore {

   private static final Log log = LogFactory.getLog(JdbcMixedCacheStore.class);

   private JdbcMixedCacheStoreConfiguration configuration;

   private JdbcBinaryCacheStore binaryCacheStore = new JdbcBinaryCacheStore();
   private JdbcStringBasedCacheStore stringBasedCacheStore = new JdbcStringBasedCacheStore();
   private ConnectionFactory sharedConnectionFactory;

   @Override
   public void init(CacheLoaderConfiguration configuration, Cache<?, ?> cache, StreamingMarshaller m) throws
         CacheLoaderException {
      this.configuration = validateConfigurationClass(configuration, JdbcMixedCacheStoreConfiguration.class);
      super.init(configuration, cache, m);
      binaryCacheStore.init(buildBinaryStoreConfiguration(this.configuration), cache, m);
      stringBasedCacheStore.init(buildStringStoreConfiguration(this.configuration), cache, m);
   }

   @Override
   public void start() throws CacheLoaderException {
      super.start();
      ConnectionFactoryConfiguration factoryConfig = configuration.connectionFactory();
      sharedConnectionFactory = ConnectionFactory.getConnectionFactory(factoryConfig.connectionFactoryClass().getName(),
            configuration.getClass().getClassLoader());
      sharedConnectionFactory.start(factoryConfig, configuration.getClass().getClassLoader());
      binaryCacheStore.doConnectionFactoryInitialization(sharedConnectionFactory);
      binaryCacheStore.start();
      stringBasedCacheStore.doConnectionFactoryInitialization(sharedConnectionFactory);
      stringBasedCacheStore.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      super.stop();

      Throwable cause = null;
      try {
         binaryCacheStore.stop();
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      try {
         stringBasedCacheStore.stop();
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      try {
         sharedConnectionFactory.stop();
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      if (cause != null) {
         throw new CacheLoaderException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   protected void purgeInternal() throws CacheLoaderException {
      binaryCacheStore.purgeInternal();
      stringBasedCacheStore.purgeInternal();
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      return getCacheStore(key).load(key);
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      Set<InternalCacheEntry> fromBuckets = binaryCacheStore.loadAll();
      Set<InternalCacheEntry> fromStrings = stringBasedCacheStore.loadAll();
      if (log.isTraceEnabled()) {
         log.tracef("Loaded from bucket: %s", fromBuckets);
         log.tracef("Loaded from string: %s", fromStrings);
      }
      fromBuckets.addAll(fromStrings);
      return fromBuckets;
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      if (numEntries < 0) return loadAll();
      Set<InternalCacheEntry> set = stringBasedCacheStore.load(numEntries);

      if (set.size() < numEntries) {
         Set<InternalCacheEntry> otherSet = binaryCacheStore.load(numEntries - set.size());
         set.addAll(otherSet);
      }

      return set;
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      Set<Object> fromBuckets = binaryCacheStore.loadAllKeys(keysToExclude);
      Set<Object> fromStrings = stringBasedCacheStore.loadAllKeys(keysToExclude);
      fromBuckets.addAll(fromStrings);
      return fromBuckets;
   }

   @Override
   public void store(InternalCacheEntry ed) throws CacheLoaderException {
      getCacheStore(ed.getKey()).store(ed);
   }

   @Override
   public void fromStream(ObjectInput inputStream) throws CacheLoaderException {
      binaryCacheStore.fromStream(inputStream);
      stringBasedCacheStore.fromStream(inputStream);
   }

   @Override
   public void toStream(ObjectOutput outputStream) throws CacheLoaderException {
      binaryCacheStore.toStream(outputStream);
      stringBasedCacheStore.toStream(outputStream);
   }

   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      return getCacheStore(key).remove(key);
   }

   @Override
   public void clear() throws CacheLoaderException {
      binaryCacheStore.clear();
      stringBasedCacheStore.clear();
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

   // Methods to build the String and Binary Configurations.

   private JdbcStringBasedCacheStoreConfiguration buildStringStoreConfiguration(JdbcMixedCacheStoreConfiguration configuration){
      ConfigurationBuilder builder = new ConfigurationBuilder();
      JdbcStringBasedCacheStoreConfigurationBuilder stringBuilder = builder.loaders().addLoader
            (JdbcStringBasedCacheStoreConfigurationBuilder.class).manageConnectionFactory(false);
      stringBuilder.
            key2StringMapper(configuration.key2StringMapper()).
            table().read(configuration.stringTable());

      return stringBuilder.create();
   }

   private JdbcBinaryCacheStoreConfiguration buildBinaryStoreConfiguration(JdbcMixedCacheStoreConfiguration configuration) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      JdbcBinaryCacheStoreConfigurationBuilder binaryBuilder = builder.loaders().addLoader
            (JdbcBinaryCacheStoreConfigurationBuilder.class).manageConnectionFactory(false);
      binaryBuilder.table().read(configuration.binaryTable());
      return binaryBuilder.create();
   }
}
