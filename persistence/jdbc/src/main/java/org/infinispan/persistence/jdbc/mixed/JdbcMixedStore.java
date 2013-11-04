package org.infinispan.persistence.jdbc.mixed;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.jdbc.binary.JdbcBinaryStore;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcBinaryStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.JdbcMixedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.spi.AdvancedLoadWriteStore;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.Executor;

/**
 * Cache store that combines functionality of {@link org.infinispan.persistence.jdbc.binary.JdbcBinaryStore} and {@link org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore}. It
 * aggregates an instance of JdbcBinaryStore and JdbcStringBasedStore, delegating work to one of them
 * (sometimes both, see below) based on the passed in key. In order to determine which store to use it will rely on the
 * configured {@link org.infinispan.persistence.keymappers.Key2StringMapper} )(see configuration).
 * <p/>
 * The advantage it brings is the possibility of efficiently storing string(able) keyed {@link
 * org.infinispan.container.entries.InternalCacheEntry}s, and at the same time being able to store any other keys, a la
 * {@link org.infinispan.persistence.jdbc.binary.JdbcBinaryStore}.
 * <p/>
 * There will only be a performance cost for the aggregate operations: loadAll, fromStream, toStream and clear. For
 * these operations there will be two distinct database call, one for each JdbcStore implementation. Most of application
 * are only using these operations at lifecycles changes (e.g. fromStream and toStream at cluster join time, loadAll at
 * startup for warm caches), so performance drawback shouldn't be significant (again, most of the cases).
 * <p/>
 * Resource sharing - both aggregated cache stores have locks and connection pools. The locking is not shared, each
 * loader keeping its own {@link org.infinispan.util.concurrent.locks.StripedLock} instance. Also the tables (even though
 * similar as definition) are different in order to avoid key collision. On the other hand, the connection pooling is a
 * shared resource.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.persistence.jdbc.binary.JdbcBinaryStore
 * @see org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore
 */
public class JdbcMixedStore implements AdvancedLoadWriteStore {

   private static final Log log = LogFactory.getLog(JdbcMixedStore.class);

   private JdbcMixedStoreConfiguration configuration;

   private JdbcBinaryStore binaryStore = new JdbcBinaryStore();
   private JdbcStringBasedStore stringStore = new JdbcStringBasedStore();
   private ConnectionFactory sharedConnectionFactory;

   @Override
   public void init(InitializationContext ctx) {
      this.configuration = ctx.getConfiguration();
      binaryStore.init(new InitialisationContextDelegate(ctx, buildBinaryStoreConfiguration(this.configuration)));
      stringStore.init(new InitialisationContextDelegate(ctx, buildStringStoreConfiguration(this.configuration)));
   }

   @Override
   public void start()  {
      ConnectionFactoryConfiguration factoryConfig = configuration.connectionFactory();
      sharedConnectionFactory = ConnectionFactory.getConnectionFactory(factoryConfig.connectionFactoryClass().getName(),
            configuration.getClass().getClassLoader());
      sharedConnectionFactory.start(factoryConfig, configuration.getClass().getClassLoader());
      binaryStore.doConnectionFactoryInitialization(sharedConnectionFactory);
      binaryStore.start();
      stringStore.initializeConnectionFactory(sharedConnectionFactory);
      stringStore.start();
   }

   @Override
   public void stop()  {

      Throwable cause = null;
      try {
         binaryStore.stop();
      } catch (Throwable t) {
         if (cause == null) cause = t;
         log.debug("Exception while stopping", t);
      }
      try {
         stringStore.stop();
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
         throw new PersistenceException("Exceptions occurred while stopping store", cause);
      }
   }

   @Override
   public void purge(Executor threadPool, PurgeListener task) {
      binaryStore.purge(threadPool, task);
      stringStore.purge(threadPool, task);
   }
   
   @Override
   public MarshalledEntry load(Object key)  {
      return getStore(key).load(key);
   }

   @Override
   public void process(KeyFilter filter, CacheLoaderTask task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      binaryStore.process(filter, task, executor, fetchValue, fetchMetadata);
      stringStore.process(filter, task, executor, fetchValue, fetchMetadata);
   }

   @Override
   public void write(MarshalledEntry ed)  {
      getStore(ed.getKey()).write(ed);
   }

   @Override
   public boolean delete(Object key) {
      return getStore(key).delete(key);
   }

   @Override
   public int size() {
      return stringStore.size() + binaryStore.size();
   }

   @Override
   public boolean contains(Object key) {
      return getStore(key).contains(key);
   }

   @Override
   public void clear()  {
      binaryStore.clear();
      stringStore.clear();
   }

   public ConnectionFactory getConnectionFactory() {
      return sharedConnectionFactory;
   }

   public JdbcBinaryStore getBinaryStore() {
      return binaryStore;
   }

   public JdbcStringBasedStore getStringStore() {
      return stringStore;
   }

   // Methods to build the String and Binary Configurations.

   private JdbcStringBasedStoreConfiguration buildStringStoreConfiguration(JdbcMixedStoreConfiguration configuration){
      ConfigurationBuilder builder = new ConfigurationBuilder();
      JdbcStringBasedStoreConfigurationBuilder stringBuilder = builder.persistence().addStore
            (JdbcStringBasedStoreConfigurationBuilder.class).manageConnectionFactory(false);
      stringBuilder.
            key2StringMapper(configuration.key2StringMapper()).
            table().read(configuration.stringTable());

      return stringBuilder.create();
   }

   private JdbcBinaryStoreConfiguration buildBinaryStoreConfiguration(JdbcMixedStoreConfiguration configuration) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      JdbcBinaryStoreConfigurationBuilder binaryBuilder = builder.persistence().addStore
            (JdbcBinaryStoreConfigurationBuilder.class).manageConnectionFactory(false);
      binaryBuilder.table().read(configuration.binaryTable());
      return binaryBuilder.create();
   }

   private AdvancedLoadWriteStore getStore(Object key) {
      return stringStore.supportsKey(key.getClass()) ? stringStore : binaryStore;
   }

   public JdbcMixedStoreConfiguration getConfiguration() {
      return configuration;
   }
}
