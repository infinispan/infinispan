package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.loaders.CacheLoader;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configuration a specific cache loader or cache store
 * @author pmuir
 *
 */
public class LoaderConfigurationBuilder extends AbstractLoaderConfigurationBuilder<LoaderConfiguration> implements
      LoaderConfigurationChildBuilder {

   private static final Log log = LogFactory.getLog(LoaderConfigurationBuilder.class);

   private CacheLoader cacheLoader;
   private boolean fetchPersistentState = false;
   private boolean ignoreModifications = false;
   private boolean purgeOnStartup = false;
   private int purgerThreads = 1;
   private boolean purgeSynchronously = false;
   private Properties properties = new Properties();

   LoaderConfigurationBuilder(LoadersConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    * 
    * @param cacheLoader
    * @return
    */
   public LoaderConfigurationBuilder cacheLoader(CacheLoader cacheLoader) {
      this.cacheLoader = cacheLoader;
      return this;
   }

   /**
    * If true, fetch persistent state when joining a cluster. If multiple cache stores are chained,
    * only one of them can have this property enabled. Persistent state transfer with a shared cache
    * store does not make sense, as the same persistent store that provides the data will just end
    * up receiving it. Therefore, if a shared cache store is used, the cache will not allow a
    * persistent state transfer even if a cache store has this property set to true. Finally,
    * setting it to true only makes sense if in a clustered environment, and only 'replication' and
    * 'invalidation' cluster modes are supported.
    */
   public LoaderConfigurationBuilder fetchPersistentState(boolean b) {
      this.fetchPersistentState = b;
      return this;
   }

   /**
    * If true, any operation that modifies the cache (put, remove, clear, store...etc) won't be
    * applied to the cache store. This means that the cache store could become out of sync with the
    * cache.
    */
   public LoaderConfigurationBuilder ignoreModifications(boolean b) {
      this.ignoreModifications = b;
      return this;
   }

   /**
    * If true, purges this cache store when it starts up.
    */
   public LoaderConfigurationBuilder purgeOnStartup(boolean b) {
      this.purgeOnStartup = b;
      return this;
   }

   /**
    * The number of threads to use when purging asynchronously.
    */
   public LoaderConfigurationBuilder purgerThreads(int i) {
      this.purgerThreads = i;
      return this;
   }

   /**
    * If true, CacheStore#purgeExpired() call will be done synchronously
    */
   public LoaderConfigurationBuilder purgeSynchronously(boolean b) {
      this.purgeSynchronously = b;
      return this;
   }

   /**
    * <p>
    * Defines a single property. Can be used multiple times to define all needed properties, but the
    * full set is overridden by {@link #withProperties(Properties)}.
    * </p>
    * <p>
    * These properties are passed directly to the cache loader.
    * </p>
    */
   public LoaderConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * <p>
    * These properties are passed directly to the cache loader.
    * </p>
    */
   public LoaderConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return this;
   }

   @Override
   void validate() {
      async.validate();
      singletonStore.validate();
      if (!getLoadersBuilder().shared() && fetchPersistentState && purgeOnStartup
            && getBuilder().clustering().cacheMode().isClustered())
         log.staleEntriesWithoutFetchPersistentStateOrPurgeOnStartup();
   }

   @Override
   LoaderConfiguration create() {
      return new LoaderConfiguration(TypedProperties.toTypedProperties(properties), cacheLoader, fetchPersistentState,
            ignoreModifications, purgeOnStartup, purgerThreads, purgeSynchronously, async.create(), singletonStore.create());
   }

   @Override
   public LoaderConfigurationBuilder read(LoaderConfiguration template) {
      this.cacheLoader = template.cacheLoader();
      this.fetchPersistentState = template.fetchPersistentState();
      this.ignoreModifications = template.ignoreModifications();
      this.properties = template.properties();
      this.purgeOnStartup = template.purgeOnStartup();
      this.purgerThreads = template.purgerThreads();
      this.purgeSynchronously = template.purgeSynchronously();
      
      this.async.read(template.async());
      this.singletonStore.read(template.singletonStore());
      
      return this;
   }

}
