package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.Version;
import org.infinispan.commands.RemoveCacheCommand;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.InternalCacheFactory;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.registry.impl.ClusterRegistryImpl;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.security.impl.PrincipalRoleMapperContextImpl;
import org.infinispan.security.impl.SecureCacheImpl;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.stats.impl.CacheContainerStatsImpl;
import org.infinispan.util.CyclicDependencyException;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A <tt>CacheManager</tt> is the primary mechanism for retrieving a {@link Cache} instance, and is often used as a
 * starting point to using the {@link Cache}.
 * <p/>
 * <tt>CacheManager</tt>s are heavyweight objects, and we foresee no more than one <tt>CacheManager</tt> being used per
 * JVM (unless specific configuration requirements require more than one; but either way, this would be a minimal and
 * finite number of instances).
 * <p/>
 * Constructing a <tt>CacheManager</tt> is done via one of its constructors, which optionally take in a {@link
 * org.infinispan.configuration.cache.Configuration} or a path or URL to a configuration XML file.
 * <p/>
 * Lifecycle - <tt>CacheManager</tt>s have a lifecycle (it implements {@link Lifecycle}) and the default constructors
 * also call {@link #start()}. Overloaded versions of the constructors are available, that do not start the
 * <tt>CacheManager</tt>, although it must be kept in mind that <tt>CacheManager</tt>s need to be started before they
 * can be used to create <tt>Cache</tt> instances.
 * <p/>
 * Once constructed, <tt>CacheManager</tt>s should be made available to any component that requires a <tt>Cache</tt>,
 * via JNDI or via some other mechanism such as an IoC container.
 * <p/>
 * You obtain <tt>Cache</tt> instances from the <tt>CacheManager</tt> by using one of the overloaded
 * <tt>getCache()</tt>, methods. Note that with <tt>getCache()</tt>, there is no guarantee that the instance you get is
 * brand-new and empty, since caches are named and shared. Because of this, the <tt>CacheManager</tt> also acts as a
 * repository of <tt>Cache</tt>s, and is an effective mechanism of looking up or creating <tt>Cache</tt>s on demand.
 * <p/>
 * When the system shuts down, it should call {@link #stop()} on the <tt>CacheManager</tt>. This will ensure all caches
 * within its scope are properly stopped as well.
 * <p/>
 * Sample usage:
 * <pre><code>
 *    CacheManager manager = CacheManager.getInstance("my-config-file.xml");
 *    Cache&lt;String, Person&gt; entityCache = manager.getCache("myEntityCache");
 *    entityCache.put("aPerson", new Person());
 *
 *    ConfigurationBuilder confBuilder = new ConfigurationBuilder();
 *    confBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
 *    manager.defineConfiguration("myReplicatedCache", confBuilder.build());
 *    Cache&lt;String, String&gt; replicatedCache = manager.getCache("myReplicatedCache");
 * </code></pre>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
@MBean(objectName = DefaultCacheManager.OBJECT_NAME, description = "Component that acts as a manager, factory and container for caches in the system.")
public class DefaultCacheManager implements EmbeddedCacheManager {
   public static final String OBJECT_NAME = "CacheManager";
   private static final Log log = LogFactory.getLog(DefaultCacheManager.class);
   protected final GlobalConfiguration globalConfiguration;
   protected final Configuration defaultConfiguration;
   private final ConcurrentMap<String, CacheWrapper> caches = CollectionFactory.makeConcurrentMap();
   private final ConcurrentMap<String, Configuration> configurationOverrides = CollectionFactory.makeConcurrentMap();
   private final GlobalComponentRegistry globalComponentRegistry;
   private volatile boolean stopping;
   private final AuthorizationHelper authzHelper;
   private final DependencyGraph<String> cacheDependencyGraph = new DependencyGraph<>();
   private final CacheContainerStats stats;

   /**
    * Constructs and starts a default instance of the CacheManager, using configuration defaults.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * and {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    */
   public DefaultCacheManager() {
      this((GlobalConfiguration) null, null, true);
   }

   /**
    * Constructs a default instance of the CacheManager, using configuration defaults.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * and {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    *
    * @param start if true, the cache manager is started
    */
   public DefaultCacheManager(boolean start) {
      this((GlobalConfiguration) null, null, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the default configuration passed in.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * and {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration to use as a template for all caches created
    */
   public DefaultCacheManager(Configuration defaultConfiguration) {
      this(null, defaultConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the default configuration passed in.  See
    * {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration file to use as a template for all caches created
    * @param start                if true, the cache manager is started
    */
   public DefaultCacheManager(Configuration defaultConfiguration, boolean start) {
      this(null, defaultConfiguration, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global configuration passed in, and system
    * defaults for the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration) {
      this(globalConfiguration, null, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global configuration passed in, and system defaults for
    * the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    * @param start               if true, the cache manager is started.
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, boolean start) {
      this(globalConfiguration, null, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global and default configurations passed in.
    * If either of these are null, system defaults are used.
    *
    * @param globalConfiguration  global configuration to use. If null, a default instance is created.
    * @param defaultConfiguration default configuration to use. If null, a default instance is created.
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, Configuration defaultConfiguration) {
      this(globalConfiguration, defaultConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global and default configurations passed in. If either of
    * these are null, system defaults are used.
    *
    * @param globalConfiguration  global configuration to use. If null, a default instance is created.
    * @param defaultConfiguration default configuration to use. If null, a default instance is created.
    * @param start                if true, the cache manager is started
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, Configuration defaultConfiguration,
                              boolean start) {
      this.globalConfiguration = globalConfiguration == null ? new GlobalConfigurationBuilder().build() : globalConfiguration;
      this.defaultConfiguration = defaultConfiguration == null ? new ConfigurationBuilder().build() : defaultConfiguration;
      this.globalComponentRegistry = new GlobalComponentRegistry(this.globalConfiguration, this, caches.keySet());
      this.authzHelper = new AuthorizationHelper(this.globalConfiguration.security(), AuditContext.CACHEMANAGER, this.globalConfiguration.globalJmxStatistics().cacheManagerName());
      this.stats = new CacheContainerStatsImpl(this);
      if (start)
         start();
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the configuration file name passed in. This
    * constructor first searches for the named file on the classpath, and failing that, treats the file name as an
    * absolute path.
    *
    * @param configurationFile name of configuration file to use as a template for all caches created
    *
    * @throws java.io.IOException if there is a problem with the configuration file.
    */
   public DefaultCacheManager(String configurationFile) throws IOException {
      this(configurationFile, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the configuration file name passed in. This constructor first
    * searches for the named file on the classpath, and failing that, treats the file name as an absolute path.
    *
    * @param configurationFile name of configuration file to use as a template for all caches created
    * @param start             if true, the cache manager is started
    *
    * @throws java.io.IOException if there is a problem with the configuration file.
    */
   public DefaultCacheManager(String configurationFile, boolean start) throws IOException {
	  this(FileLookupFactory.newInstance().lookupFileStrict(configurationFile, Thread.currentThread().getContextClassLoader()), start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the input stream passed in to read configuration
    * file contents.
    *
    * @param configurationStream stream containing configuration file contents, to use as a template for all caches
    *                            created
    *
    * @throws java.io.IOException if there is a problem with the configuration stream.
    */
   public DefaultCacheManager(InputStream configurationStream) throws IOException {
      this(configurationStream, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the input stream passed in to read configuration file
    * contents.
    *
    * @param configurationStream stream containing configuration file contents, to use as a template for all caches
    *                            created
    * @param start               if true, the cache manager is started
    *
    * @throws java.io.IOException if there is a problem reading the configuration stream
    */
   public DefaultCacheManager(InputStream configurationStream, boolean start) throws IOException {
      this(new ParserRegistry().parse(configurationStream), start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the holder passed in to read configuration settings.
    *
    * @param holder holder containing configuration settings, to use as a template for all caches
    *                            created
    * @param start               if true, the cache manager is started
    */
   public DefaultCacheManager(ConfigurationBuilderHolder holder, boolean start) {
      try {
         globalConfiguration = holder.getGlobalConfigurationBuilder().build();
         defaultConfiguration = holder.getDefaultConfigurationBuilder().build(globalConfiguration);

         for (Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
            ConfigurationBuilder builder = entry.getValue();
            org.infinispan.configuration.cache.Configuration c = builder.build(globalConfiguration);
            configurationOverrides.put(entry.getKey(), c);
         }

         globalComponentRegistry = new GlobalComponentRegistry(globalConfiguration, this, caches.keySet());
         authzHelper = new AuthorizationHelper(globalConfiguration.security(), AuditContext.CACHEMANAGER, globalConfiguration.globalJmxStatistics().cacheManagerName());
         stats = new CacheContainerStatsImpl(this);
      } catch (CacheConfigurationException ce) {
         throw ce;
      } catch (RuntimeException re) {
         throw new CacheConfigurationException(re);
      }
      if (start)
         start();
   }

   /**
    * Constructs a new instance of the CacheManager, using the two configuration file names passed in. The first file
    * contains the GlobalConfiguration configuration The second file contain the Default configuration. The third
    * filename contains the named cache configuration This constructor first searches for the named file on the
    * classpath, and failing that, treats the file name as an absolute path.
    *
    * @param start                    if true, the cache manager is started
    * @param globalConfigurationFile  name of file that contains the global configuration
    * @param defaultConfigurationFile name of file that contains the default configuration
    * @param namedCacheFile           name of file that contains the named cache configuration
    *
    * @throws java.io.IOException if there is a problem with the configuration file.
    */
   @Deprecated
   public DefaultCacheManager(String globalConfigurationFile, String defaultConfigurationFile, String namedCacheFile,
                              boolean start) throws IOException {
      ParserRegistry parserRegistry = new ParserRegistry();

      ConfigurationBuilderHolder globalConfigurationBuilderHolder = parserRegistry.parseFile(globalConfigurationFile);
      ConfigurationBuilderHolder defaultConfigurationBuilderHolder = parserRegistry.parseFile(defaultConfigurationFile);

      globalConfiguration = globalConfigurationBuilderHolder.getGlobalConfigurationBuilder().build();
      defaultConfiguration = defaultConfigurationBuilderHolder.getDefaultConfigurationBuilder().build(globalConfiguration);

      if (namedCacheFile != null) {
         ConfigurationBuilderHolder namedConfigurationBuilderHolder = parserRegistry.parseFile(namedCacheFile);
         Entry<String, ConfigurationBuilder> entry = namedConfigurationBuilderHolder.getNamedConfigurationBuilders().entrySet().iterator().next();
         ConfigurationBuilder builder = entry.getValue();
         configurationOverrides.put(entry.getKey(), builder.build(globalConfiguration));
      }

      globalComponentRegistry = new GlobalComponentRegistry(this.globalConfiguration, this, caches.keySet());
      authzHelper = new AuthorizationHelper(globalConfiguration.security(), AuditContext.CACHEMANAGER, globalConfiguration.globalJmxStatistics().cacheManagerName());
      stats = new CacheContainerStatsImpl(this);
      if (start)
         start();
   }

   @Override
   public Configuration defineConfiguration(String cacheName, Configuration configuration) {
      return defineConfiguration(cacheName, configuration, null, true);
   }

   @Override
   public Configuration defineConfiguration(String cacheName, String templateName, Configuration configurationOverride) {
      if (templateName != null) {
         Configuration c = DEFAULT_CACHE_NAME.equals(templateName) ? defaultConfiguration : configurationOverrides.get(templateName);
         if (c != null)
            return defineConfiguration(cacheName, configurationOverride, c, false);
         return defineConfiguration(cacheName, configurationOverride);
      }
      return defineConfiguration(cacheName, configurationOverride);
   }

   private Configuration defineConfiguration(String cacheName, Configuration configOverride,
                                             Configuration defaultConfigIfNotPresent, boolean checkExisting) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      assertIsNotTerminated();
      if (cacheName == null || configOverride == null)
         throw new NullPointerException("Null arguments not allowed");
      if (cacheName.equals(DEFAULT_CACHE_NAME))
         throw log.illegalCacheName(DEFAULT_CACHE_NAME);
      if (checkExisting) {
         Configuration existing = configurationOverrides.get(cacheName);
         if (existing != null) {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.read(existing);
            builder.read(configOverride);
            Configuration configuration = builder.build(globalConfiguration);
            configurationOverrides.put(cacheName, configuration);
            return configuration;
         }
      }
      ConfigurationBuilder builder = new ConfigurationBuilder();
      if (defaultConfigIfNotPresent != null) {
         builder.read(defaultConfigIfNotPresent);
      }
      builder.read(configOverride);
      Configuration configuration = builder.build(globalConfiguration);
      configurationOverrides.put(cacheName, configuration);
      return configuration;
   }

   @Override
   public void undefineConfiguration(String configurationName) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      if (configurationName.equals(DEFAULT_CACHE_NAME))
         throw log.illegalCacheName(DEFAULT_CACHE_NAME);
      Configuration existing = configurationOverrides.get(configurationName);
      if (existing != null) {
         for(CacheWrapper cache : caches.values()) {
            if (cache.getCache().getCacheConfiguration() == existing && cache.getCache().getStatus() != ComponentStatus.TERMINATED) {
               throw log.configurationInUse(configurationName);
            }
         }
         configurationOverrides.remove(configurationName);
      }
   }

   /**
    * Retrieves the default cache associated with this cache manager. Note that the default cache does not need to be
    * explicitly created with {@link #createCache(String)} since it is automatically created lazily when first used.
    * <p/>
    * As such, this method is always guaranteed to return the default cache.
    *
    * @return the default cache.
    */
   @Override
   public <K, V> Cache<K, V> getCache() {
      return getCache(DEFAULT_CACHE_NAME);
   }

   /**
    * Retrieves a named cache from the system. If the cache has been previously created with the same name, the running
    * cache instance is returned. Otherwise, this method attempts to create the cache first.
    * <p/>
    * When creating a new cache, this method will use the configuration passed in to the CacheManager on construction,
    * as a template, and then optionally apply any overrides previously defined for the named cache using the {@link
    * #defineConfiguration(String, Configuration)} or {@link #defineConfiguration(String, String, Configuration)}
    * methods, or declared in the configuration file.
    *
    * @param cacheName name of cache to retrieve
    *
    * @return a cache instance identified by cacheName
    */
   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      assertIsNotTerminated();
      if (cacheName == null)
         throw new NullPointerException("Null arguments not allowed");

      CacheWrapper cw = caches.get(cacheName);
      if (cw != null) {
         return cw.getCache();
      }

      return createCache(cacheName);
   }

   @Override
   public boolean cacheExists(String cacheName) {
      return caches.containsKey(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent) {
      boolean cacheExists = cacheExists(cacheName);
      if (!cacheExists && !createIfAbsent)
         return null;
      else
         return getCache(cacheName);
   }

   @Override
   public EmbeddedCacheManager startCaches(final String... cacheNames) {
      authzHelper.checkPermission(AuthorizationPermission.LIFECYCLE);
      List<Thread> threads = new ArrayList<Thread>(cacheNames.length);
      final AtomicReference<RuntimeException> exception = new AtomicReference<RuntimeException>(null);
      for (final String cacheName : cacheNames) {

         String threadName = "CacheStartThread," + globalConfiguration.transport().nodeName() + "," + cacheName;
         Thread thread = new Thread(threadName) {
            @Override
            public void run() {
               try {
                  createCache(cacheName);
               } catch (RuntimeException e) {
                  exception.set(e);
               } catch (Throwable t) {
                  exception.set(new RuntimeException(t));
               }
            }
         };
         thread.start();
         threads.add(thread);
      }
      try {
         for (Thread thread : threads) {
            thread.join();
         }
      } catch (InterruptedException e) {
         throw new CacheException("Interrupted while waiting for the caches to start");
      }

      if (exception.get() != null) {
         throw exception.get();
      }

      return this;
   }

   @Override
   public void removeCache(String cacheName) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      ComponentRegistry cacheComponentRegistry = globalComponentRegistry.getNamedComponentRegistry(cacheName);
      if (cacheComponentRegistry != null) {
         RemoveCacheCommand cmd = new RemoveCacheCommand(cacheName, this, globalComponentRegistry,
               cacheComponentRegistry.getComponent(PersistenceManager.class),
               cacheComponentRegistry.getComponent(CacheJmxRegistration.class));
         Transport transport = getTransport();
         try {
            if (transport != null) {
               Configuration c = getConfiguration(cacheName);
               // Use sync replication timeout
               transport.invokeRemotely(null, cmd, ResponseMode.SYNCHRONOUS, c.clustering().sync().replTimeout(), null, DeliverOrder.NONE, false);
            }
            // Once sent to the cluster, remove the local cache
            cmd.perform(null);

            // Remove cache configuration and remove it from the computed cache name list
            configurationOverrides.remove(cacheName);
            // Remove cache from dependency graph
            cacheDependencyGraph.remove(cacheName);
         } catch (Throwable t) {
            throw new CacheException("Error removing cache", t);
         }
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public List<Address> getMembers() {
      Transport t = getTransport();
      return t == null ? null : t.getMembers();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Address getAddress() {
      Transport t = getTransport();
      return t == null ? null : t.getAddress();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Address getCoordinator() {
      Transport t = getTransport();
      return t == null ? null : t.getCoordinator();
   }

   @ManagedAttribute(description = "The logical address of the cluster's coordinator", displayName = "Coordinator address", displayType = DisplayType.SUMMARY)
   public String getCoordinatorAddress() {
      Transport t = getTransport();
      return t == null ? "N/A" : t.getCoordinator().toString();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @ManagedAttribute(description = "Indicates whether this node is coordinator", displayName = "Is coordinator?", displayType = DisplayType.SUMMARY)
   public boolean isCoordinator() {
      Transport t = getTransport();
      return t != null && t.isCoordinator();
   }

   private <K, V> Cache<K, V> createCache(String cacheName) {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         Cache<K, V> cache = wireAndStartCache(cacheName);
         // a null return value means the cache was created by someone else before we got the lock
         if (cache == null)
            return caches.get(cacheName).getCache();
         return cache;
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   /**
    * @return a null return value means the cache was created by someone else before we got the lock
    */
   private <K, V> Cache<K, V> wireAndStartCache(String cacheName) {
      CacheWrapper createdCacheWrapper = null;
      Configuration c = null;
      try {
         synchronized (caches) {
            //fetch it again with the lock held
            CacheWrapper existingCacheWrapper = caches.get(cacheName);
            if (existingCacheWrapper != null) {
               return null; //signal that the cache was created by someone else
            }
            c = getConfiguration(cacheName);
            if (c.security().authorization().enabled()) {
               // Don't even attempt to wire anything if we don't have LIFECYCLE privileges
               authzHelper.checkPermission(c.security().authorization(), AuthorizationPermission.LIFECYCLE);
            }
            createdCacheWrapper = new CacheWrapper();
            if (caches.put(cacheName, createdCacheWrapper) != null) {
               throw new IllegalStateException("attempt to initialize the cache twice");
            }
         }

         globalComponentRegistry.start();

         log.tracef("About to wire and start cache %s", cacheName);
         Cache<K, V> cache = new InternalCacheFactory<K, V>().createCache(c, globalComponentRegistry, cacheName);

         if(cache.getAdvancedCache().getAuthorizationManager() != null) {
            cache = new SecureCacheImpl<K, V>(cache.getAdvancedCache());
         }
         createdCacheWrapper.setCache(cache);

         // start the cache-level components
         cache.start();
         return cache;
      } finally {
         // allow other threads to access the cache
         if (createdCacheWrapper != null) {
            log.tracef("Closing latch for cache %s", cacheName);
            createdCacheWrapper.latch.countDown();
         }
      }
   }

   private Configuration getConfiguration(String cacheName) {
      Configuration c;
      if (cacheName.equals(DEFAULT_CACHE_NAME) || !configurationOverrides.containsKey(cacheName))
         c = new ConfigurationBuilder().read(defaultConfiguration).build(globalConfiguration);
      else
         c = configurationOverrides.get(cacheName);
      return c;
   }

   @Override
   public void start() {
      authzHelper.checkPermission(AuthorizationPermission.LIFECYCLE);
      if (globalConfiguration.security().authorization().enabled() && System.getSecurityManager() == null) {
         log.authorizationEnabledWithoutSecurityManager();
      }
      globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).start();
      String clusterName = globalConfiguration.transport().clusterName();
      String nodeName = globalConfiguration.transport().nodeName();
      if (globalConfiguration.security().authorization().enabled()) {
         globalConfiguration.security().authorization().principalRoleMapper().setContext(new PrincipalRoleMapperContextImpl(this));
      }
      log.debugf("Started cache manager %s on %s", clusterName, nodeName);
   }

   private void terminate(String cacheName) {
      CacheWrapper cacheWrapper = this.caches.get(cacheName);
      if (cacheWrapper != null && cacheWrapper.cache != null) {
         Cache<?, ?> cache = cacheWrapper.cache;
         unregisterCacheMBean(cache);
         cache.stop();
      }
   }

   @Override
   public void stop() {
      authzHelper.checkPermission(AuthorizationPermission.LIFECYCLE);
      if (!stopping) {
         synchronized (this) {
            // DCL to make sure that only one thread calls stop at one time,
            // and any other calls by other threads are ignored.
            if (!stopping) {
               log.debugf("Stopping cache manager %s on %s", globalConfiguration.transport().clusterName(), getAddress());
               stopping = true;
               Set<String> cachesToStop = new LinkedHashSet<>(this.caches.size());
               boolean defaultCacheHasDependency = false;
               // stop ordered caches first
               try {
                  List<String> ordered = cacheDependencyGraph.topologicalSort();
                  defaultCacheHasDependency = ordered.contains(DEFAULT_CACHE_NAME);
                  cachesToStop.addAll(ordered);
               } catch (CyclicDependencyException e) {
                  log.stopOrderIgnored();
               }
               cachesToStop.addAll(caches.keySet());
               // will be stopped by the GCR
               cachesToStop.remove(ClusterRegistryImpl.GLOBAL_REGISTRY_CACHE_NAME);
               // make sure we stop the default cache LAST!
               if(!defaultCacheHasDependency) {
                  cachesToStop.add(DEFAULT_CACHE_NAME);
               }
               for (String cacheName : cachesToStop) {
                  terminate(cacheName);
               }
               globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).stop();
               globalComponentRegistry.stop();

            } else {
               log.trace("Ignore call to stop as the cache manager is stopping");
            }
         }
      } else {
         log.trace("Ignore call to stop as the cache manager is stopping");
      }
   }

   private void unregisterCacheMBean(Cache<?, ?> cache) {
      // Unregister cache mbean regardless of jmx statistics setting
      cache.getAdvancedCache().getComponentRegistry().getComponent(CacheJmxRegistration.class)
              .unregisterCacheMBean();
   }

   @Override
   public void addListener(Object listener) {
      authzHelper.checkPermission(AuthorizationPermission.LISTEN);
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      notifier.addListener(listener);
   }

   @Override
   public void removeListener(Object listener) {
      authzHelper.checkPermission(AuthorizationPermission.LISTEN);
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      notifier.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      authzHelper.checkPermission(AuthorizationPermission.LISTEN);
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      return notifier.getListeners();
   }

   @Override
   public ComponentStatus getStatus() {
      authzHelper.checkPermission(AuthorizationPermission.LIFECYCLE);
      return globalComponentRegistry.getStatus();
   }

   @Override
   public GlobalConfiguration getCacheManagerConfiguration() {
      return globalConfiguration;
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getDefaultCacheConfiguration() {
      return defaultConfiguration;
   }

   @Override
   public Configuration getCacheConfiguration(String name) {
      Configuration configuration = configurationOverrides.get(name);
      if (configuration == null && cacheExists(name)) {
         return defaultConfiguration;
      }
      return configuration;
   }

   @Override
   public Set<String> getCacheNames() {
      // Get the XML/programmatically defined caches
      Set<String> names = new HashSet<String>(configurationOverrides.keySet());
      // Add the caches created dynamically without explicit config
      // Since caches could be modified dynamically, make a safe copy of keys
      names.addAll(Immutables.immutableSetConvert(caches.keySet()));
      names.remove(DEFAULT_CACHE_NAME);
      InternalCacheRegistry internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.filterInternalCaches(names);
      if (names.isEmpty())
         return InfinispanCollections.emptySet();
      else
         return Immutables.immutableSetWrap(names);
   }

   @Override
   public boolean isRunning(String cacheName) {
      CacheWrapper w = caches.get(cacheName);
      try {
         return w != null && w.latch.await(0, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         return false;
      }
   }

   @Override
   public boolean isDefaultRunning() {
      return isRunning(DEFAULT_CACHE_NAME);
   }

   @ManagedAttribute(description = "The status of the cache manager instance.", displayName = "Cache manager status", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getCacheManagerStatus() {
      return getStatus().toString();
   }

   @ManagedAttribute(description = "The defined cache names and their statuses.  The default cache is not included in this representation.", displayName = "List of defined caches", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getDefinedCacheNames() {
      StringBuilder result = new StringBuilder("[");
      for (String cacheName : getCacheNames()) {
         boolean started = caches.containsKey(cacheName);
         result.append(cacheName).append(started ? "(created)" : "(not created)");
      }
      result.append("]");
      return result.toString();
   }

   @ManagedAttribute(description = "The total number of defined caches, excluding the default cache.", displayName = "Number of caches defined", displayType = DisplayType.SUMMARY)
   public String getDefinedCacheCount() {
      return String.valueOf(this.configurationOverrides.keySet().size());
   }

   @ManagedAttribute(description = "The total number of created caches, including the default cache.", displayName = "Number of caches created", displayType = DisplayType.SUMMARY)
   public String getCreatedCacheCount() {
      return String.valueOf(this.caches.keySet().size());
   }

   @ManagedAttribute(description = "The total number of running caches, including the default cache.", displayName = "Number of running caches", displayType = DisplayType.SUMMARY)
   public String getRunningCacheCount() {
      int running = 0;
      for (CacheWrapper cachew : caches.values()) {
         Cache<?, ?> cache = cachew.cache;
         if (cache != null && cache.getStatus() == ComponentStatus.RUNNING)
            running++;
      }
      return String.valueOf(running);
   }

   @ManagedAttribute(description = "Returns the version of Infinispan", displayName = "Infinispan version", displayType = DisplayType.SUMMARY, dataType = DataType.TRAIT)
   public String getVersion() {
      return Version.getVersion();
   }

   @ManagedAttribute(description = "The name of this cache manager", displayName = "Cache manager name", displayType = DisplayType.SUMMARY, dataType = DataType.TRAIT)
   public String getName() {
      return globalConfiguration.globalJmxStatistics().cacheManagerName();
   }

   @ManagedOperation(description = "Starts the default cache associated with this cache manager", displayName = "Starts the default cache")
   public void startCache() {
      getCache();
   }

   @ManagedOperation(description = "Starts a named cache from this cache manager", name = "startCache", displayName = "Starts a cache with the given name")
   public void startCache(@Parameter(name = "cacheName", description = "Name of cache to start") String cacheName) {
      getCache(cacheName);
   }

   @ManagedAttribute(description = "The network address associated with this instance", displayName = "Network address", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getNodeAddress() {
      return getLogicalAddressString();
   }

   @ManagedAttribute(description = "The physical network addresses associated with this instance", displayName = "Physical network addresses", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getPhysicalAddresses() {
      Transport t = getTransport();
      if (t == null) return "local";
      List<Address> address = t.getPhysicalAddresses();
      return address == null ? "local" : address.toString();
   }

   @ManagedAttribute(description = "List of members in the cluster", displayName = "Cluster members", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getClusterMembers() {
      Transport t = getTransport();
      if (t == null) return "local";
      List<Address> addressList = t.getMembers();
      return addressList.toString();
   }

   @ManagedAttribute(description = "Size of the cluster in number of nodes", displayName = "Cluster size", displayType = DisplayType.SUMMARY)
   public int getClusterSize() {
      Transport t = getTransport();
      if (t == null) return 1;
      return t.getMembers().size();
   }

   /**
    * {@inheritDoc}
    */
   @ManagedAttribute(description = "Cluster name", displayName = "Cluster name", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   @Override
   public String getClusterName() {
      return globalConfiguration.transport().clusterName();
   }

   private String getLogicalAddressString() {
      return getAddress() == null ? "local" : getAddress().toString();
   }

   private void assertIsNotTerminated() {
      if (globalComponentRegistry.getStatus().isTerminated())
         throw new IllegalLifecycleStateException(
               "Cache container has been stopped and cannot be reused. Recreate the cache container.");
   }

   @Override
   public Transport getTransport() {
      if (globalComponentRegistry == null) return null;
      return globalComponentRegistry.getComponent(Transport.class);
   }

   @Override
   public GlobalComponentRegistry getGlobalComponentRegistry() {
      return globalComponentRegistry;
   }

   @Override
   public void addCacheDependency(String from, String to) {
      cacheDependencyGraph.addDependency(from, to);
   }

   @Override
   public String toString() {
      return super.toString() + "@Address:" + getAddress();
   }

   private final static class CacheWrapper {
      private volatile Cache<?, ?> cache;
      private final CountDownLatch latch = new CountDownLatch(1);

      public void setCache(Cache<?, ?> cache) {
         this.cache = cache;
      }

      @SuppressWarnings("unchecked")
      private <K, V> Cache<K, V> getCache() {
         try {
            latch.await();
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
         return (Cache<K, V>) cache;
      }
   }

   /**
    * {@inheritDoc}
    */
   @ManagedAttribute(description = "Global configuration properties", displayName = "Global configuration properties", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public Properties getGlobalConfigurationAsProperties() {
      return new PropertyFormatter().format(globalConfiguration);
   }

   @Override
   public CacheContainerStats getStats() {
      return stats;
   }
}
