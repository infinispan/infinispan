package org.infinispan.manager;

import static org.infinispan.factories.KnownComponentNames.CACHE_DEPENDENCY_GRAPH;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfiguration;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.InternalCacheFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.health.Health;
import org.infinispan.health.impl.HealthImpl;
import org.infinispan.health.impl.jmx.HealthJMXExposerImpl;
import org.infinispan.health.jmx.HealthJMXExposer;
import org.infinispan.jmx.CacheJmxRegistration;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.impl.ClusterExecutors;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationHelper;
import org.infinispan.security.impl.PrincipalRoleMapperContextImpl;
import org.infinispan.security.impl.SecureCacheImpl;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.stats.impl.CacheContainerStatsImpl;
import org.infinispan.util.ByteString;
import org.infinispan.util.CyclicDependencyException;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private final ConcurrentMap<String, CompletableFuture<Cache<?, ?>>> caches = CollectionFactory.makeConcurrentMap();
   private final GlobalComponentRegistry globalComponentRegistry;
   private final AuthorizationHelper authzHelper;
   private final DependencyGraph<String> cacheDependencyGraph = new DependencyGraph<>();
   private final CacheContainerStats stats;
   private final Health health;
   private final ConfigurationManager configurationManager;
   private final String defaultCacheName;

   private final Lock lifecycleLock = new ReentrantLock();
   private volatile boolean stopping;

   /**
    * Constructs and starts a default instance of the CacheManager, using configuration defaults. See
    * {@link org.infinispan.configuration.cache.Configuration} and {@link org.infinispan.configuration.global.GlobalConfiguration}
    * for details of these defaults.
    */
   public DefaultCacheManager() {
      this(null, null, true);
   }

   /**
    * Constructs a default instance of the CacheManager, using configuration defaults.  See
    * {@link org.infinispan.configuration.cache.Configuration} and {@link org.infinispan.configuration.global.GlobalConfiguration}
    * for details of these defaults.
    *
    * @param start if true, the cache manager is started
    */
   public DefaultCacheManager(boolean start) {
      this(null, null, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the default configuration passed in.  See
    * {@link org.infinispan.configuration.cache.Configuration} and {@link org.infinispan.configuration.global.GlobalConfiguration}
    * for details of these defaults.
    *
    * @param defaultConfiguration configuration to use as a template for all caches created
    */
   public DefaultCacheManager(Configuration defaultConfiguration) {
      this(null, defaultConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the default configuration passed in.  See
    * {@link org.infinispan.configuration.global.GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration file to use as a template for all caches created
    * @param start                if true, the cache manager is started
    */
   public DefaultCacheManager(Configuration defaultConfiguration, boolean start) {
      this(null, defaultConfiguration, start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global configuration passed in, and system
    * defaults for the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration}
    * for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration) {
      this(globalConfiguration, null, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global configuration passed in, and system defaults for
    * the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration}
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
      globalConfiguration = globalConfiguration == null ? new GlobalConfigurationBuilder().build() : globalConfiguration;
      this.configurationManager = new ConfigurationManager(globalConfiguration);
      if (defaultConfiguration != null) {
         if (globalConfiguration.defaultCacheName().isPresent()) {
            defaultCacheName = globalConfiguration.defaultCacheName().get();
         } else {
            log.defaultCacheConfigurationWithoutName();
            defaultCacheName = DEFAULT_CACHE_NAME;
         }
         configurationManager.putConfiguration(defaultCacheName, defaultConfiguration);
      } else {
         if (globalConfiguration.defaultCacheName().isPresent()) {
            throw log.missingDefaultCacheDeclaration(globalConfiguration.defaultCacheName().get());
         } else {
            defaultCacheName = null;
         }
      }
      this.authzHelper = new AuthorizationHelper(globalConfiguration.security(), AuditContext.CACHEMANAGER, globalConfiguration.globalJmxStatistics().cacheManagerName());
      this.globalComponentRegistry = new GlobalComponentRegistry(globalConfiguration, this, caches.keySet());
      this.globalComponentRegistry.registerComponent(configurationManager, ConfigurationManager.class);
      this.globalComponentRegistry.registerComponent(cacheDependencyGraph, CACHE_DEPENDENCY_GRAPH, false);
      this.globalComponentRegistry.registerComponent(authzHelper, AuthorizationHelper.class);
      this.stats = new CacheContainerStatsImpl(this);
      health = new HealthImpl(this);
      globalComponentRegistry.registerComponent(new HealthJMXExposerImpl(health), HealthJMXExposer.class);
      if (start)
         start();
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the configuration file name passed in. This
    * constructor first searches for the named file on the classpath, and failing that, treats the file name as an
    * absolute path.
    *
    * @param configurationFile name of configuration file to use as a template for all caches created
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
    * @throws java.io.IOException if there is a problem reading the configuration stream
    */
   public DefaultCacheManager(InputStream configurationStream, boolean start) throws IOException {
      this(new ParserRegistry().parse(configurationStream), start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the holder passed in to read configuration settings.
    *
    * @param holder holder containing configuration settings, to use as a template for all caches created
    * @param start  if true, the cache manager is started
    */
   public DefaultCacheManager(ConfigurationBuilderHolder holder, boolean start) {
      try {
         configurationManager = new ConfigurationManager(holder);
         GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
         defaultCacheName = globalConfiguration.defaultCacheName().orElse(null);
         globalComponentRegistry = new GlobalComponentRegistry(globalConfiguration, this, caches.keySet());
         globalComponentRegistry.registerComponent(configurationManager, ConfigurationManager.class);
         globalComponentRegistry.registerComponent(cacheDependencyGraph, CACHE_DEPENDENCY_GRAPH, false);
         authzHelper = new AuthorizationHelper(globalConfiguration.security(), AuditContext.CACHEMANAGER, globalConfiguration.globalJmxStatistics().cacheManagerName());
         stats = new CacheContainerStatsImpl(this);
         health = new HealthImpl(this);
         globalComponentRegistry.registerComponent(new HealthJMXExposerImpl(health), HealthJMXExposer.class);
      } catch (CacheConfigurationException ce) {
         throw ce;
      } catch (RuntimeException re) {
         throw new CacheConfigurationException(re);
      }
      if (start)
         start();
   }

   @Override
   public Configuration defineConfiguration(String name, Configuration configuration) {
      return doDefineConfiguration(name, configuration);
   }

   @Override
   public Configuration defineConfiguration(String name, String template, Configuration configurationOverride) {
      if (template != null) {
         Configuration c = configurationManager.getConfiguration(template);
         if (c == null) {
            throw log.undeclaredConfiguration(template, name);
         } else if (configurationOverride == null) {
            return doDefineConfiguration(name, c);
         } else {
            return doDefineConfiguration(name, c, configurationOverride);
         }
      }
      return doDefineConfiguration(name, configurationOverride);
   }

   private Configuration doDefineConfiguration(String name, Configuration... configurations) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      assertIsNotTerminated();
      if (name == null || configurations == null)
         throw new NullPointerException("Null arguments not allowed");

      if (name.equals(DEFAULT_CACHE_NAME))
         throw log.illegalCacheName(DEFAULT_CACHE_NAME);
      Configuration existing = configurationManager.getConfiguration(name);
      if (existing != null) {
         throw log.configAlreadyDefined(name);
      }
      ConfigurationBuilder builder = new ConfigurationBuilder();
      boolean template = true;
      for (Configuration configuration : configurations) {
         if (configuration != null) {
            builder.read(configuration);
            template = template && configuration.isTemplate();
         } else {
            throw new NullPointerException("Null arguments not allowed");
         }
      }
      builder.template(template);
      return configurationManager.putConfiguration(name, builder);
   }

   @Override
   public void undefineConfiguration(String configurationName) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      if (configurationName.equals(DEFAULT_CACHE_NAME))
         throw log.illegalCacheName(DEFAULT_CACHE_NAME);
      Configuration existing = configurationManager.getConfiguration(configurationName);
      if (existing != null) {
         for (CompletableFuture<Cache<?, ?>> cacheFuture : caches.values()) {
            Cache<?, ?> cache = cacheFuture.exceptionally(t -> null).join();
            if (cache != null && cache.getCacheConfiguration() == existing && cache.getStatus() != ComponentStatus.TERMINATED) {
               throw log.configurationInUse(configurationName);
            }
         }
         configurationManager.removeConfiguration(configurationName);
      }
   }

   /**
    * Retrieves the default cache associated with this cache manager. Note that the default cache does not need to be
    * explicitly created with {@link #createCache(String, String)} (String)} since it is automatically created lazily
    * when first used.
    * <p/>
    * As such, this method is always guaranteed to return the default cache.
    *
    * @return the default cache.
    */
   @Override
   public <K, V> Cache<K, V> getCache() {
      if (defaultCacheName == null) {
         throw log.noDefaultCache();
      }
      return internalGetCache(defaultCacheName, defaultCacheName);
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
    * @return a cache instance identified by cacheName
    */
   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      return getCache(cacheName, cacheName);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, String configurationName) {
      if (cacheName == null)
         throw new NullPointerException("Null arguments not allowed");
      if (DEFAULT_CACHE_NAME.equals(cacheName)) {
         if (defaultCacheName == null) {
            throw log.noDefaultCache();
         }
         cacheName = defaultCacheName;
         log.deprecatedDefaultCache();
      }
      return internalGetCache(cacheName, configurationName);
   }

   public <K, V> Cache<K, V> internalGetCache(String cacheName, String configurationName) {
      assertIsNotTerminated();
      CompletableFuture<Cache<?, ?>> cacheFuture = caches.get(cacheName);
      if (cacheFuture != null) {
         try {
            return ((Cache<K, V>) cacheFuture.join());
         } catch (CompletionException e) {
            throw ((CacheException) e.getCause());
         }
      }

      return createCache(cacheName, configurationName);
   }

   @Override
   public boolean cacheExists(String cacheName) {
      return caches.containsKey(cacheName);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent) {
      return getCache(cacheName, cacheName, createIfAbsent);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, String configurationTemplate, boolean createIfAbsent) {
      boolean cacheExists = cacheExists(cacheName);
      if (!cacheExists && !createIfAbsent)
         return null;
      else
         return getCache(cacheName, configurationTemplate);
   }

   @Override
   public EmbeddedCacheManager startCaches(final String... cacheNames) {
      authzHelper.checkPermission(AuthorizationPermission.LIFECYCLE);
      Map<String, Thread> threads = new HashMap<>(cacheNames.length);
      final AtomicReference<RuntimeException> exception = new AtomicReference<RuntimeException>(null);
      for (final String cacheName : cacheNames) {
         if (!threads.containsKey(cacheName)) {
            String threadName = "CacheStartThread," + configurationManager.getGlobalConfiguration().transport().nodeName() + "," + cacheName;
            Thread thread = new Thread(threadName) {
               @Override
               public void run() {
                  try {
                     createCache(cacheName, cacheName);
                  } catch (RuntimeException e) {
                     exception.set(e);
                  } catch (Throwable t) {
                     exception.set(new RuntimeException(t));
                  }
               }
            };
            thread.start();
            threads.put(cacheName, thread);
         }
      }
      try {
         for (Thread thread : threads.values()) {
            thread.join();
         }
      } catch (InterruptedException e) {
         throw new CacheException("Interrupted while waiting for the caches to start");
      }

      RuntimeException runtimeException = exception.get();
      if (runtimeException != null) {
         throw runtimeException;
      }

      return this;
   }

   @Override
   public void removeCache(String cacheName) {
      authzHelper.checkPermission(AuthorizationPermission.ADMIN);
      ComponentRegistry cacheComponentRegistry = globalComponentRegistry.getNamedComponentRegistry(cacheName);
      if (cacheComponentRegistry != null) {
         RemoveCacheCommand cmd = new RemoveCacheCommand(ByteString.fromString(cacheName), this);
         Transport transport = getTransport();
         try {
            CompletableFuture<?> future;
            if (transport != null) {
               Configuration c = configurationManager.getConfiguration(cacheName, defaultCacheName);
               // Use sync replication timeout
               CompletableFuture<Map<Address, Response>> remoteFuture =
                     transport.invokeRemotelyAsync(null, cmd, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
                           c.clustering().remoteTimeout(), null, DeliverOrder.NONE, false);
               future = cmd.invokeAsync().thenCompose(o -> remoteFuture);
            } else {
               future = cmd.invokeAsync();
            }

            future.get();
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

   private <K, V> Cache<K, V> createCache(String cacheName, String configurationName) {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         return wireAndStartCache(cacheName, configurationName);
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   /**
    * @return a null return value means the cache was created by someone else before we got the lock
    */
   private <K, V> Cache<K, V> wireAndStartCache(String cacheName, String configurationName) {
      boolean sameCache = cacheName.equals(configurationName);
      Configuration c = configurationManager.getConfiguration(configurationName, defaultCacheName);
      if (c == null) {
         throw log.noSuchCacheConfiguration(configurationName);
      } else if (!sameCache) {
         Configuration definedConfig = configurationManager.getConfiguration(cacheName);
         if (definedConfig != null) {
            log.warnAttemptToOverrideExistingConfiguration(cacheName);
            c = definedConfig;
         }
      }

      if (c.security().authorization().enabled()) {
         // Don't even attempt to wire anything if we don't have LIFECYCLE privileges
         authzHelper.checkPermission(c.security().authorization(), AuthorizationPermission.LIFECYCLE);
      }
      if (c.isTemplate() && cacheName.equals(configurationName)) {
         throw log.templateConfigurationStartAttempt(cacheName);
      }

      CompletableFuture<Cache<?, ?>> cacheFuture = new CompletableFuture<>();
      CompletableFuture<Cache<?, ?>> oldFuture = caches.computeIfAbsent(cacheName, name -> {
         assertIsNotTerminated();
         return cacheFuture;
      });
      try {
         if (oldFuture != cacheFuture) {
            return (Cache<K, V>) oldFuture.join();
         }
      } catch (CompletionException ce) {
         throw ((CacheException) ce.getCause());
      }

      try {
         log.tracef("About to wire and start cache %s", cacheName);
         Cache<K, V> cache = new InternalCacheFactory<K, V>().createCache(c, globalComponentRegistry, cacheName);
         ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();

         if (cache.getAdvancedCache().getAuthorizationManager() != null) {
            cache = new SecureCacheImpl<K, V>(cache.getAdvancedCache());
         }

         boolean notStartedYet =
               cr.getStatus() != ComponentStatus.RUNNING && cr.getStatus() != ComponentStatus.INITIALIZING;
         // start the cache-level components
         cache.start();
         cacheFuture.complete(cache);
         boolean needToNotifyCacheStarted = notStartedYet && cr.getStatus() == ComponentStatus.RUNNING;
         if (needToNotifyCacheStarted) {
            globalComponentRegistry.notifyCacheStarted(cacheName);
         }
         log.tracef("Cache %s started", cacheName);
         return cache;
      } catch (CacheException e) {
         cacheFuture.completeExceptionally(e);
         throw e;
      } catch (Throwable t) {
         cacheFuture.completeExceptionally(new CacheException(t));
         throw t;
      }
   }

   @Override
   public void start() {
      authzHelper.checkPermission(AuthorizationPermission.LIFECYCLE);
      lifecycleLock.lock();
      try {
         final GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
         if (globalConfiguration.security().authorization().enabled() && System.getSecurityManager() == null) {
            log.authorizationEnabledWithoutSecurityManager();
         }
         globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).start();
         String clusterName = globalConfiguration.transport().clusterName();
         String nodeName = globalConfiguration.transport().nodeName();
         if (globalConfiguration.security().authorization().enabled()) {
            globalConfiguration.security().authorization().principalRoleMapper().setContext(
                  new PrincipalRoleMapperContextImpl(this));
         }
         globalComponentRegistry.start();
         log.debugf("Started cache manager %s on %s", clusterName, nodeName);
      } finally {
         lifecycleLock.unlock();
      }
   }

   private void terminate(String cacheName) {
      CompletableFuture<Cache<?, ?>> cacheFuture = this.caches.get(cacheName);
      if (cacheFuture != null) {
         Cache<?, ?> cache = cacheFuture.join();
         unregisterCacheMBean(cache);
         if (cache.getStatus().isTerminated()) {
            log.tracef("Ignoring cache %s, it is already terminated.", cacheName);
            return;
         }
         cache.stop();
      }
   }

   @Override
   public void stop() {
      authzHelper.checkPermission(AuthorizationPermission.LIFECYCLE);

      lifecycleLock.lock();
      try {
         if (stopping) {
            log.trace("Ignore call to stop as the cache manager is not running");
            return;
         }

         log.debugf("Stopping cache manager %s on %s", configurationManager.getGlobalConfiguration().transport().clusterName(), getAddress());
         stopping = true;
         stopCaches();
         globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).stop();
         globalComponentRegistry.stop();
      } finally {
         lifecycleLock.unlock();
      }
   }

   private void stopCaches() {
      Set<String> cachesToStop = new LinkedHashSet<>(this.caches.size());
      // stop ordered caches first
      try {
         List<String> ordered = cacheDependencyGraph.topologicalSort();
         cachesToStop.addAll(ordered);
      } catch (CyclicDependencyException e) {
         log.stopOrderIgnored();
      }
      // The caches map includes the default cache
      cachesToStop.addAll(caches.keySet());
      log.tracef("Cache stop order: %s", cachesToStop);

      for (String cacheName : cachesToStop) {
         try {
            terminate(cacheName);
         } catch (Throwable t) {
            log.componentFailedToStop(t);
         }
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
      return configurationManager.getGlobalConfiguration();
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getDefaultCacheConfiguration() {
      if (defaultCacheName != null) {
         return configurationManager.getConfiguration(defaultCacheName);
      } else {
         return null;
      }
   }

   @Override
   public Configuration getCacheConfiguration(String name) {
      Configuration configuration = configurationManager.getConfiguration(name);
      if (configuration == null && cacheExists(name)) {
         return getDefaultCacheConfiguration();
      }
      return configuration;
   }

   @Override
   public Set<String> getCacheNames() {
      // Get the XML/programmatically defined caches
      Set<String> names = new HashSet<>(configurationManager.getDefinedCaches());
      // Add the caches created dynamically without explicit config
      // Since caches could be modified dynamically, make a safe copy of keys
      names.addAll(Immutables.immutableSetConvert(caches.keySet()));
      names.remove(DEFAULT_CACHE_NAME);
      InternalCacheRegistry internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.filterPrivateCaches(names);
      if (names.isEmpty())
         return Collections.emptySet();
      else
         return Immutables.immutableSetWrap(names);
   }

   @Override
   public Set<String> getCacheConfigurationNames() {
      // Get the XML/programmatically defined caches
      Set<String> names = new HashSet<>(configurationManager.getDefinedConfigurations());
      names.remove(DEFAULT_CACHE_NAME);
      InternalCacheRegistry internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.filterPrivateCaches(names);
      if (names.isEmpty())
         return Collections.emptySet();
      else
         return Immutables.immutableSetWrap(names);
   }

   @Override
   public boolean isRunning(String cacheName) {
      CompletableFuture<Cache<?, ?>> cacheFuture = caches.get(cacheName);
      boolean started = cacheFuture != null && cacheFuture.isDone() && !cacheFuture.isCompletedExceptionally();
      return started && cacheFuture.join().getStatus() == ComponentStatus.RUNNING;
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

   @ManagedAttribute(description = "The defined cache configuration names.", displayName = "List of defined cache configurations", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public String getDefinedCacheConfigurationNames() {
      StringBuilder result = new StringBuilder("[");
      boolean comma = false;
      for (String cacheName : getCacheConfigurationNames()) {
         if (comma)
            result.append(",");
         else
            comma = true;
         result.append(cacheName);
      }
      result.append("]");
      return result.toString();
   }

   @ManagedAttribute(description = "The total number of defined cache configurations.", displayName = "Number of caches defined", displayType = DisplayType.SUMMARY)
   public String getDefinedCacheCount() {
      return String.valueOf(configurationManager.getDefinedCaches().size());
   }

   @ManagedAttribute(description = "The total number of created caches, including the default cache.", displayName = "Number of caches created", displayType = DisplayType.SUMMARY)
   public String getCreatedCacheCount() {
      return String.valueOf(this.caches.keySet().size());
   }

   @ManagedAttribute(description = "The total number of running caches, including the default cache.", displayName = "Number of running caches", displayType = DisplayType.SUMMARY)
   public String getRunningCacheCount() {
      long running = caches.keySet().stream().filter(this::isRunning).count();
      return String.valueOf(running);
   }

   @ManagedAttribute(description = "Returns the version of Infinispan", displayName = "Infinispan version", displayType = DisplayType.SUMMARY, dataType = DataType.TRAIT)
   public String getVersion() {
      return Version.getVersion();
   }

   @ManagedAttribute(description = "The name of this cache manager", displayName = "Cache manager name", displayType = DisplayType.SUMMARY, dataType = DataType.TRAIT)
   public String getName() {
      return configurationManager.getGlobalConfiguration().globalJmxStatistics().cacheManagerName();
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
      return configurationManager.getGlobalConfiguration().transport().clusterName();
   }

   private String getLogicalAddressString() {
      return getAddress() == null ? "local" : getAddress().toString();
   }

   private void assertIsNotTerminated() {
      if (stopping)
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

   /**
    * {@inheritDoc}
    */
   @ManagedAttribute(description = "Global configuration properties", displayName = "Global configuration properties", dataType = DataType.TRAIT, displayType = DisplayType.SUMMARY)
   public Properties getGlobalConfigurationAsProperties() {
      return new PropertyFormatter().format(configurationManager.getGlobalConfiguration());
   }

   @Override
   public CacheContainerStats getStats() {
      return stats;
   }

   @Override
   public Health getHealth() {
      return health;
   }

   @Override
   public ClusterExecutor executor() {
      if (globalComponentRegistry.getStatus() != ComponentStatus.RUNNING) {
         throw new IllegalStateException("CacheManager must be started before retrieving a ClusterExecutor!");
      }
      JGroupsTransport transport = (JGroupsTransport) globalComponentRegistry.getComponent(Transport.class);
      if (transport != null) {
         long time = getCacheManagerConfiguration().transport().distributedSyncTimeout();
         return ClusterExecutors.allSubmissionExecutor(null, this, transport, time, TimeUnit.MILLISECONDS,
               globalComponentRegistry.getComponent(ExecutorService.class, KnownComponentNames.REMOTE_COMMAND_EXECUTOR),
               globalComponentRegistry.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR));
      } else {
         return ClusterExecutors.allSubmissionExecutor(null, this, null,
               TransportConfiguration.DISTRIBUTED_SYNC_TIMEOUT.getDefaultValue(), TimeUnit.MILLISECONDS, ForkJoinPool.commonPool(),
               globalComponentRegistry.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR));
      }
   }
}
