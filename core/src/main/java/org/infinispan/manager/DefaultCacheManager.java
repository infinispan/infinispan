package org.infinispan.manager;

import static org.infinispan.factories.KnownComponentNames.CACHE_DEPENDENCY_GRAPH;
import static org.infinispan.util.logging.Log.CONFIG;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AliasCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.io.ConfigurationResourceResolvers;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.internal.BlockHoundUtil;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.format.PropertyFormatter;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
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
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.health.Health;
import org.infinispan.health.impl.HealthImpl;
import org.infinispan.health.impl.jmx.HealthJMXExposerImpl;
import org.infinispan.health.jmx.HealthJMXExposer;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.impl.ClusterExecutors;
import org.infinispan.manager.impl.InternalCacheManager;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.GlobalSecurityManager;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.impl.AuthorizationManagerImpl;
import org.infinispan.security.impl.AuthorizationMapperContextImpl;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.security.impl.SecureCacheImpl;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.stats.impl.CacheContainerStatsImpl;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.ByteString;
import org.infinispan.util.CyclicDependencyException;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A <code>CacheManager</code> is the primary mechanism for retrieving a {@link Cache} instance, and is often used as a
 * starting point to using the {@link Cache}.
  * <code>CacheManager</code>s are heavyweight objects, and we foresee no more than one <code>CacheManager</code> being used per
 * JVM (unless specific configuration requirements require more than one; but either way, this would be a minimal and
 * finite number of instances).
  * Constructing a <code>CacheManager</code> is done via one of its constructors, which optionally take in a
 * {@link org.infinispan.configuration.cache.Configuration} or a path or URL to a configuration XML file.
  * Lifecycle - <code>CacheManager</code>s have a lifecycle (it implements {@link Lifecycle}) and the default constructors
 * also call {@link #start()}. Overloaded versions of the constructors are available, that do not start the
 * <code>CacheManager</code>, although it must be kept in mind that <code>CacheManager</code>s need to be started before they
 * can be used to create <code>Cache</code> instances.
  * Once constructed, <code>CacheManager</code>s should be made available to any component that requires a <code>Cache</code>,
 * via JNDI or via some other mechanism such as an IoC container.
  * You obtain <code>Cache</code> instances from the <code>CacheManager</code> by using one of the overloaded
 * <code>getCache()</code>, methods. Note that with <code>getCache()</code>, there is no guarantee that the instance you get is
 * brand-new and empty, since caches are named and shared. Because of this, the <code>CacheManager</code> also acts as a
 * repository of <code>Cache</code>s, and is an effective mechanism of looking up or creating <code>Cache</code>s on demand.
  * When the system shuts down, it should call {@link #stop()} on the <code>CacheManager</code>. This will ensure all caches
 * within its scope are properly stopped as well.
  * Sample usage:
 * <pre><code>
 *    CacheManager manager = CacheManager.getInstance("my-config-file.xml");
 *    Cache&lt;String, Person&gt; entityCache = manager.getCache("myEntityCache");
 *    entityCache.put("aPerson", new Person());
 *
 *    ConfigurationBuilder confBuilder = new ConfigurationBuilder();
 *    confBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
 *    manager.createCache("myReplicatedCache", confBuilder.build());
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
public class DefaultCacheManager extends InternalCacheManager {
   public static final String OBJECT_NAME = "CacheManager";
   private static final Log log = LogFactory.getLog(DefaultCacheManager.class);

   private final ConcurrentMap<String, CompletableFuture<Cache<?, ?>>> caches = new ConcurrentHashMap<>();
   private final GlobalComponentRegistry globalComponentRegistry;
   private final Authorizer authorizer;
   private final DependencyGraph<String> cacheDependencyGraph = new DependencyGraph<>();
   private final CacheContainerStats stats;
   private final Health health;
   private final ConfigurationManager configurationManager;
   private final String defaultCacheName;


   private final Lock lifecycleLock = new ReentrantLock();
   private final Condition lifecycleCondition = lifecycleLock.newCondition();
   private volatile ComponentStatus status = ComponentStatus.INSTANTIATED;

   private final DefaultCacheManagerAdmin cacheManagerAdmin;
   private final ClassAllowList classAllowList;
   private final CacheManagerInfo cacheManagerInfo;

   // Keep the transport around so async view listeners can still see the address after stop
   private volatile Transport transport;

   // When enabled, isRunning(name) sets the thread-local value and getCache(name) verifies it
   private static ThreadLocal<String> getCacheBlockingCheck;

   /**
    * Constructs and starts a default instance of the CacheManager, using configuration defaults. See
    * {@link org.infinispan.configuration.cache.Configuration} and
    * {@link org.infinispan.configuration.global.GlobalConfiguration} for details of these defaults.
    */
   public DefaultCacheManager() {
      this(true);
   }

   /**
    * Constructs a default instance of the CacheManager, using configuration defaults.  See
    * {@link org.infinispan.configuration.cache.Configuration} and
    * {@link org.infinispan.configuration.global.GlobalConfiguration} for details of these defaults.
    *
    * @param start if true, the cache manager is started
    */
   public DefaultCacheManager(boolean start) {
      this(new ConfigurationBuilderHolder(), start);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the global configuration passed in, and system
    * defaults for the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration}
    * for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration) {
      this(globalConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the global configuration passed in, and system defaults for
    * the default named cache configuration.  See {@link org.infinispan.configuration.cache.Configuration} for details
    * of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    * @param start               if true, the cache manager is started.
    */
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, boolean start) {
      this(new ConfigurationBuilderHolder(globalConfiguration.classLoader(), new GlobalConfigurationBuilder().read(globalConfiguration)), start);
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
      this(FileLookupFactory.newInstance().lookupFileLocationStrict(configurationFile, Thread.currentThread().getContextClassLoader()), start);
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
      this(new ParserRegistry().parse(configurationStream, ConfigurationResourceResolvers.DEFAULT, MediaType.APPLICATION_XML), start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the input stream passed in to read configuration file
    * contents.
    *
    * @param configurationURL stream containing configuration file contents, to use as a template for all caches
    *                         created
    * @param start            if true, the cache manager is started
    * @throws java.io.IOException if there is a problem reading the configuration stream
    */
   public DefaultCacheManager(URL configurationURL, boolean start) throws IOException {
      this(new ParserRegistry().parse(configurationURL), start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the holder passed in to read configuration settings.
    *
    * @param holder holder containing configuration settings, to use as a template for all caches created
    */
   public DefaultCacheManager(ConfigurationBuilderHolder holder) {
      this(holder, true);
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
         classAllowList = globalConfiguration.serialization().allowList().create();
         defaultCacheName = globalConfiguration.defaultCacheName().orElse(null);
         ModuleRepository moduleRepository = ModuleRepository.newModuleRepository(globalConfiguration.classLoader(), globalConfiguration);
         globalComponentRegistry = new GlobalComponentRegistry(globalConfiguration, this, caches.keySet(),
               moduleRepository, configurationManager);

         InternalCacheRegistry internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
         globalComponentRegistry.registerComponent(cacheDependencyGraph, CACHE_DEPENDENCY_GRAPH, false);

         stats = new CacheContainerStatsImpl(this);
         globalComponentRegistry.registerComponent(stats, CacheContainerStats.class);

         health = new HealthImpl(this, internalCacheRegistry);
         cacheManagerInfo = new CacheManagerInfo(this, getConfigurationManager(), internalCacheRegistry, globalComponentRegistry.getComponent(LocalTopologyManager.class));
         globalComponentRegistry.registerComponent(new HealthJMXExposerImpl(health), HealthJMXExposer.class);
         authorizer = new Authorizer(globalConfiguration.security(), AuditContext.CACHEMANAGER, globalConfiguration.cacheManagerName(), null);
         globalComponentRegistry.registerComponent(authorizer, Authorizer.class);
         cacheManagerAdmin = new DefaultCacheManagerAdmin(this, authorizer, EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class),
               null, globalComponentRegistry.getComponent(GlobalConfigurationManager.class));
      } catch (CacheConfigurationException ce) {
         throw ce;
      } catch (RuntimeException re) {
         throw new CacheConfigurationException(re);
      }
      if (start)
         start();
   }

   private DefaultCacheManager(DefaultCacheManager original) {
      this.authorizer = original.authorizer;
      this.cacheManagerInfo = original.cacheManagerInfo;
      this.cacheManagerAdmin = original.cacheManagerAdmin;
      this.classAllowList = original.classAllowList;
      this.defaultCacheName = original.defaultCacheName;
      this.configurationManager = original.configurationManager;
      this.globalComponentRegistry = original.globalComponentRegistry;
      this.health = original.health;
      this.stats = original.stats;
      this.status = original.status;
      this.transport = original.transport;
   }

   @Override
   public Configuration defineConfiguration(String name, Configuration configuration) {
      return doDefineConfiguration(name, configuration);
   }

   @Override
   public Configuration defineConfiguration(String name, String template, Configuration configurationOverride) {
      if (template != null) {
         Configuration c = configurationManager.getConfiguration(template, true);
         if (c == null) {
            throw CONFIG.undeclaredConfiguration(template, name);
         } else if (configurationOverride == null) {
            return doDefineConfiguration(name, c);
         } else {
            return doDefineConfiguration(name, c, configurationOverride);
         }
      }
      return doDefineConfiguration(name, configurationOverride);
   }

   private Configuration doDefineConfiguration(String name, Configuration... configurations) {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      assertIsNotTerminated();
      if (name == null || configurations == null)
         throw new NullPointerException("Null arguments not allowed");
      if (!ByteString.isValid(name))
         throw CONFIG.invalidNameSize(name);

      Configuration existing = configurationManager.getConfiguration(name, false);
      if (existing != null) {
         throw CONFIG.configAlreadyDefined(name);
      }
      ConfigurationBuilder builder = new ConfigurationBuilder();
      boolean template = true;
      for (Configuration configuration : configurations) {
         if (configuration != null) {
            builder.read(configuration, Combine.DEFAULT);
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
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      Configuration existing = configurationManager.getConfiguration(configurationName, false);
      if (existing != null) {
         for (CompletableFuture<Cache<?, ?>> cacheFuture : caches.values()) {
            Cache<?, ?> cache = cacheFuture.exceptionally(t -> null).join();
            if (cache != null && cache.getCacheConfiguration() == existing && cache.getStatus() != ComponentStatus.TERMINATED) {
               throw CONFIG.configurationInUse(configurationName);
            }
         }
         configurationManager.removeConfiguration(configurationName);
         globalComponentRegistry.removeCache(configurationName);
      }
   }

   @Override
   public <K, V> Cache<K, V> createCache(String name, Configuration configuration) {
      defineConfiguration(name, configuration);
      return getCache(name);
   }

   /**
    * Retrieves the default cache associated with this cache manager. Note that the default cache does not need to be
    * explicitly created with {@link #createCache(String)} (String)} since it is automatically created lazily when first
    * used.
        * As such, this method is always guaranteed to return the default cache.
    *
    * @return the default cache.
    */
   @Override
   public <K, V> Cache<K, V> getCache() {
      if (defaultCacheName == null) {
         throw CONFIG.noDefaultCache();
      }
      return internalGetCache(defaultCacheName);
   }

   /**
    * Retrieves a named cache from the system. If the cache has been previously created with the same name, the running
    * cache instance is returned. Otherwise, this method attempts to create the cache first.
        * When creating a new cache, this method will use the configuration passed in to the CacheManager on construction,
    * as a template, and then optionally apply any overrides previously defined for the named cache using the
    * {@link #defineConfiguration(String, Configuration)} or {@link #defineConfiguration(String, String, Configuration)}
    * methods, or declared in the configuration file.
    *
    * @param cacheName name of cache to retrieve
    * @return a cache instance identified by cacheName
    */
   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      return internalGetCache(cacheName);
   }

   private <K, V> Cache<K, V> internalGetCache(String cacheName) {
      if (cacheName == null)
         throw new NullPointerException("Null arguments not allowed");

      assertIsNotTerminated();
      if (!globalComponentRegistry.getStatus().allowInvocations()) {
         throw new IllegalLifecycleStateException("Cache cannot be retrieved while global registry is not running!!");
      }

      String actualName = configurationManager.selectCache(cacheName);
      if (getCacheBlockingCheck != null) {
         if (actualName.equals(getCacheBlockingCheck.get())) {
            // isRunning() was called before getCache(), all good
            getCacheBlockingCheck.set(null);
         } else {
            // isRunning() was not called, let BlockHound know getCache() is potentially blocking
            BlockHoundUtil.pretendBlock();
         }
      }

      // No need to block if another thread (or even the current thread) is starting the global components
      // Because each cache component will wait for the global components it depends on
      // and ComponentRegistry depends on GlobalComponentRegistry.ModuleInitializer
      internalStart(false);

      CompletableFuture<Cache<?, ?>> cacheFuture = caches.get(actualName);
      if (cacheFuture != null) {
         try {
            return (Cache<K, V>) cacheFuture.join();
         } catch (CompletionException e) {
            caches.computeIfPresent(actualName, (k, v) -> {
               if (v == cacheFuture) {
                  log.failedToInitializeCache(actualName, e);
                  return null;
               }
               return v;
            });
            // We were interrupted don't attempt to restart the cache
            if (Util.getRootCause(e) instanceof InterruptedException) {
               return null;
            }
         }
      }
      AdvancedCache<K, V> cache = (AdvancedCache<K, V>) createCache(actualName);
      return actualName.equals(cacheName) ? cache : new AliasCache<>(cache, cacheName);
   }

   @Override
   public boolean cacheExists(String cacheName) {
      return caches.containsKey(cacheName);
   }

   @Override
   public boolean cacheConfigurationExists(String name) {
      return configurationManager.getDefinedConfigurations().contains(name) || configurationManager.getAliases().contains(name);
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName, boolean createIfAbsent) {
      boolean cacheExists = cacheExists(cacheName);
      if (!cacheExists && !createIfAbsent)
         return null;
      else {
         return internalGetCache(cacheName);
      }
   }

   @Override
   public EmbeddedCacheManager startCaches(final String... cacheNames) {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.LIFECYCLE);

      internalStart(false);

      Map<String, Thread> threads = new HashMap<>(cacheNames.length);
      final AtomicReference<RuntimeException> exception = new AtomicReference<>(null);
      for (final String cacheName : cacheNames) {
         if (!threads.containsKey(cacheName)) {
            String threadName = "CacheStartThread," + identifierString() + "," + cacheName;
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

   @ManagedAttribute(description = "The logical address of the cluster's coordinator", displayName = "Coordinator address")
   public String getCoordinatorAddress() {
      return cacheManagerInfo.getCoordinatorAddress();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @ManagedAttribute(description = "Indicates whether this node is coordinator", displayName = "Is coordinator?")
   public boolean isCoordinator() {
      return cacheManagerInfo.isCoordinator();
   }

   private <K, V> Cache<K, V> createCache(String cacheName) {
      final boolean trace = log.isTraceEnabled();
      LogFactory.pushNDC(cacheName, trace);
      try {
         return wireAndStartCache(cacheName);
      } finally {
         LogFactory.popNDC(trace);
      }
   }

   /**
    * @return a null return value means the cache was created by someone else before we got the lock
    */
   private <K, V> Cache<K, V> wireAndStartCache(String cacheName) {
      Configuration c = configurationManager.getConfiguration(cacheName);
      if (c == null) {
         throw CONFIG.noSuchCacheConfiguration(cacheName);
      }

      if (c.security().authorization().enabled()) {
         // Don't even attempt to wire anything if we don't have LIFECYCLE privileges
         authorizer.checkPermission(c.security().authorization(), getSubject(), AuthorizationPermission.LIFECYCLE, null);
      }
      if (c.isTemplate()) {
         throw CONFIG.templateConfigurationStartAttempt(cacheName);
      }

      CompletableFuture<Cache<?, ?>> cacheFuture = new CompletableFuture<>();
      CompletableFuture<Cache<?, ?>> oldFuture = caches.computeIfAbsent(cacheName, name -> {
         assertIsNotTerminated();
         return cacheFuture;
      });

      Cache<K, V> cache = null;
      try {
         if (oldFuture != cacheFuture) {
            cache = (Cache<K, V>) oldFuture.join();
            if (!cache.getStatus().isTerminated()) {
               return cache;
            }
         }
      } catch (CompletionException ce) {
         throw ((CacheException) ce.getCause());
      }

      try {
         log.debugf("Creating cache %s on %s", cacheName, identifierString());
         if (cache == null) {
            cache = new InternalCacheFactory<K, V>().createCache(c, globalComponentRegistry, cacheName);
            if (cache.getAdvancedCache().getAuthorizationManager() != null) {
               cache = new SecureCacheImpl<>(cache.getAdvancedCache());
            }
         }
         ComponentRegistry cr = ComponentRegistry.of(SecurityActions.getUnwrappedCache(cache));

         boolean notStartedYet =
               cr.getStatus() != ComponentStatus.RUNNING && cr.getStatus() != ComponentStatus.INITIALIZING;
         // start the cache-level components
         cache.start();
         cacheFuture.complete(cache);
         boolean needToNotifyCacheStarted = notStartedYet && cr.getStatus() == ComponentStatus.RUNNING;
         if (needToNotifyCacheStarted) {
            globalComponentRegistry.notifyCacheStarted(cacheName);
         }
         log.tracef("Cache %s is ready", cacheName);
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
      authorizer.checkPermission(getSubject(), AuthorizationPermission.LIFECYCLE);
      internalStart(true);
   }

   /**
    * @param block {@code true} when we need all the global components to be running.
    */
   private void internalStart(boolean block) {
      if (status == ComponentStatus.RUNNING)
         return;

      final GlobalConfiguration globalConfiguration = configurationManager.getGlobalConfiguration();
      lifecycleLock.lock();
      try {
         while (block && status == ComponentStatus.INITIALIZING) {
            lifecycleCondition.await();
         }
         if (status != ComponentStatus.INSTANTIATED) {
            return;
         }

         log.debugf("Starting cache manager %s", identifierString());
         initializeSecurity(globalConfiguration);

         updateStatus(ComponentStatus.INITIALIZING);
      } catch (InterruptedException e) {
         throw new CacheException("Interrupted waiting for the cache manager to start");
      } finally {
         lifecycleLock.unlock();
      }

      try {
         globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).start();
         globalComponentRegistry.start();
         // Some caches are started during postStart and we have to extract it out
         globalComponentRegistry.postStart();
         // We could get a stop concurrently, in which case we only want to update to RUNNING if not
         ComponentStatus prev = updateStatusIfPrevious(ComponentStatus.INITIALIZING, ComponentStatus.RUNNING);
         if (prev != null) {
            log.debugf("Cache status changed to %s whiled starting %s", prev, identifierString());
            return;
         }
         log.debugf("Started cache manager %s", identifierString());
      } catch (Exception e) {
         log.failedToInitializeGlobalRegistry(e);

         boolean performShutdown = false;
         lifecycleLock.lock();
         try {
            // First wait if another is stopping us.. if they are we have to wait until they are done with the stop
            while (status == ComponentStatus.STOPPING) {
               lifecycleCondition.await();
            }
            // It is possible another concurrent stop happened which killed our start, so we stop only if that
            // wasn't taking place
            if (status != ComponentStatus.FAILED && status != ComponentStatus.TERMINATED) {
               performShutdown = true;
               status = ComponentStatus.STOPPING;
               lifecycleCondition.signalAll();
            }
         } catch (InterruptedException ie) {
            throw new CacheException("Interrupted waiting for the cache manager to stop");
         } finally {
            lifecycleLock.unlock();
         }

         if (performShutdown) {
            log.tracef("Stopping all caches first before global component registry");
            stopCaches();
            try {
               globalComponentRegistry.componentFailed(e);
            } catch (Exception e1) {
               // Certain tests require this exception
               throw new EmbeddedCacheManagerStartupException(e1);
            } finally {
               updateStatus(ComponentStatus.FAILED);
            }
            throw new EmbeddedCacheManagerStartupException(e);
         }
      }
   }

   private void initializeSecurity(GlobalConfiguration globalConfiguration) {
      GlobalAuthorizationConfiguration authorizationConfig = globalConfiguration.security().authorization();
      if (authorizationConfig.enabled()) {
         AuthorizationMapperContextImpl context = new AuthorizationMapperContextImpl(this);
         authorizationConfig.principalRoleMapper().setContext(context);
         authorizationConfig.rolePermissionMapper().setContext(context);
      }
   }

   private void updateStatus(ComponentStatus status) {
      lifecycleLock.lock();
      try {
         this.status = status;
         lifecycleCondition.signalAll();
      } finally {
         lifecycleLock.unlock();
      }
   }

   /**
    * Updates the status to the new status only if the previous status is equal. Returns null if updated otherwise
    * returns the previous status that didn't match.
    */
   private ComponentStatus updateStatusIfPrevious(ComponentStatus prev, ComponentStatus status) {
      lifecycleLock.lock();
      try {
         if (this.status == prev) {
            this.status = status;
            lifecycleCondition.signalAll();
            return null;
         }
         return this.status;
      } finally {
         lifecycleLock.unlock();
      }
   }

   private void terminate(String cacheName) {
      CompletableFuture<Cache<?, ?>> cacheFuture = this.caches.get(cacheName);
      if (cacheFuture != null) {
         if (!cacheFuture.isDone()) {
            ComponentRegistry cr = globalComponentRegistry.getNamedComponentRegistry(cacheName);
            if (cr != null) cr.stop();
            cacheFuture.completeExceptionally(log.cacheManagerIsStopping());
            return;
         }
         Cache<?, ?> cache = cacheFuture.join();
         if (cache.getStatus().isTerminated()) {
            log.tracef("Ignoring cache %s, it is already terminated.", cacheName);
            return;
         }
         cache.stop();
      }
   }

   /*
    * Shutdown cluster-wide resources of the CacheManager, calling {@link Cache#shutdown()} on both user and internal caches
    * to ensure that they are safely terminated.
    */
   public void shutdownAllCaches() {
      log.tracef("Attempting to shutdown cache manager: " + getAddress());
      authorizer.checkPermission(getSubject(), AuthorizationPermission.LIFECYCLE);
      Set<String> cachesToShutdown = new LinkedHashSet<>(this.caches.size());
      // stop ordered caches first
      try {
         List<String> ordered = cacheDependencyGraph.topologicalSort();
         cachesToShutdown.addAll(ordered);
      } catch (CyclicDependencyException e) {
         CONTAINER.stopOrderIgnored();
      }
      // The caches map includes the default cache
      cachesToShutdown.addAll(caches.keySet());

      log.tracef("Cache shutdown order: %s", cachesToShutdown);
      for (String cacheName : cachesToShutdown) {
         try {
            CompletableFuture<Cache<?, ?>> cacheFuture = this.caches.get(cacheName);
            if (cacheFuture != null) {
               Cache<?, ?> cache = cacheFuture.join();
               if (cache.getStatus().isTerminated()) {
                  log.tracef("Ignoring cache %s, it is already terminated.", cacheName);
                  continue;
               }
               cache.shutdown();
            }
         } catch (Throwable t) {
            CONTAINER.componentFailedToStop(t);
         }
      }
   }

   @Override
   public void stop() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.LIFECYCLE);

      internalStop();
   }

   @Override
   public void stopCache(String cacheName) {
      terminate(cacheName);
   }

   private void internalStop() {
      lifecycleLock.lock();
      String identifierString = identifierString();
      try {
         while (status == ComponentStatus.STOPPING) {
            lifecycleCondition.await();
         }
         if (!status.stopAllowed()) {
            log.trace("Ignore call to stop as the cache manager is not running");
            return;
         }

         // We can stop the manager
         log.debugf("Stopping cache manager %s", identifierString);
         updateStatus(ComponentStatus.STOPPING);
      } catch (InterruptedException e) {
         throw new CacheException("Interrupted waiting for the cache manager to stop");
      } finally {
         lifecycleLock.unlock();
      }

      try {
         log.debugf("Starting shutdown of caches at %s", identifierString);
         stopCaches();
      } catch (Throwable t) {
         log.errorf(t, "Exception during shutdown of caches. Proceeding...");
      }

      try {
         log.debugf("Starting JMX shutdown at %s", identifierString);
         globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).stop();
      } catch (Throwable t) {
         log.errorf(t, "Exception during JMX shutdown. Proceeding...");
      }

      try {
         globalComponentRegistry.stop();
         log.debugf("Stopped cache manager %s", identifierString);
      } finally {
         updateStatus(ComponentStatus.TERMINATED);
      }
   }

   private void stopCaches() {
      Set<String> cachesToStop = new LinkedHashSet<>(this.caches.size());
      // stop ordered caches first
      try {
         List<String> ordered = cacheDependencyGraph.topologicalSort();
         cachesToStop.addAll(ordered);
      } catch (CyclicDependencyException e) {
         CONTAINER.stopOrderIgnored();
      }
      // The caches map includes the default cache
      cachesToStop.addAll(caches.keySet());
      log.tracef("Cache stop order: %s", cachesToStop);

      for (String cacheName : cachesToStop) {
         try {
            stopCache(cacheName);
         } catch (Throwable t) {
            CONTAINER.componentFailedToStop(t);
         }
      }
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.LISTEN);
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      return notifier.addListenerAsync(listener);
   }

   @Override
   public CompletionStage<Void> removeListenerAsync(Object listener) {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.LISTEN);
      try {
         CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
         return notifier.removeListenerAsync(listener);
      } catch (IllegalLifecycleStateException e) {
         // Ignore the exception for backwards compatibility
         return CompletableFutures.completedNull();
      }
   }

   @Override
   public ComponentStatus getStatus() {
      return status;
   }

   @Override
   public GlobalConfiguration getCacheManagerConfiguration() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      return configurationManager.getGlobalConfiguration();
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getDefaultCacheConfiguration() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      if (defaultCacheName != null) {
         return configurationManager.getConfiguration(defaultCacheName, true);
      } else {
         return null;
      }
   }

   @Override
   public Configuration getCacheConfiguration(String name) {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      Configuration configuration = configurationManager.getConfiguration(name, true);
      if (configuration == null && cacheExists(name)) {
         return getDefaultCacheConfiguration();
      }
      return configuration;
   }

   @Override
   public Set<String> getCacheNames() {
      // Get the XML/programmatically defined caches
      Set<String> names = new TreeSet<>(configurationManager.getDefinedCaches());
      // Add the caches created dynamically without explicit config
      names.addAll(caches.keySet());
      InternalCacheRegistry internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.filterPrivateCaches(names);
      if (names.isEmpty())
         return Collections.emptySet();
      else
         return Immutables.immutableSetWrap(names);
   }

   @Override
   public Set<String> getAccessibleCacheNames() {
      if (configurationManager.getGlobalConfiguration().security().authorization().enabled()) {
         Set<String> names = new TreeSet<>();
         GlobalSecurityManager gsm = globalComponentRegistry.getComponent(GlobalSecurityManager.class);
         for (String name : configurationManager.getDefinedCaches()) {
            Configuration cfg = configurationManager.getConfiguration(name);
            AuthorizationManagerImpl am = new AuthorizationManagerImpl();
            am.init(name, configurationManager.getGlobalConfiguration(), cfg, gsm);
            if (!am.getPermissions(Security.getSubject()).isEmpty()) {
               names.add(name);
            }
         }
         InternalCacheRegistry internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
         internalCacheRegistry.filterPrivateCaches(names);
         return names;
      } else {
         return getCacheNames();
      }
   }

   @Override
   public Set<String> getCacheConfigurationNames() {
      return cacheManagerInfo.getCacheConfigurationNames();
   }

   @Override
   public boolean isRunning(String cacheName) {
      cacheName = configurationManager.selectCache(cacheName);
      if (getCacheBlockingCheck != null) {
         getCacheBlockingCheck.set(cacheName);
      }
      CompletableFuture<Cache<?, ?>> cacheFuture = caches.get(cacheName);
      boolean started = cacheFuture != null && cacheFuture.isDone() && !cacheFuture.isCompletedExceptionally();
      return started && cacheFuture.join().getStatus() == ComponentStatus.RUNNING;
   }

   @Override
   public boolean isDefaultRunning() {
      Optional<String> defaultCacheName = configurationManager.getGlobalConfiguration().defaultCacheName();
      return defaultCacheName.isPresent() && isRunning(defaultCacheName.get());
   }

   @ManagedAttribute(description = "The status of the cache manager instance.", displayName = "Cache manager status", dataType = DataType.TRAIT)
   public String getCacheManagerStatus() {
      return cacheManagerInfo.getCacheManagerStatus();
   }

   @ManagedAttribute(description = "The defined cache names and their statuses.  The default cache is not included in this representation.", displayName = "List of defined caches", dataType = DataType.TRAIT)
   public String getDefinedCacheNames() {
      StringJoiner stringJoiner = new StringJoiner("", "[", "]");
      cacheManagerInfo.getDefinedCaches().forEach(c -> stringJoiner.add(c.name).add(c.isStarted() ? "(created)" : "(not created)"));
      return stringJoiner.toString();
   }

   @ManagedAttribute(description = "The defined cache configuration names.", displayName = "List of defined cache configurations", dataType = DataType.TRAIT)
   public String getDefinedCacheConfigurationNames() {
      StringJoiner stringJoiner = new StringJoiner(",", "[", "]");
      cacheManagerInfo.getCacheConfigurationNames().forEach(stringJoiner::add);
      return stringJoiner.toString();
   }

   @ManagedAttribute(description = "The total number of defined cache configurations.", displayName = "Number of caches defined")
   public String getDefinedCacheCount() {
      return String.valueOf(getNumberOfCacheConfigurations());
   }

   @ManagedAttribute(description = "The total number of defined cache configurations.", displayName = "Number of caches defined")
   public int getNumberOfCacheConfigurations() {
      return getCacheConfigurationNames().size();
   }

   @ManagedAttribute(description = "The total number of created caches, including the default cache.", displayName = "Number of caches created")
   public String getCreatedCacheCount() {
      return String.valueOf(getNumberOfCreatedCaches());
   }

   @ManagedAttribute(description = "The total number of created caches, including the default cache.", displayName = "Number of caches created")
   public long getNumberOfCreatedCaches() {
      return cacheManagerInfo.getCreatedCacheCount();
   }

   @ManagedAttribute(description = "The total number of running caches, including the default cache.", displayName = "Number of running caches")
   public String getRunningCacheCount() {
      return String.valueOf(getNumberOfRunningCaches());
   }

   @ManagedAttribute(description = "The total number of running caches, including the default cache.", displayName = "Number of running caches")
   public long getNumberOfRunningCaches() {
      return cacheManagerInfo.getRunningCacheCount();
   }

   @ManagedAttribute(description = "Returns the version of Infinispan", displayName = "Infinispan version", dataType = DataType.TRAIT)
   public String getVersion() {
      return cacheManagerInfo.getVersion();
   }

   @ManagedAttribute(description = "The name of this cache manager", displayName = "Cache manager name", dataType = DataType.TRAIT)
   public String getName() {
      return cacheManagerInfo.getName();
   }

   @ManagedOperation(description = "Starts the default cache associated with this cache manager", displayName = "Starts the default cache")
   public void startCache() {
      if (defaultCacheName == null) {
         throw CONFIG.noDefaultCache();
      }
      startCache(defaultCacheName);
   }

   @ManagedOperation(description = "Starts a named cache from this cache manager", name = "startCache", displayName = "Starts a cache with the given name")
   public void startCache(@Parameter(name = "cacheName", description = "Name of cache to start") String cacheName) {
      if (cacheName == null)
         throw new NullPointerException("Null arguments not allowed");

      assertIsNotTerminated();
      // No need to block if another thread (or even the current thread) is starting the global components
      // Because each cache component will wait for the global components it depends on
      // And and ComponentRegistry depends on GlobalComponentRegistry.ModuleInitializer
      internalStart(false);

      CompletableFuture<Cache<?, ?>> cacheFuture = caches.get(cacheName);
      if (cacheFuture != null) {
         try {
            Cache<?, ?> cache = cacheFuture.join();
            if (!cache.getStatus().isTerminated()) {
               return;
            }
         } catch (CompletionException e) {
            throw ((CacheException) e.getCause());
         }
      }

      createCache(cacheName);
   }

   @ManagedAttribute(description = "The network address associated with this instance", displayName = "Network address", dataType = DataType.TRAIT)
   public String getNodeAddress() {
      return cacheManagerInfo.getNodeAddress();
   }

   @ManagedAttribute(description = "The physical network addresses associated with this instance", displayName = "Physical network addresses", dataType = DataType.TRAIT)
   public String getPhysicalAddresses() {
      return cacheManagerInfo.getPhysicalAddresses();
   }

   @ManagedAttribute(description = "List of members in the cluster", displayName = "Cluster members", dataType = DataType.TRAIT)
   public String getClusterMembers() {
      List<String> clusterMembers = cacheManagerInfo.getClusterMembers();
      return clusterMembers.size() == 1 ? clusterMembers.iterator().next() : clusterMembers.toString();
   }

   @ManagedAttribute(description = "List of members in the cluster", displayName = "Cluster members", dataType = DataType.TRAIT)
   public String getClusterMembersPhysicalAddresses() {
      return cacheManagerInfo.getClusterMembersPhysicalAddresses().toString();
   }

   @ManagedAttribute(description = "Size of the cluster in number of nodes", displayName = "Cluster size")
   public int getClusterSize() {
      return cacheManagerInfo.getClusterSize();
   }

   /**
    * {@inheritDoc}
    */
   @ManagedAttribute(description = "Cluster name", displayName = "Cluster name", dataType = DataType.TRAIT)
   @Override
   public String getClusterName() {
      return cacheManagerInfo.getClusterName();
   }

   @ManagedAttribute(description = "Returns the local site name", displayName = "Local site name", dataType = DataType.TRAIT)
   public String getSite() {
      return cacheManagerInfo.getLocalSite();
   }

   @ManagedAttribute(description = "Lists all online sites", displayName = "Online Sites", dataType = DataType.TRAIT)
   public String getSiteView() {
      return String.valueOf(cacheManagerInfo.getSites());
   }

   @ManagedAttribute(description = "Indicates whether this node is a relay node", displayName = "Is relay node?", dataType = DataType.TRAIT)
   public boolean isRelayNode() {
      return cacheManagerInfo.isRelayNode();
   }

   @ManagedAttribute(description = "Lists relay nodes in the local site", displayName = "Relay nodes", dataType = DataType.TRAIT)
   public String getRelayNodesAddress() {
      return String.valueOf(cacheManagerInfo.getRelayNodesAddress());
   }

   String getLogicalAddressString() {
      return getAddress() == null ? "local" : getAddress().toString();
   }

   private void assertIsNotTerminated() {
      if (status == ComponentStatus.STOPPING ||
            status == ComponentStatus.TERMINATED ||
            status == ComponentStatus.FAILED)
         throw new IllegalLifecycleStateException(
               "Cache container has been stopped and cannot be reused. Recreate the cache container.");
   }

   private Transport getTransport() {
      if (transport == null) {
         lifecycleLock.lock();
         try {
            // Do not start the transport if the manager hasn't been started yet or we are already stopping
            if (transport == null && (status == ComponentStatus.RUNNING || status == ComponentStatus.INITIALIZING)) {
               transport = globalComponentRegistry.getComponent(Transport.class);
            }
         } finally {
            lifecycleLock.unlock();
         }
      }
      return transport;
   }

   @Override
   public void addCacheDependency(String from, String to) {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      cacheDependencyGraph.addDependency(from, to);
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " " + identifierString();
   }

   private String identifierString() {
      if (getAddress() != null) {
         return getAddress().toString();
      } else if (configurationManager.getGlobalConfiguration().transport().nodeName() != null) {
         return configurationManager.getGlobalConfiguration().transport().nodeName();
      } else {
         return configurationManager.getGlobalConfiguration().cacheManagerName();
      }
   }

   @ManagedAttribute(description = "Global configuration properties", displayName = "Global configuration properties", dataType = DataType.TRAIT)
   public Properties getGlobalConfigurationAsProperties() {
      return new PropertyFormatter().format(configurationManager.getGlobalConfiguration());
   }

   @Override
   public CacheContainerStats getStats() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.MONITOR);
      return stats;
   }

   @Override
   public Health getHealth() {
      return health;
   }

   @Override
   public CacheManagerInfo getCacheManagerInfo() {
      return cacheManagerInfo;
   }

   @Override
   public ClusterExecutor executor() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.EXEC);
      // Allow INITIALIZING state so ClusterExecutor can be used by components in a @Start method.
      if (globalComponentRegistry.getStatus() != ComponentStatus.RUNNING &&
            globalComponentRegistry.getStatus() != ComponentStatus.INITIALIZING) {
         throw new IllegalStateException("CacheManager must be started before retrieving a ClusterExecutor!");
      }
      // TODO: This is to be removed in https://issues.redhat.com/browse/ISPN-11482
      BlockingManager blockingManager = globalComponentRegistry.getComponent(BlockingManager.class);
      // Have to make sure the transport is running before we retrieve it
      Transport transport = globalComponentRegistry.getComponent(BasicComponentRegistry.class).getComponent(Transport.class).running();
      if (transport != null) {
         long time = configurationManager.getGlobalConfiguration().transport().distributedSyncTimeout();
         return ClusterExecutors.allSubmissionExecutor(null, this, transport, time, TimeUnit.MILLISECONDS,
               // This can run arbitrary code, including user - such commands can block
               blockingManager,
               globalComponentRegistry.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR));
      } else {
         return ClusterExecutors.allSubmissionExecutor(null, this, null,
               TransportConfiguration.DISTRIBUTED_SYNC_TIMEOUT.getDefaultValue().longValue(), TimeUnit.MILLISECONDS,
               blockingManager,
               globalComponentRegistry.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR));
      }
   }

   @Override
   public void close() throws IOException {
      stop();
   }

   @Override
   public ClassAllowList getClassAllowList() {
      return classAllowList;
   }

   @Override
   public EmbeddedCacheManagerAdmin administration() {
      return cacheManagerAdmin;
   }

   ConcurrentMap<String, CompletableFuture<Cache<?, ?>>> getCaches() {
      return caches;
   }

   ConfigurationManager getConfigurationManager() {
      return configurationManager;
   }

   @Override
   public Subject getSubject() {
      return null;
   }

   @Override
   public EmbeddedCacheManager withSubject(Subject subject) {
      if (subject == null) {
         return this;
      } else {
         return new DefaultCacheManager(this) {
            @Override
            public EmbeddedCacheManager withSubject(Subject subject) {
               throw new IllegalArgumentException("Cannot set a Subject on an EmbeddedCacheManager more than once");
            }

            @Override
            public Subject getSubject() {
               return subject;
            }
         };
      }
   }

   protected GlobalComponentRegistry globalComponentRegistry() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      return globalComponentRegistry;
   }

   static void enableGetCacheBlockingCheck() {
      getCacheBlockingCheck = new ThreadLocal<>();
   }
}
