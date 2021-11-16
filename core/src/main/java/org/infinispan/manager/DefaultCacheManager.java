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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.internal.BlockHoundUtil;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.commons.util.Immutables;
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
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.AuthorizationMapperContextImpl;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.security.impl.SecureCacheImpl;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.stats.impl.CacheContainerStatsImpl;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.CyclicDependencyException;
import org.infinispan.util.DependencyGraph;
import org.infinispan.util.concurrent.CompletableFutures;
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
public class DefaultCacheManager implements EmbeddedCacheManager {
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
    * @deprecated Since 11.0, please use {@link #DefaultCacheManager(ConfigurationBuilderHolder, boolean)} instead.
    */
   @Deprecated
   public DefaultCacheManager(Configuration defaultConfiguration) {
      this(null, defaultConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the default configuration passed in.  See
    * {@link org.infinispan.configuration.global.GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration file to use as a template for all caches created
    * @param start                if true, the cache manager is started
    * @deprecated Since 11.0, please use {@link #DefaultCacheManager(ConfigurationBuilderHolder, boolean)} instead.
    */
   @Deprecated
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
    * @deprecated Since 11.0, please use {@link #DefaultCacheManager(ConfigurationBuilderHolder, boolean)} instead.
    */
   @Deprecated
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
    * @deprecated Since 11.0, please use {@link #DefaultCacheManager(ConfigurationBuilderHolder, boolean)} instead.
    */
   @Deprecated
   public DefaultCacheManager(GlobalConfiguration globalConfiguration, Configuration defaultConfiguration,
                              boolean start) {
      globalConfiguration = globalConfiguration == null ? new GlobalConfigurationBuilder().build() : globalConfiguration;
      this.configurationManager = new ConfigurationManager(globalConfiguration);
      if (defaultConfiguration != null) {
         if (globalConfiguration.defaultCacheName().isPresent()) {
            defaultCacheName = globalConfiguration.defaultCacheName().get();
         } else {
            throw CONFIG.defaultCacheConfigurationWithoutName();
         }
         configurationManager.putConfiguration(defaultCacheName, defaultConfiguration);
      } else {
         if (globalConfiguration.defaultCacheName().isPresent()) {
            throw CONFIG.missingDefaultCacheDeclaration(globalConfiguration.defaultCacheName().get());
         } else {
            defaultCacheName = null;
         }
      }
      ModuleRepository moduleRepository = ModuleRepository.newModuleRepository(globalConfiguration.classLoader(), globalConfiguration);
      this.classAllowList = globalConfiguration.serialization().allowList().create();
      this.globalComponentRegistry = new GlobalComponentRegistry(globalConfiguration, this, caches.keySet(),
                                                                 moduleRepository, configurationManager);

      InternalCacheRegistry internalCacheRegistry = globalComponentRegistry.getComponent(InternalCacheRegistry.class);
      this.globalComponentRegistry.registerComponent(cacheDependencyGraph, CACHE_DEPENDENCY_GRAPH, false);

      this.authorizer = new Authorizer(globalConfiguration.security(), AuditContext.CACHEMANAGER, globalConfiguration.cacheManagerName(), null);
      this.globalComponentRegistry.registerComponent(authorizer, Authorizer.class);

      this.stats = new CacheContainerStatsImpl(this);
      globalComponentRegistry.registerComponent(stats, CacheContainerStats.class);

      health = new HealthImpl(this, globalComponentRegistry.getComponent(InternalCacheRegistry.class));
      cacheManagerInfo = new CacheManagerInfo(this, configurationManager, internalCacheRegistry, globalComponentRegistry.getComponent(
            LocalTopologyManager.class));
      globalComponentRegistry.registerComponent(new HealthJMXExposerImpl(health), HealthJMXExposer.class);

      this.cacheManagerAdmin = new DefaultCacheManagerAdmin(this, authorizer, EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class), null,
                                                            globalComponentRegistry.getComponent(GlobalConfigurationManager.class));
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
      this(new ParserRegistry().parse(configurationStream, null, MediaType.APPLICATION_XML), start);
   }

   /**
    * Constructs a new instance of the CacheManager, using the input stream passed in to read configuration file
    * contents.
    *
    * @param configurationURL    stream containing configuration file contents, to use as a template for all caches
    *                            created
    * @param start               if true, the cache manager is started
    * @throws java.io.IOException if there is a problem reading the configuration stream
    */
   public DefaultCacheManager(URL configurationURL, boolean start) throws IOException {
      this(new ParserRegistry().parse(configurationURL), start);
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
      this.configurationManager = original.configurationManager;
      this.health = original.health;
      this.classAllowList = original.classAllowList;
      this.cacheManagerInfo = original.cacheManagerInfo;
      this.cacheManagerAdmin = original.cacheManagerAdmin;
      this.defaultCacheName = original.defaultCacheName;
      this.stats = original.stats;
      this.globalComponentRegistry = original.globalComponentRegistry;
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

      Configuration existing = configurationManager.getConfiguration(name, false);
      if (existing != null) {
         throw CONFIG.configAlreadyDefined(name);
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
    * explicitly created with {@link #createCache(String)} (String)} since it is automatically created lazily
    * when first used.
    * <p/>
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
      return internalGetCache(cacheName);
   }

   private <K, V> Cache<K, V> internalGetCache(String cacheName) {
      if (cacheName == null)
         throw new NullPointerException("Null arguments not allowed");

      assertIsNotTerminated();
      if (getCacheBlockingCheck != null) {
         if (cacheName.equals(getCacheBlockingCheck.get())) {
            // isRunning() was called before getCache(), all good
            getCacheBlockingCheck.set(null);
         } else {
            // isRunning() was not called, let BlockHound know getCache() is potentially blocking
            BlockHoundUtil.pretendBlock();
         }
      }

      // No need to block if another thread (or even the current thread) is starting the global components
      // Because each cache component will wait for the global components it depends on
      // And and ComponentRegistry depends on GlobalComponentRegistry.ModuleInitializer
      internalStart(false);

      CompletableFuture<Cache<?, ?>> cacheFuture = caches.get(cacheName);
      if (cacheFuture != null) {
         try {
            return (Cache<K, V>) cacheFuture.join();
         } catch (CompletionException e) {
            throw ((CacheException) e.getCause());
         }
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

   @Override
   public void removeCache(String cacheName) {
      cacheManagerAdmin.removeCache(cacheName);
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
         ComponentRegistry cr = SecurityActions.getUnwrappedCache(cache).getAdvancedCache().getComponentRegistry();

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

         log.debugf("Started cache manager %s", identifierString());
      } catch (Exception e) {
         throw new EmbeddedCacheManagerStartupException(e);
      } finally {
         updateStatus(globalComponentRegistry.getStatus());
      }
   }

   private void initializeSecurity(GlobalConfiguration globalConfiguration) {
      GlobalAuthorizationConfiguration authorizationConfig = globalConfiguration.security().authorization();
      if (authorizationConfig.enabled() && System.getSecurityManager() == null) {
         CONFIG.authorizationEnabledWithoutSecurityManager();
      }
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

   private void terminate(String cacheName) {
      CompletableFuture<Cache<?, ?>> cacheFuture = this.caches.get(cacheName);
      if (cacheFuture != null) {
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

   private void internalStop() {
      lifecycleLock.lock();
      String identifierString = identifierString();
      try {
         while (status == ComponentStatus.STOPPING) {
            lifecycleCondition.await();
         }
         if (status != ComponentStatus.RUNNING && status != ComponentStatus.FAILED) {
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
         stopCaches();
         globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).stop();
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
            terminate(cacheName);
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

   @Deprecated
   @Override
   public Set<Object> getListeners() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.LISTEN);
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      return notifier.getListeners();
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
      Set<String> names = new HashSet<>(configurationManager.getDefinedCaches());
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
   public Set<String> getCacheConfigurationNames() {
      return cacheManagerInfo.getCacheConfigurationNames();
   }

   @Override
   public boolean isRunning(String cacheName) {
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
      if (cacheName == null )
         throw new NullPointerException("Null arguments not allowed");

      assertIsNotTerminated();
      // No need to block if another thread (or even the current thread) is starting the global components
      // Because each cache component will wait for the global components it depends on
      // And and ComponentRegistry depends on GlobalComponentRegistry.ModuleInitializer
      internalStart(false);

      CompletableFuture<Cache<?, ?>> cacheFuture = caches.get(cacheName);
      if (cacheFuture != null) {
         try {
            Cache<?,?> cache = cacheFuture.join();
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

   @Override
   public Transport getTransport() {
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
   public GlobalComponentRegistry getGlobalComponentRegistry() {
      authorizer.checkPermission(getSubject(), AuthorizationPermission.ADMIN);
      return globalComponentRegistry;
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
      } else if (configurationManager.getGlobalConfiguration().transport().nodeName() != null){
         return configurationManager.getGlobalConfiguration().transport().nodeName();
      } else {
         return configurationManager.getGlobalConfiguration().cacheManagerName();
      }
   }

   /**
    * {@inheritDoc}
    */
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
      Executor blockingExecutor = globalComponentRegistry.getComponent(ExecutorService.class, KnownComponentNames.BLOCKING_EXECUTOR);
      // Have to make sure the transport is running before we retrieve it
      Transport transport = globalComponentRegistry.getComponent(BasicComponentRegistry.class).getComponent(Transport.class).running();
      if (transport != null) {
         long time = configurationManager.getGlobalConfiguration().transport().distributedSyncTimeout();
         return ClusterExecutors.allSubmissionExecutor(null, this, transport, time, TimeUnit.MILLISECONDS,
               // This can run arbitrary code, including user - such commands can block
               blockingExecutor,
               globalComponentRegistry.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR));
      } else {
         return ClusterExecutors.allSubmissionExecutor(null, this, null,
               TransportConfiguration.DISTRIBUTED_SYNC_TIMEOUT.getDefaultValue(), TimeUnit.MILLISECONDS,
               blockingExecutor,
               globalComponentRegistry.getComponent(ScheduledExecutorService.class, KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR));
      }
   }

   @Override
   public void close() throws IOException {
      stop();
   }

   @Override
   public ClassAllowList getClassWhiteList() {
      return getClassAllowList();
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

   static void enableGetCacheBlockingCheck() {
      getCacheBlockingCheck = new ThreadLocal<>();
   }
}
