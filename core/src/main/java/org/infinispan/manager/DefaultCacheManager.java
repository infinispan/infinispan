/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.manager;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.Version;
import org.infinispan.commands.RemoveCacheCommand;
import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.LegacyConfigurationAdaptor;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.LegacyGlobalConfigurationAdaptor;
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
import org.infinispan.lifecycle.Lifecycle;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.Immutables;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
 * <code>
 *    CacheManager manager = CacheManager.getInstance("my-config-file.xml");
 *    Cache&lt;String, Person&gt; entityCache = manager.getCache("myEntityCache");
 *    entityCache.put("aPerson", new Person());
 *
 *    ConfigurationBuilder confBuilder = new ConfigurationBuilder();
 *    confBuilder.clustering().cacheMode(CacheMode.REPL_SYNC);
 *    manager.defineConfiguration("myReplicatedCache", confBuilder.build());
 *    Cache&lt;String, String&gt; replicatedCache = manager.getCache("myReplicatedCache");
 * </code>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
@MBean(objectName = DefaultCacheManager.OBJECT_NAME, description = "Component that acts as a manager, factory and container for caches in the system.")
public class DefaultCacheManager implements EmbeddedCacheManager, CacheManager {
   public static final String OBJECT_NAME = "CacheManager";
   private static final Log log = LogFactory.getLog(DefaultCacheManager.class);
   protected final GlobalConfiguration globalConfiguration;
   protected final Configuration defaultConfiguration;
   private final ConcurrentMap<String, CacheWrapper> caches = CollectionFactory.makeConcurrentMap();
   private final ConcurrentMap<String, Configuration> configurationOverrides = CollectionFactory.makeConcurrentMap();
   private final GlobalComponentRegistry globalComponentRegistry;
   private volatile boolean stopping;

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
    * Constructs and starts a new instance of the CacheManager, using the default configuration passed in. Uses defaults
    * for a {@link GlobalConfiguration}.  See {@link GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration to use as a template for all caches created
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.cache.Configuration)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.Configuration defaultConfiguration) {
      this(null, defaultConfiguration, true);
   }

   /**
    * Constructs and starts a new instance of the CacheManager, using the default configuration passed in.  See {@link org.infinispan.configuration.cache.Configuration Configuration}
    * and {@link org.infinispan.configuration.global.GlobalConfiguration GlobalConfiguration} for details of these defaults.
    *
    * @param defaultConfiguration configuration to use as a template for all caches created
    */
   public DefaultCacheManager(org.infinispan.configuration.cache.Configuration defaultConfiguration) {
      this(null, defaultConfiguration, true);
   }

   /**
    * Constructs a new instance of the CacheManager, using the default configuration passed in. Uses defaults for a
    * {@link org.infinispan.config.GlobalConfiguration}.  See {@link GlobalConfiguration} for details of these
    * defaults.
    *
    * @param defaultConfiguration configuration file to use as a template for all caches created
    * @param start                if true, the cache manager is started
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.cache.Configuration, boolean)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.Configuration defaultConfiguration, boolean start) {
      this(null, defaultConfiguration, start);
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
    * defaults for the default named cache configuration.  See {@link Configuration} for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration) {
      this(globalConfiguration, null, true);
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
    * the default named cache configuration.  See {@link Configuration} for details of these defaults.
    *
    * @param globalConfiguration GlobalConfiguration to use for all caches created
    * @param start               if true, the cache manager is started.
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration, boolean)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration, boolean start) {
      this(globalConfiguration, null, start);
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
    * Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration, org.infinispan.configuration.cache.Configuration)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration, org.infinispan.config.Configuration defaultConfiguration) {
      this(globalConfiguration, defaultConfiguration, true);
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
    * @deprecated Use {@link #DefaultCacheManager(org.infinispan.configuration.global.GlobalConfiguration, org.infinispan.configuration.cache.Configuration, boolean)} instead
    */
   @Deprecated
   public DefaultCacheManager(org.infinispan.config.GlobalConfiguration globalConfiguration, org.infinispan.config.Configuration defaultConfiguration,
                              boolean start) {
      this(LegacyGlobalConfigurationAdaptor.adapt(globalConfiguration), LegacyConfigurationAdaptor.adapt(defaultConfiguration), start);
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
      this(new ParserRegistry(Thread.currentThread().getContextClassLoader()).parse(configurationStream), start);
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
         defaultConfiguration = holder.getDefaultConfigurationBuilder().build();

         for (Entry<String, ConfigurationBuilder> entry : holder.getNamedConfigurationBuilders().entrySet()) {
            org.infinispan.configuration.cache.Configuration c = entry.getValue().build();
            configurationOverrides.put(entry.getKey(), c);
         }

         globalComponentRegistry = new GlobalComponentRegistry(globalConfiguration, this, caches.keySet());
      } catch (ConfigurationException ce) {
         throw ce;
      } catch (RuntimeException re) {
         throw new ConfigurationException(re);
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
      ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());

      ConfigurationBuilderHolder globalConfigurationBuilderHolder = parserRegistry.parseFile(globalConfigurationFile);
      ConfigurationBuilderHolder defaultConfigurationBuilderHolder = parserRegistry.parseFile(defaultConfigurationFile);

      globalConfiguration = globalConfigurationBuilderHolder.getGlobalConfigurationBuilder().build();
      defaultConfiguration = defaultConfigurationBuilderHolder.getDefaultConfigurationBuilder().build();

      if (namedCacheFile != null) {
         ConfigurationBuilderHolder namedConfigurationBuilderHolder = parserRegistry.parseFile(namedCacheFile);
         Entry<String, ConfigurationBuilder> entry = namedConfigurationBuilderHolder.getNamedConfigurationBuilders().entrySet().iterator().next();
         configurationOverrides.put(entry.getKey(), entry.getValue().build());
      }

      globalComponentRegistry = new GlobalComponentRegistry(this.globalConfiguration, this, caches.keySet());
      if (start)
         start();
   }


   @Override
   public Configuration defineConfiguration(String cacheName, Configuration configuration) {
      return defineConfiguration(cacheName, configuration, defaultConfiguration, true);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @Deprecated
   public org.infinispan.config.Configuration defineConfiguration(String cacheName, org.infinispan.config.Configuration configurationOverride) {
      return defineConfiguration(cacheName, configurationOverride, LegacyConfigurationAdaptor.adapt(defaultConfiguration), true);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @Deprecated
   public org.infinispan.config.Configuration defineConfiguration(String cacheName, String templateName, org.infinispan.config.Configuration configurationOverride) {
      if (templateName != null) {
         Configuration c = configurationOverrides.get(templateName);
         if (c != null)
            return defineConfiguration(cacheName, configurationOverride, LegacyConfigurationAdaptor.adapt(c), false);
         return defineConfiguration(cacheName, configurationOverride);
      }
      return defineConfiguration(cacheName, configurationOverride);
   }

   @Deprecated
   private org.infinispan.config.Configuration defineConfiguration(String cacheName, org.infinispan.config.Configuration configOverride,
         org.infinispan.config.Configuration defaultConfigIfNotPresent, boolean checkExisting) {
      assertIsNotTerminated();
      if (cacheName == null || configOverride == null)
         throw new NullPointerException("Null arguments not allowed");
      if (cacheName.equals(DEFAULT_CACHE_NAME))
         throw new IllegalArgumentException("Cache name cannot be used as it is a reserved, internal name");
      if (checkExisting) {
         org.infinispan.config.Configuration existing =
               LegacyConfigurationAdaptor.adapt(configurationOverrides.get(cacheName));
         if (existing != null) {
            existing.applyOverrides(configOverride);
            configurationOverrides.put(cacheName, LegacyConfigurationAdaptor.adapt(existing));
            return existing.clone();
         }
      }
      org.infinispan.config.Configuration configuration = defaultConfigIfNotPresent.clone();
      configuration.applyOverrides(configOverride.clone());
      configurationOverrides.put(cacheName, LegacyConfigurationAdaptor.adapt(configuration));
      return configuration;
   }

   public Configuration defineConfiguration(String cacheName, String templateName, Configuration configurationOverride) {
      if (templateName != null) {
         Configuration c = configurationOverrides.get(templateName);
         if (c != null)
            return defineConfiguration(cacheName, configurationOverride, c, false);
         return defineConfiguration(cacheName, configurationOverride);
      }
      return defineConfiguration(cacheName, configurationOverride);
   }

   private Configuration defineConfiguration(String cacheName, Configuration configOverride,
                                             Configuration defaultConfigIfNotPresent, boolean checkExisting) {
      assertIsNotTerminated();
      if (cacheName == null || configOverride == null)
         throw new NullPointerException("Null arguments not allowed");
      if (cacheName.equals(DEFAULT_CACHE_NAME))
         throw new IllegalArgumentException("Cache name cannot be used as it is a reserved, internal name");
      if (checkExisting) {
         Configuration existing = configurationOverrides.get(cacheName);
         if (existing != null) {
            ConfigurationBuilder builder = new ConfigurationBuilder();
            builder.read(existing);
            builder.read(configOverride);
            Configuration configuration = builder.build();
            configurationOverrides.put(cacheName, configuration);
            return configuration;
         }
      }
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.read(defaultConfigIfNotPresent);
      builder.read(configOverride);
      Configuration configuration = builder.build();
      configurationOverrides.put(cacheName, configuration);
      return configuration;
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
      List<Thread> threads = new ArrayList<Thread>(cacheNames.length);
      for (final String cacheName : cacheNames) {

         String threadName = "CacheStartThread," + globalConfiguration.transport().nodeName() + "," + cacheName;
         Thread thread = new Thread(threadName) {
            @Override
            public void run() {
               createCache(cacheName);
            }
         };
         thread.start();
         threads.add(thread);
      }
      try {
         for (Thread thread : threads) {
            thread.join(defaultConfiguration.locking().lockAcquisitionTimeout());
         }
      } catch (InterruptedException e) {
         throw new CacheException("Interrupted while waiting for the caches to start");
      }

      return this;
   }

   @Override
   public void removeCache(String cacheName) {
      ComponentRegistry cacheComponentRegistry = globalComponentRegistry.getNamedComponentRegistry(cacheName);
      if (cacheComponentRegistry != null) {
         RemoveCacheCommand cmd = new RemoveCacheCommand(cacheName, this, globalComponentRegistry,
               cacheComponentRegistry.getComponent(CacheLoaderManager.class));
         Transport transport = getTransport();
         try {
            if (transport != null) {
               Configuration c = getConfiguration(cacheName);
               // Use sync replication timeout
               transport.invokeRemotely(null, cmd, ResponseMode.SYNCHRONOUS, c.clustering().sync().replTimeout(), false, null, false, false);
            }
            // Once sent to the cluster, remove the local cache
            cmd.perform(null);
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

   /**
    * {@inheritDoc}
    */
   @Override
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

      try {
         synchronized (caches) {
            //fetch it again with the lock held
            CacheWrapper existingCacheWrapper = caches.get(cacheName);
            if (existingCacheWrapper != null) {
               return null; //signal that the cache was created by someone else
            }
            createdCacheWrapper = new CacheWrapper();
            if (caches.put(cacheName, createdCacheWrapper) != null) {
               throw new IllegalStateException("attempt to initialize the cache twice");
            }
         }

         globalComponentRegistry.start();
         Configuration c = getConfiguration(cacheName);

         log.tracef("About to wire and start cache %s", cacheName);
         Cache<K, V> cache = new InternalCacheFactory<K, V>().createCache(c, globalComponentRegistry, cacheName);
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
         c = new ConfigurationBuilder().read(defaultConfiguration).build();
      else
         c = configurationOverrides.get(cacheName);
      return c;
   }

   @Override
   public void start() {
      globalComponentRegistry.getComponent(CacheManagerJmxRegistration.class).start();
      String clusterName = globalConfiguration.transport().clusterName();
      String nodeName = globalConfiguration.transport().nodeName();
      log.debugf("Started cache manager %s on %s", clusterName, nodeName);
   }

   @Override
   public void stop() {
      if (!stopping) {
         synchronized (this) {
            // DCL to make sure that only one thread calls stop at one time,
            // and any other calls by other threads are ignored.
            if (!stopping) {
               log.debugf("Stopping cache manager %s on %s", globalConfiguration.transport().clusterName(), getAddress());
               stopping = true;
               // make sure we stop the default cache LAST!
               Cache<?, ?> defaultCache = null;
               for (Map.Entry<String, CacheWrapper> entry : caches.entrySet()) {
                  if (entry.getKey().equals(DEFAULT_CACHE_NAME)) {
                     defaultCache = entry.getValue().cache;
                  } else {
                     Cache<?, ?> c = entry.getValue().cache;
                     if (c != null) {
                        unregisterCacheMBean(c);
                        c.stop();
                     }
                  }
               }

               if (defaultCache != null) {
                  unregisterCacheMBean(defaultCache);
                  defaultCache.stop();
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
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      notifier.addListener(listener);
   }

   @Override
   public void removeListener(Object listener) {
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      notifier.removeListener(listener);
   }

   @Override
   public Set<Object> getListeners() {
      CacheManagerNotifier notifier = globalComponentRegistry.getComponent(CacheManagerNotifier.class);
      return notifier.getListeners();
   }

   @Override
   public ComponentStatus getStatus() {
      return globalComponentRegistry.getStatus();
   }

   @Override
   public org.infinispan.config.GlobalConfiguration getGlobalConfiguration() {
      return LegacyGlobalConfigurationAdaptor.adapt(globalConfiguration);
   }

   @Override
   public GlobalConfiguration getCacheManagerConfiguration() {
      return globalConfiguration;
   }

   @Override
   public org.infinispan.config.Configuration getDefaultConfiguration() {
      return LegacyConfigurationAdaptor.adapt(defaultConfiguration);
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getDefaultCacheConfiguration() {
      return defaultConfiguration;
   }

   @Override
   public Configuration getCacheConfiguration(String name) {
      return configurationOverrides.get(name);
   }

   @Override
   public Set<String> getCacheNames() {
      // Get the XML/programmatically defined caches
      Set<String> names = new HashSet<String>(configurationOverrides.keySet());
      // Add the caches created dynamically without explicit config
      // Since caches could be modified dynamically, make a safe copy of keys
      names.addAll(Immutables.immutableSetConvert(caches.keySet()));
      names.remove(DEFAULT_CACHE_NAME);
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

   @ManagedAttribute(description = "Infinispan version.", displayName = "Infinispan version", displayType = DisplayType.SUMMARY, dataType = DataType.TRAIT)
   public String getVersion() {
      return Version.printVersion();
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
         throw new IllegalStateException("Cache container has been stopped and cannot be reused. Recreate the cache container.");
   }

   @Override
   public Transport getTransport() {
      if (globalComponentRegistry == null) return null;
      return globalComponentRegistry.getComponent(Transport.class);
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
}
