/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.jcache;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.cache.*;
import javax.cache.spi.CachingProvider;
import javax.cache.transaction.IsolationLevel;
import javax.cache.transaction.Mode;
import javax.transaction.UserTransaction;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.jcache.logging.Log;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.FileLookup;
import org.infinispan.util.FileLookupFactory;
import org.infinispan.util.logging.LogFactory;

/**
 * Infinispan's implementation of {@link javax.cache.CacheManager}.
 * 
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCacheManager implements CacheManager {

   private static final Log log =
         LogFactory.getLog(JCacheManager.class, Log.class);

   private final HashMap<String, JCache<?, ?>> caches = new HashMap<String, JCache<?, ?>>();
   private final URI uri;
   private final EmbeddedCacheManager cm;
   private final CachingProvider provider;
   private volatile Status status = Status.UNINITIALISED;
   private final StackTraceElement[] allocationStackTrace;

   /**
    * Create a new InfinispanCacheManager given a cache name and a {@link ClassLoader}. Cache name
    * might refer to a file on classpath containing Infinispan configuration file.
    * 
    * @param uri identifies the cache manager
    * @param classLoader used to load classes stored in this cache manager
    */
   public JCacheManager(URI uri, ClassLoader classLoader, CachingProvider provider) {
      // Track allocation time
      this.allocationStackTrace = Thread.currentThread().getStackTrace();

      if (classLoader == null) {
         throw new IllegalArgumentException("Classloader cannot be null");
      }
      if (uri == null) {
         throw new IllegalArgumentException("Invalid CacheManager URI " + uri);
      }

      this.uri = uri;
      this.provider = provider;

      ConfigurationBuilderHolder cbh = getConfigurationBuilderHolder(classLoader);
      GlobalConfigurationBuilder globalBuilder = cbh.getGlobalConfigurationBuilder();
      // The cache manager name has to contain all uri, class loader and
      // provider information in order to guarantee JMX naming uniqueness.
      // This is tested by the TCK to make sure caching provider loaded
      // with different classloaders, even if the default classloader for
      // the cache manager is the same, are really different cache managers.
      String cacheManagerName = "uri=" + uri
            + "/classloader=" + classLoader.toString()
            + "/provider=" + provider.toString();
      // Set cache manager class loader and apply name to cache manager MBean
      globalBuilder.classLoader(classLoader)
            .globalJmxStatistics().cacheManagerName(cacheManagerName);

      cm = new DefaultCacheManager(cbh, true);
      registerPredefinedCaches();
      status = Status.STARTED;
   }

   public JCacheManager(URI uri, EmbeddedCacheManager cacheManager, CachingProvider provider) {
      // Track allocation time
      this.allocationStackTrace = Thread.currentThread().getStackTrace();
      this.uri = uri;
      this.provider = provider;
      this.cm = cacheManager;
      registerPredefinedCaches();
      status = Status.STARTED;
   }

   private void registerPredefinedCaches() {
      // TODO get predefined caches and register them
      // TODO galderz find a better way to do this as spec allows predefined caches to be
      // loaded (config file), instantiated and registered with CacheManager
      Set<String> cacheNames = cm.getCacheNames();
      for (String cacheName : cacheNames) {
         // With pre-defined caches, obey only pre-defined configuration
         caches.put(cacheName, new JCache<Object, Object>(
               cm.getCache(cacheName).getAdvancedCache(),
               this, new MutableConfiguration<Object, Object>()));
      }
   }

   private ConfigurationBuilderHolder getConfigurationBuilderHolder(
         ClassLoader classLoader) {
      try {
         FileLookup fileLookup = FileLookupFactory.newInstance();
         InputStream configurationStream = uri.isAbsolute()
               ? fileLookup.lookupFileStrict(uri, classLoader)
               : fileLookup.lookupFileStrict(uri.toString(), classLoader);
         return new ParserRegistry(classLoader).parse(configurationStream);
      } catch (FileNotFoundException e) {
         // No such file, lets use default CBH
         return new ConfigurationBuilderHolder(classLoader);
      }
   }

   @Override
   public CachingProvider getCachingProvider() {
      return provider;
   }

   @Override
   public URI getURI() {
      return uri;
   }

   @Override
   public Properties getProperties() {
      return null;
   }

   @Override
   public Status getStatus() {
      return status;
   }

   @SuppressWarnings("unchecked")
   @Override
   public <K, V> Cache<K, V> configureCache(String cacheName, Configuration<K, V> c) {
      checkStartedStatus();

      // spec required
      if (cacheName == null || cacheName.length() == 0) {
         throw new NullPointerException("cacheName specified is invalid " + cacheName);
      }

      // spec required
      if (c == null) {
         throw new NullPointerException("configuration specified is invalid " + c);
      }

      boolean noIsolationWithTx = c.getTransactionIsolationLevel() == IsolationLevel.NONE
               && c.getTransactionMode() != Mode.NONE;
      boolean isolationWithNoTx = c.getTransactionIsolationLevel() != IsolationLevel.NONE
               && c.getTransactionMode() == Mode.NONE;

      // spec required
      if (noIsolationWithTx || isolationWithNoTx) {
         throw new IllegalArgumentException("Incompatible IsolationLevel "
                  + c.getTransactionIsolationLevel() + " and tx mode " + c.getTransactionMode());
      }

      synchronized (caches) {
         JCache<?, ?> cache = caches.get(cacheName);

         if (cache == null) {
            ConfigurationAdapter<K, V> adapter = new ConfigurationAdapter<K, V>(c);
            cm.defineConfiguration(cacheName, adapter.build());
            AdvancedCache<K, V> ispnCache =
                  cm.<K, V>getCache(cacheName).getAdvancedCache();

            // In case the cache was stopped
            if (!ispnCache.getStatus().allowInvocations())
               ispnCache.start();

            cache = new JCache<K, V>(ispnCache, this, c);

            cache.start();
            caches.put(cache.getName(), cache);
         } else {
            // re-register attempt with different configuration
            if (!cache.getConfiguration().equals(c)) {
               throw new CacheException("Cache " + cacheName
                        + " already registered with configuration " + cache.getConfiguration()
                        + ", and can not be registered again with a new given configuration " + c);
            }
         }

         return (Cache<K, V>) cache;
      }
   }

   public <K, V> Cache<K, V> configureCache(String cacheName, AdvancedCache<K, V> ispnCache) {
      synchronized (caches) {
         JCache<?, ?> cache = caches.get(cacheName);
         if (cache == null) {
            cache = new JCache<K, V>(ispnCache, this, new MutableConfiguration<K, V>());
            caches.put(cacheName, cache);
         }
         return (Cache<K, V>) cache;
      }
   }

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      checkStartedStatus();
      synchronized (caches) {
         return (Cache<K, V>) caches.get(cacheName);
      }
   }

   @Override
   public Iterable<Cache<?, ?>> getCaches() {
      synchronized (caches) {
         HashSet<Cache<?, ?>> set = new HashSet<Cache<?, ?>>();
         for (Cache<?, ?> cache : caches.values()) {
            set.add(cache);
         }
         return Collections.unmodifiableSet(set);
      }
   }

   @Override
   public boolean removeCache(String cacheName) {
      checkStartedStatus();
      if (cacheName == null) {
         throw new NullPointerException();
      }
      Cache<?, ?> oldCache;
      synchronized (caches) {
         oldCache = caches.remove(cacheName);
      }
      if (oldCache != null) {
         oldCache.stop();
      }

      return oldCache != null;
   }

   @Override
   public UserTransaction getUserTransaction() {
      // TODO: Independent of cache configuration? TCK mandates it...
      return new JCacheUserTransaction(
            cm.getCache().getAdvancedCache().getTransactionManager());
   }

   @Override
   public boolean isSupported(OptionalFeature optionalFeature) {
      return provider.isSupported(optionalFeature);
   }

   @Override
   public void enableManagement(String cacheName, boolean enabled) {
      caches.get(cacheName).setManagementEnabled(enabled);
   }

   @Override
   public void enableStatistics(String cacheName, boolean enabled) {
      caches.get(cacheName).setStatisticsEnabled(enabled);
   }

   @Override
   public void close() {
      checkStartedStatus();
      ArrayList<Cache<?, ?>> cacheList;
      synchronized (caches) {
         cacheList = new ArrayList<Cache<?, ?>>(caches.values());
         caches.clear();
      }
      for (Cache<?, ?> cache : cacheList) {
         try {
            cache.stop();
         } catch (Exception e) {
            // log?
         }
      }
      cm.stop();
      status = Status.STOPPED;
   }

   @Override
   public <T> T unwrap(java.lang.Class<T> cls) {
      if (cls.isAssignableFrom(this.getClass())) {
         return cls.cast(this);
      }

      throw new IllegalArgumentException("Unwapping to " + cls
               + " is not a supported by this implementation");
   }

   /**
    * Avoid weak references to this cache manager
    * being garbage collected without being shutdown.
    */
   @Override
   protected void finalize() throws Throwable {
      try {
         if(status != Status.STOPPED) {
            // Create the leak description
            Throwable t = log.cacheManagerNotClosed();
            t.setStackTrace(allocationStackTrace);
            log.leakedCacheManager(t);
            // Close
            cm.stop();
         }
      } finally {
         super.finalize();
      }
   }

   private void checkStartedStatus() {
      if (status != Status.STARTED) {
         throw new IllegalStateException();
      }
   }
}
