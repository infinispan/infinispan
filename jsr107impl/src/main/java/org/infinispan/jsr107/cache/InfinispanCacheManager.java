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
package org.infinispan.jsr107.cache;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.Configuration;
import javax.cache.OptionalFeature;
import javax.cache.Status;
import javax.cache.transaction.IsolationLevel;
import javax.cache.transaction.Mode;
import javax.transaction.UserTransaction;

import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.FileLookupFactory;

/**
 * InfinispanCacheManager is Infinispan's implementation of {@link javax.cache.CacheManager}.
 * 
 * @author Vladimir Blagojevic
 * @since 5.3
 */
public class InfinispanCacheManager implements CacheManager {
   private final HashMap<String, Cache<?, ?>> caches = new HashMap<String, Cache<?, ?>>();
   private final String name;
   private final ClassLoader classLoader;
   private EmbeddedCacheManager cmDelegate;
   private volatile Status status = Status.UNINITIALISED;

   /**
    * Create a new InfinispanCacheManager given a cache name and a {@link ClassLoader}. Cache name
    * might refer to a file on classpath containing Infinispan configuration file.
    * 
    * @param name
    * @param classLoader
    */
   public InfinispanCacheManager(String name, ClassLoader classLoader) {
      if (classLoader == null) {
         throw new IllegalArgumentException("Classloader cannot be null");
      }
      if (name == null || name.length() == 0) {
         throw new IllegalArgumentException("Invalid CacheManager name " + name);
      }

      this.name = name;
      this.classLoader = classLoader;

      ConfigurationBuilderHolder cbh = null;
      try {
         InputStream configurationStream = FileLookupFactory.newInstance().lookupFileStrict(name,
                  classLoader);
         cbh = new ParserRegistry(classLoader).parse(configurationStream);
      } catch (FileNotFoundException e) {
         // no such file, lets use default CBH
         cbh = new ConfigurationBuilderHolder(classLoader);
         cbh.getGlobalConfigurationBuilder().transport().defaultTransport().build();
         // TODO investigate this below as TCK fails if we disable duplicate domains
         // org.jsr107.tck.CacheManagerFactoryTest fails
         cbh.getGlobalConfigurationBuilder().globalJmxStatistics().allowDuplicateDomains(true);
      }
      cmDelegate = new DefaultCacheManager(cbh, true);
      
      // TODO get predefined caches and register them
      // TODO galderz find a better way to do this as spec allows predefined caches to be
      // loaded (config file), instantiated and registered with CacheManager
      Set<String> cacheNames = cmDelegate.getCacheNames();
      for (String cacheName : cacheNames) {
         caches.put(cacheName, new InfinispanCache<Object, Object>(cmDelegate.getCache(cacheName),
                  this, classLoader, null));
      }
      
      status = Status.STARTED;
   }

   @Override
   public String getName() {
      return name;
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
         Cache<?, ?> cache = caches.get(cacheName);

         if (cache == null) {
            ConfigurationAdapter<K, V> adapter = new ConfigurationAdapter<K, V>(c, classLoader);
            org.infinispan.configuration.cache.Configuration ic = adapter.build();
            cmDelegate.defineConfiguration(cacheName, ic);
            org.infinispan.Cache<K, V> delegateCache = cmDelegate.getCache(cacheName);
            cache = new InfinispanCache<K, V>(delegateCache, this, classLoader, c);
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

   @Override
   public <K, V> Cache<K, V> getCache(String cacheName) {
      checkStartedStatus();
      synchronized (caches) {
         @SuppressWarnings("unchecked")
         final Cache<K, V> cache = (Cache<K, V>) caches.get(cacheName);
         return cache;
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
      // TODO
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isSupported(OptionalFeature optionalFeature) {
      return Caching.isSupported(optionalFeature);
   }

   @Override
   public void shutdown() {
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
      cmDelegate.stop();
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

   private void checkStartedStatus() {
      if (status != Status.STARTED) {
         throw new IllegalStateException();
      }
   }
}
