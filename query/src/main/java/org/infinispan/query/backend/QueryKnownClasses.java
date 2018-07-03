package org.infinispan.query.backend;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.transaction.Transaction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

// TODO [anistor] This class must be removed in 10.0 after we remove autodetection.

/**
 * Stores all entity classes known to query module in a replicated cache. The entry value is a boolean which indicates
 * if the type is indexable. The key is a KeyValuePair composed of the cache name and the class. This cache is 'append only'.
 * <p>
 * Write operations are expected to happen only exceptionally, therefore this code
 * is heavily optimized for reads (at cost of writes).
 * Also we're assuming all entries are small: there is no size limit nor cleanup strategy.
 * <p>
 * This is not caching the fact that some key is not defined: that would be tricky to
 * get right and is not needed for our use case.
 *
 * @author Sanne Grinovero (C) 2013 Red Hat Inc.
 * @author anistor@redhat.com
 * @deprecated To be removed in Infinispan 10.0
 */
@ThreadSafe
@Deprecated
public final class QueryKnownClasses {

   private static final Log log = LogFactory.getLog(QueryKnownClasses.class);

   public static final String QUERY_KNOWN_CLASSES_CACHE_NAME = "___query_known_classes";

   private final Set<Class<?>> indexedEntities;

   private final String cacheName;

   private final EmbeddedCacheManager cacheManager;

   private final InternalCacheRegistry internalCacheRegistry;

   private volatile SearchFactoryHandler searchFactoryHandler;

   /**
    * A replicated cache that is lazily instantiated on first access.
    */
   private volatile AdvancedCache<KeyValuePair<String, Class<?>>, Boolean> knownClassesCache;

   private volatile TransactionHelper transactionHelper;

   /**
    * A second level cache. Not using a ConcurrentHashMap as this will degenerate into a read-only Map at runtime;
    * in the Query specific case we're only adding new class types while they are being discovered,
    * after this initial phase this is supposed to be a read-only immutable map.
    */
   private final AtomicReference<Map<Class<?>, Boolean>> localCache;

   /**
    * Constructor used only in pre-declared mode.
    */
   QueryKnownClasses(Set<Class<?>> indexedEntities) {
      this.indexedEntities = Collections.unmodifiableSet(new HashSet<>(indexedEntities));
      this.cacheName = null;
      this.cacheManager = null;
      this.internalCacheRegistry = null;
      this.localCache = null;
   }

   /**
    * Constructor used only in autodetect mode.
    *
    * @deprecated will be removed in Infinispan 10.0
    */
   @Deprecated
   QueryKnownClasses(String cacheName, EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry) {
      this.indexedEntities = null;
      this.cacheName = cacheName;
      this.cacheManager = cacheManager;
      this.internalCacheRegistry = internalCacheRegistry;
      this.localCache = new AtomicReference<>(Collections.emptyMap());
   }

   String getCacheName() {
      return cacheName;
   }

   boolean isAutodetectEnabled() {
      return indexedEntities == null;
   }

   void start(SearchFactoryHandler searchFactoryHandler) {
      if (indexedEntities != null) {
         throw new IllegalStateException("Cannot start internal cache unless we are in autodetect mode");
      }
      if (searchFactoryHandler == null) {
         throw new IllegalArgumentException("null argument not allowed");
      }
      this.searchFactoryHandler = searchFactoryHandler;
      startInternalCache();
      knownClassesCache.addListener(searchFactoryHandler.getCacheListener(), key -> key.getKey().equals(cacheName));
   }

   void stop() {
      if (knownClassesCache != null) {
         if (searchFactoryHandler != null) {
            knownClassesCache.removeListener(searchFactoryHandler.getCacheListener());
            searchFactoryHandler = null;
         }
         knownClassesCache = null;
      }
   }

   Set<Class<?>> keys() {
      if (indexedEntities != null) {
         return indexedEntities;
      }

      startInternalCache();
      Set<Class<?>> result = new HashSet<>();
      Transaction tx = transactionHelper.suspendTxIfExists();
      try {
         for (KeyValuePair<String, Class<?>> key : knownClassesCache.keySet()) {
            if (key.getKey().equals(cacheName)) {
               result.add(key.getValue());
            }
         }
         return result;
      } finally {
         transactionHelper.resume(tx);
      }
   }

   boolean containsKey(final Class<?> clazz) {
      if (indexedEntities != null) {
         return indexedEntities.contains(clazz);
      }
      return localCache.get().containsKey(clazz);
   }

   Boolean get(final Class<?> clazz) {
      if (indexedEntities != null) {
         return indexedEntities.contains(clazz);
      }
      return localCache.get().get(clazz);
   }

   void put(final Class<?> clazz, final Boolean value) {
      if (indexedEntities != null) {
         throw new IllegalStateException("Autodetect mode is not enabled");
      }

      if (value == null) {
         throw new IllegalArgumentException("Null values are not allowed");
      }
      startInternalCache();
      Transaction tx = transactionHelper.suspendTxIfExists();
      try {
         runCommand(() -> knownClassesCache.put(new KeyValuePair<>(cacheName, clazz), value));
      } finally {
         transactionHelper.resume(tx);
      }

      localCacheInsert(clazz, value);
   }

   private void localCacheInsert(final Class<?> key, final Boolean value) {
      synchronized (localCache) {
         final Map<Class<?>, Boolean> currentContent = localCache.get();
         final int currentSize = currentContent.size();
         if (currentSize == 0) {
            localCache.lazySet(Collections.singletonMap(key, value));
         } else {
            Map<Class<?>, Boolean> updatedContent = new HashMap<>(currentSize + 1);
            updatedContent.putAll(currentContent);
            updatedContent.put(key, value);
            localCache.lazySet(Collections.unmodifiableMap(updatedContent));
         }
      }
   }

   /**
    * Start the internal cache lazily.
    */
   private void startInternalCache() {
      if (knownClassesCache == null) {
         synchronized (this) {
            if (knownClassesCache == null) {
               internalCacheRegistry.registerInternalCache(QUERY_KNOWN_CLASSES_CACHE_NAME, getInternalCacheConfig(), EnumSet.of(InternalCacheRegistry.Flag.PERSISTENT));
               Cache<KeyValuePair<String, Class<?>>, Boolean> knownClassesCache = SecurityActions.getCache(cacheManager, QUERY_KNOWN_CLASSES_CACHE_NAME);
               this.knownClassesCache = knownClassesCache.getAdvancedCache().withFlags(Flag.SKIP_LOCKING, Flag.IGNORE_RETURN_VALUES);
               transactionHelper = new TransactionHelper(this.knownClassesCache.getTransactionManager());
            }
         }
      }
   }

   private Configuration getInternalCacheConfig() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

      // allow the registry to work for local caches as well as clustered caches
      CacheMode cacheMode = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration().isClustered()
            ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      configurationBuilder.clustering().cacheMode(cacheMode);

      // use invocation batching (cache-only transactions) for high consistency as writes are expected to be rare in this cache
      configurationBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(null).invocationBatching().enable();

      configurationBuilder.security().authorization().disable();
      return configurationBuilder.build();
   }

   /**
    * Run command and retry if suspect exception was thrown.
    */
   private void runCommand(Runnable runnable) {
      while (true) {
         try {
            runnable.run();
            break;
         } catch (CacheException e) {
            if (SuspectException.isSuspectExceptionInChain(e)) {
               // Retry the command
               log.trace("Ignoring suspect exception and retrying operation for internal cache.");
            } else {
               throw e;
            }
         }
      }
   }

}
