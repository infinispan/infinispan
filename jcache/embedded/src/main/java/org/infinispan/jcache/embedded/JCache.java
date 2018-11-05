package org.infinispan.jcache.embedded;


import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListener;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;
import javax.management.MBeanServer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ExpirationConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.Param;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.jcache.AbstractJCache;
import org.infinispan.jcache.AbstractJCacheListenerAdapter;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.FailureEntryProcessorResult;
import org.infinispan.jcache.JCacheEntry;
import org.infinispan.jcache.SuccessEntryProcessorResult;
import org.infinispan.jcache.embedded.functions.GetAndPut;
import org.infinispan.jcache.embedded.functions.GetAndRemove;
import org.infinispan.jcache.embedded.functions.GetAndReplace;
import org.infinispan.jcache.embedded.functions.Invoke;
import org.infinispan.jcache.embedded.functions.Put;
import org.infinispan.jcache.embedded.functions.PutIfAbsent;
import org.infinispan.jcache.embedded.functions.ReadWithExpiry;
import org.infinispan.jcache.embedded.functions.Remove;
import org.infinispan.jcache.embedded.functions.RemoveConditionally;
import org.infinispan.jcache.embedded.functions.Replace;
import org.infinispan.jcache.embedded.functions.ReplaceConditionally;
import org.infinispan.jcache.embedded.logging.Log;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.util.logging.LogFactory;

/**
 * Infinispan's implementation of {@link javax.cache.Cache} interface.
 *
 * @author Vladimir Blagojevic
 * @author Galder Zamarreño
 * @since 5.3
 */
public class JCache<K, V> extends AbstractJCache<K, V> {

   private static final Log log =
         LogFactory.getLog(JCache.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final AdvancedCache<K, V> cache;
   private final AdvancedCache<K, V> skipCacheLoadCache;
   private final AdvancedCache<K, V> skipCacheLoadAndStatsCache;
   private final AdvancedCache<K, V> skipListenerCache;
   private final ReadWriteMap<K, V> rwMap;
   private final ReadWriteMap<K, V> rwMapSkipCacheLoad;
   private final RICacheStatistics stats;

   public JCache(AdvancedCache<K, V> cache, CacheManager cacheManager, ConfigurationAdapter<K, V> c) {
      super(adjustConfiguration(c.getConfiguration(), cache), cacheManager, new JCacheNotifier<>());
      this.cache = cache;
      this.skipCacheLoadCache = cache.withFlags(Flag.SKIP_CACHE_LOAD);
      this.skipCacheLoadAndStatsCache = cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_STATISTICS);
      // Typical use cases of the SKIP_LISTENER_NOTIFICATION is when trying
      // to comply with specifications such as JSR-107, which mandate that
      // {@link Cache#clear()}} calls do not fire entry removed notifications
      this.skipListenerCache = cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION);
      this.rwMap = ReadWriteMapImpl.create(FunctionalMapImpl.create(cache));
      this.rwMapSkipCacheLoad = rwMap.withParams(Param.PersistenceMode.SKIP_LOAD);
      this.stats = new RICacheStatistics(this.cache);

      addConfigurationListeners();

      if (cache.getComponentRegistry().getComponent(ExpiryPolicy.class) == null) {
         cache.getComponentRegistry().registerComponent(expiryPolicy, ExpiryPolicy.class);
      }

      setCacheLoader(configuration);
      setCacheWriter(configuration);

      if (configuration.isManagementEnabled())
         setManagementEnabled(true);

      if (configuration.isStatisticsEnabled())
         setStatisticsEnabled(true);
   }

   private static <K, V> MutableConfiguration<K, V> adjustConfiguration(MutableConfiguration<K, V> configuration, AdvancedCache<K, V> cache) {
      Configuration cfg = cache.getCacheConfiguration();
      boolean lifespanSet = cfg.expiration().attributes().attribute(ExpirationConfiguration.LIFESPAN).isModified();
      boolean maxIdleSet = cfg.expiration().attributes().attribute(ExpirationConfiguration.MAX_IDLE).isModified();
      if (lifespanSet || maxIdleSet) {
         configuration.setExpiryPolicyFactory(new LimitExpiryFactory(
               configuration.getExpiryPolicyFactory(), cfg.expiration().lifespan(), cfg.expiration().maxIdle()));
      }
      return configuration;
   }

   protected void addCacheLoaderAdapter(CacheLoader<K, V> cacheLoader) {
      PersistenceManagerImpl persistenceManager =
            (PersistenceManagerImpl) cache.getComponentRegistry().getComponent(PersistenceManager.class);
      JCacheLoaderAdapter<K, V> adapter = getCacheLoaderAdapter(persistenceManager);
      adapter.setCacheLoader(jcacheLoader);
      adapter.setExpiryPolicy(expiryPolicy);
   }

   @Override
   protected void addCacheWriterAdapter(CacheWriter<? super K, ? super V> cacheWriter) {
      PersistenceManagerImpl persistenceManager =
            (PersistenceManagerImpl) cache.getComponentRegistry().getComponent(PersistenceManager.class);
      JCacheWriterAdapter<K, V> ispnCacheStore = getCacheWriterAdapter(persistenceManager);
      ispnCacheStore.setCacheWriter(jcacheWriter);
   }

   @SuppressWarnings("unchecked")
   private JCacheLoaderAdapter<K, V> getCacheLoaderAdapter(PersistenceManagerImpl persistenceManager) {
      return (JCacheLoaderAdapter<K, V>) persistenceManager.getAllLoaders().get(0);
   }

   @SuppressWarnings("unchecked")
   private JCacheWriterAdapter<K, V> getCacheWriterAdapter(PersistenceManagerImpl persistenceManager) {
      return (JCacheWriterAdapter<K, V>) persistenceManager.getAllWriters().get(0);
   }

   @Override
   public void clear() {
      // TCK expects clear() to not fire any remove events
      try {
         skipListenerCache.clear();
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public boolean containsKey(final K key) {
      checkNotClosed();

      if (trace)
         log.tracef("Invoke containsKey(key=%s)", key);

      if (key == null)
         throw log.parameterMustNotBeNull("key");

      try {
         return skipCacheLoadAndStatsCache.containsKey(key);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public V get(final K key) {
      checkNotClosed();
      checkNotNull(key, "key");
      try {
         return readMap().eval(key, new ReadWithExpiry<>()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   private ReadWriteMap<K, V> readMap() {
      return configuration.isReadThrough() ? this.rwMap : rwMapSkipCacheLoad;
   }

   @Override
   public Map<K, V> getAll(Set<? extends K> keys) {
      checkNotClosed();
      verifyKeys(keys);
      if (keys.isEmpty()) {
         return Collections.emptyMap();
      }
      try {
         return evalMany(readMap(), keys, new ReadWithExpiry<>());
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   private <R> Map<K, R> evalMany(ReadWriteMap<K, V> map, Set<? extends K> keys, Function<EntryView.ReadWriteEntryView<K, V>, R> function) {
      // ReadWriteMap.evalMany is not that useful since it forces us to transfer keys
      return keys.stream().map(k -> new SimpleEntry<>(k, map.eval(k, function)))
            // intermediary list to force all removes to be invoked before waiting for any
            .collect(Collectors.toList()).stream().filter(e -> e.getValue().join() != null)
            .collect(Collectors.toMap(SimpleEntry::getKey, e -> e.getValue().join()));
   }

   @Override
   public V getAndPut(final K key, final V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");
      try {
         return rwMapSkipCacheLoad.eval(key, value, new GetAndPut<>()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public V getAndRemove(final K key) {
      checkNotClosed();
      checkNotNull(key, "key");
      try {
         // We cannot use the regular remove because it does not count hit/miss statistics as JCache expects
         return rwMapSkipCacheLoad.eval(key, GetAndRemove.getInstance()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public V getAndReplace(final K key, final V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");
      try {
         checkNotNull(value, "value");
         return rwMapSkipCacheLoad.eval(key, value, new GetAndReplace<>()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public void close() {
      super.close();
      cache.stop();
   }

   @Override
   public boolean isClosed() {
      return cache.getStatus().isTerminated();
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public <T> T invoke(final K key, final EntryProcessor<K, V, T> entryProcessor, final Object... arguments) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(entryProcessor, "entryProcessor");
      try {
         ReadWriteMap<K, V> rw = configuration.isReadThrough() ? rwMap : rwMapSkipCacheLoad;
         return rw.eval(key, new Invoke<>(entryProcessor, arguments,
               !configuration.isStoreByValue())).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
      checkNotClosed();
      verifyKeys(keys);
      checkNotNull(entryProcessor, "entryProcessor");
      try {
         ReadWriteMap<K, V> rw = configuration.isReadThrough() ? rwMap : rwMapSkipCacheLoad;
         // ReadWriteMap.evalMany is not that useful since it forces us to transfer keys
         return keys.stream().map(k -> new SimpleEntry<>(k, rw.eval(k, new Invoke<>(entryProcessor, arguments, !configuration.isStoreByValue()))))
               // intermediary list to force all removes to be invoked before waiting for any
               .collect(Collectors.toList()).stream().filter(e -> {
                  try {
                     return e.getValue().join() != null;
                  } catch (CompletionException ex) {
                     return true;
                  }
               })
               .collect(Collectors.toMap(SimpleEntry::getKey, e -> {
                  try {
                     return new SuccessEntryProcessorResult(e.getValue().join());
                  } catch (CompletionException ex) {
                     return new FailureEntryProcessorResult<>(ex);
                  }
               }));
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public Iterator<Cache.Entry<K, V>> iterator() {
      if (isClosed()) {
         throw log.cacheClosed(cache.getStatus());
      }
      // TODO
      return new Itr();
   }

   @Override
   public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, final CompletionListener listener) {
      checkNotClosed();
      verifyKeys(keys);

      // Separate logic based on whether a JCache cache loader or an Infinispan
      // cache loader might be plugged. TCK has some strict expectations when
      // it comes to expiry policy usage when loaded entries are stored in the
      // cache for the first time, or if they're overriding a value (forced by
      // replaceExistingValues). These two cases cannot be discerned at the
      // cache loader level. The only alternative way would be for a jcache
      // loader interceptor to be created, but getting the interaction right
      // with existing cache loader interceptor would not be trivial. Hence,
      // we separate logic at this level.

      try {
         if (jcacheLoader == null && jcacheWriter != null)
            setListenerCompletion(listener);
         else if (jcacheLoader != null) {
            loadAllFromJCacheLoader(keys, replaceExistingValues, listener);
         } else
            loadAllFromInfinispanCacheLoader(keys, replaceExistingValues, listener);
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   private void loadAllFromJCacheLoader(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener listener) {
      AtomicInteger countDown = new AtomicInteger(1);
      BiConsumer<Object, Throwable> completionAction = (nil, t) -> {
         if (t != null) {
            if (countDown.getAndSet(0) != 0) {
               setListenerException(listener, t);
            }
            return;
         }
         if (countDown.decrementAndGet() == 0) {
            setListenerCompletion(listener);
         }
      };
      try {
         Map<K, V> loaded = loadAllKeys(keys);
         for (Map.Entry<K, V> entry : loaded.entrySet()) {
            K loadedKey = entry.getKey();
            V loadedValue = entry.getValue();
            if (loadedValue == null) {
               continue;
            }
            countDown.incrementAndGet();
            if (replaceExistingValues) {
               rwMap.withParams(Param.PersistenceMode.SKIP)
                     .eval(loadedKey, loadedValue, new Put<>()).whenComplete(completionAction);
            } else {
               rwMap.withParams(Param.PersistenceMode.SKIP)
                     .eval(loadedKey, loadedValue, new PutIfAbsent<>()).whenComplete(completionAction);
            }
         }
         completionAction.accept(null, null);
      } catch (Throwable t) {
         completionAction.accept(null, t);
      }
   }

   @Override
   public void put(final K key, final V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");
      // A normal put should not fire notifications when checking TTL
      try {
         rwMapSkipCacheLoad.eval(key, value, new Put<>()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> inputMap) {
      checkNotClosed();
      InfinispanCollections.assertNotNullEntries(inputMap, "inputMap");  // spec required check
      try {
         if (configuration.isWriteThrough()) {
            // Write-through caching expects that a failure persisting one entry
            // does not block the commit of other, already persisted entries
            inputMap.entrySet().stream().map(e -> rwMap.eval(e.getKey(), e.getValue(), new Put<>()))
                  .collect(Collectors.toList()).forEach(CompletableFuture::join);
         } else {
            rwMapSkipCacheLoad.evalMany(inputMap, new Put<>()).forEach(nil -> {});
         }
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public boolean putIfAbsent(final K key, final V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");
      try {
         return rwMapSkipCacheLoad.eval(key, value, new PutIfAbsent<>()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public boolean remove(final K key) {
      checkNotClosed();
      checkNotNull(key, "key");
      try {
         return rwMapSkipCacheLoad.eval(key, Remove.getInstance()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public boolean remove(final K key, final V oldValue) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(oldValue, "oldValue");
      try {
         return rwMapSkipCacheLoad.eval(key, oldValue, new RemoveConditionally<>()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public void removeAll() {
      if (isClosed()) {
         throw log.cacheClosed(cache.getStatus());
      }
      // Calling cache.clear() won't work since there's currently no way to
      // for an Infinispan cache store to figure out all keys store and pass
      // them to CacheWriter.deleteAll(), hence, delete individually.
      // TODO: What happens with entries only in store but not in memory?

      // Delete asynchronously and then wait for removals to complete
      List<CompletableFuture<V>> futures = new ArrayList<>();
      try {
         for (final K key : cache.keySet()) {
            futures.add(cache.removeAsync(key));
         }
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }

      try {
         CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public void removeAll(Set<? extends K> keys) {
      checkNotClosed();
      verifyKeys(keys);
      try {
         if (configuration.isWriteThrough()) {
            // Write-through caching expects that a failure persisting one entry
            // does not block the commit of other, already persisted entries
            keys.stream().map(k -> rwMapSkipCacheLoad.eval(k, Remove.getInstance()))
                  .collect(Collectors.toList()).forEach(CompletableFuture::join);
         } else {
            rwMapSkipCacheLoad.evalMany(keys, Remove.getInstance()).forEach(b -> {});
         }
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public boolean replace(final K key, final V value) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(value, "value");
      try {
         return rwMapSkipCacheLoad.eval(key, value, new Replace<>()).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public boolean replace(final K key, final V oldValue, final V newValue) {
      checkNotClosed();
      checkNotNull(key, "key");
      checkNotNull(oldValue, "oldValue");
      checkNotNull(newValue, "newValue");
      try {
         return rwMapSkipCacheLoad.eval(key, newValue, new ReplaceConditionally<>(oldValue)).join();
      } catch (CompletionException e) {
         throw Exceptions.launderException(e);
      } catch (org.infinispan.commons.CacheException e) {
         throw Exceptions.launderException(e);
      }
   }

   @Override
   public <T> T unwrap(Class<T> clazz) {
      return ReflectionUtil.unwrapAny(clazz, this, cache);
   }

   @Override
   public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      notifier.addListener(listenerCfg, this, notifier);
      addCacheEntryListenerConfiguration(listenerCfg);
   }

   @Override
   public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> listenerCfg) {
      notifier.removeListener(listenerCfg, this);
      removeCacheEntryListenerConfiguration(listenerCfg);
   }

   public void setStatisticsEnabled(boolean enabled) {
      cache.getStats().setStatisticsEnabled(enabled);
      super.setStatisticsEnabled(enabled);
   }

   protected CacheStatisticsMXBean getCacheStatisticsMXBean() {
      return stats;
   }

   protected MBeanServer getMBeanServer() {
      GlobalJmxStatisticsConfiguration jmxConfig = cache.getCacheManager().getCacheManagerConfiguration().globalJmxStatistics();
      if (jmxConfig.enabled()) {
         return JmxUtil.lookupMBeanServer(jmxConfig.mbeanServerLookup(), jmxConfig.properties());
      } else {
         return null;
      }
   }

   protected AbstractJCache<K, V> checkNotClosed() {
      if (isClosed()) {
         throw log.cacheClosed(cache.getStatus());
      }

      return this;
   }

   private void loadAllFromInfinispanCacheLoader(Set<? extends K> keys, boolean replaceExistingValues, final CompletionListener listener) {
      AtomicInteger countDown = new AtomicInteger(keys.size() + 1);
      BiConsumer<V, Throwable> completionAction = (v, t) -> {
         if (t != null) {
            if (countDown.getAndSet(0) != 0) {
               setListenerException(listener, t);
            }
            return;
         }
         if (trace)
            log.tracef("Key loaded, wait for the rest of keys to load");

         if (countDown.decrementAndGet() == 0) {
            setListenerCompletion(listener);
         }
      };
      try {
         for (K k : keys) {
            if (replaceExistingValues) {
               cache.evict(k);
            }
            cache.getAsync(k).whenComplete(completionAction);
         }
         completionAction.accept(null, null);
      } catch (Throwable t) {
         log.errorLoadingAll(keys, t);
         completionAction.accept(null, t);
      }
   }

   private class Itr implements Iterator<Cache.Entry<K, V>> {

      private final Iterator<Map.Entry<K, V>> it = cache.entrySet().iterator();
      private Entry<K, V> current;
      private Entry<K, V> next;

      Itr() {
         fetchNext();
      }

      private void fetchNext() {
         long start = statisticsEnabled() ? System.nanoTime() : 0;
         if (it.hasNext()) {
            Map.Entry<K, V> entry = it.next();
            next = new JCacheEntry<>(
                  entry.getKey(), entry.getValue());

            if (statisticsEnabled()) {
               stats.increaseCacheHits(1);
               stats.addGetTimeNano(System.nanoTime() - start);
            }
         } else {
            next = null;
         }
      }

      @Override
      public boolean hasNext() {
         return next != null;
      }

      @Override
      public Entry<K, V> next() {
         if (next == null)
            fetchNext();

         if (next == null)
            throw new NoSuchElementException();

         // Set return value
         Entry<K, V> ret = next;

         // Force expiration if needed
         updateTTLForAccessed(cache, next.getKey(), next.getValue());

         current = next;

         // Fetch next...
         fetchNext();

         return ret;
      }

      @Override
      public void remove() {
         if (current == null)
            throw new IllegalStateException();

         // TODO: Should Infinispan's core iterators be mutable?
         // It can be worked around as shown here for JSR-107 needs
         K k = current.getKey();
         current = null;
         cache.remove(k);
      }
   }

   @Override
   protected void addListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter) {
      cache.addListener(listenerAdapter);
   }

   @Override
   protected void removeListener(AbstractJCacheListenerAdapter<K, V> listenerAdapter) {
      cache.removeListener(listenerAdapter);
   }

   @Override
   protected void evict(K key) {
      cache.evict(key);
   }
}
