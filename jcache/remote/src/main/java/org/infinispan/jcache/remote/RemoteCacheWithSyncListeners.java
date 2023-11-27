package org.infinispan.jcache.remote;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.cache.Cache;
import javax.cache.event.CacheEntryListenerException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.AbstractJCacheNotifier;
import org.infinispan.jcache.remote.logging.Log;

public class RemoteCacheWithSyncListeners<K, V> extends RemoteCacheWrapper<K, V> {
   private static final boolean DONT_EXPECT_EVENT_ON_NULL_RESULT = false;

   private static final Log log = LogFactory.getLog(RemoteCacheWithSyncListeners.class, Log.class);

   private final AbstractJCacheNotifier<K, V> notifier;
   private final Cache<K, V> cache;
   private final int timeout;

   public RemoteCacheWithSyncListeners(RemoteCache<K, V> delegate, AbstractJCacheNotifier<K, V> notifier, Cache<K, V> cache) {
      super(delegate);
      this.notifier = notifier;
      this.cache = cache;
      this.timeout = delegate.getRemoteCacheContainer().getConfiguration().socketTimeout();
   }

   @Override
   public V put(final K key, final V value) {
      return withSyncListeners(notifier.hasSyncCreatedListener(), key, value, () -> delegate.put(key, value));
   }

   @Override
   public V putIfAbsent(final K key, final V value) {
      return withSyncListeners(notifier.hasSyncCreatedListener(), key, value, () -> delegate.putIfAbsent(key, value));
   }

   public V remove(final Object key) {
      return withSyncListeners(notifier.hasSyncRemovedListener(), DONT_EXPECT_EVENT_ON_NULL_RESULT, (K) key, null, () -> delegate.remove(key));
   }

   @Override
   public boolean remove(Object key, Object oldValue) {
      return withSyncListeners(notifier.hasSyncRemovedListener(), DONT_EXPECT_EVENT_ON_NULL_RESULT, (K) key, null, () -> delegate.remove(key, oldValue));
   }

   @Override
   public V replace(final K key, final V value) {
      return withSyncListeners(notifier.hasSyncUpdatedListener(), key, value, () ->  delegate.replace(key, value));
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return withSyncListeners(notifier.hasSyncUpdatedListener(), key, newValue, () -> delegate.replace(key, oldValue, newValue));
   }

   @Override
   public boolean replaceWithVersion(final K key, final V newValue, final long version) {
      return withSyncListeners(notifier.hasSyncUpdatedListener(), key, newValue, () -> delegate.replaceWithVersion(key, newValue, version));
   }

   private <R> R withSyncListeners(boolean hasListeners, K key, V value, Callable<R> callable) {
      return withSyncListeners(hasListeners, true, key, value, callable);
   }

   private <R> R withSyncListeners(boolean hasListeners, boolean expectEventOnNull, K key, V value, Callable<R> callable) {
      try {
         if (!hasListeners) {
            return callable.call();
         }
         CountDownLatch latch = new CountDownLatch(1);
         notifier.addSyncNotificationLatch(cache, key, value, latch);
         try {
            R ret = callable.call();
            if ((ret == null) && !expectEventOnNull) {
               return ret;
            }
            boolean wasClosed = latch.await(timeout, TimeUnit.MILLISECONDS);
            if (!wasClosed) {
               log.timeoutWaitingEvent();
            }
            return ret;
         } finally {
            notifier.removeSyncNotificationLatch(cache, key, value, latch);
         }
      } catch (CacheEntryListenerException ex) {
         throw ex;
      } catch (Exception e) {
         throw new CacheEntryListenerException(e);
      }
   }

}
