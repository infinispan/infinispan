package org.infinispan.jcache;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;

import org.testng.Assert;

/**
 * Borrowed from TCK tests
 *
 * @author gustavonalle
 */
public class TrackingCacheEntryListener<K, V> implements CacheEntryCreatedListener<K, V>, CacheEntryUpdatedListener<K, V>, CacheEntryExpiredListener<K, V>, CacheEntryRemovedListener<K, V>, Serializable, AutoCloseable {
   final AtomicInteger created = new AtomicInteger();
   final AtomicInteger updated = new AtomicInteger();
   final AtomicInteger removed = new AtomicInteger();

   public TrackingCacheEntryListener() {
   }

   public int getCreated() {
      return this.created.get();
   }

   public int getUpdated() {
      return this.updated.get();
   }

   public int getRemoved() {
      return this.removed.get();
   }

   public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {

      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
         Assert.assertEquals(EventType.CREATED, event.getEventType());
         this.created.incrementAndGet();
      }

   }

   public void onExpired(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {
   }

   public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {

      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
         Assert.assertEquals(EventType.REMOVED, event.getEventType());
         this.removed.incrementAndGet();
         if (event.isOldValueAvailable()) {
            event.getOldValue();
         }
      }

   }

   public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {

      for (CacheEntryEvent<? extends K, ? extends V> event : events) {
         Assert.assertEquals(EventType.UPDATED, event.getEventType());
         this.updated.incrementAndGet();
         if (event.isOldValueAvailable()) {
            event.getOldValue();
         }
      }

   }

   public void close() {
   }
}

