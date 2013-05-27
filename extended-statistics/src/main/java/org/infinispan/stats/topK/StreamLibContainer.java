package org.infinispan.stats.topK;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This contains all the stream lib top keys. Stream lib is a space efficient technique to obtains the top-most
 * counters.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class StreamLibContainer {

   public static final int MAX_CAPACITY = 100000;
   private static final Log log = LogFactory.getLog(StreamLibContainer.class);
   private final String cacheName;
   private final String address;
   private final Map<Stat, StreamSummary<Object>> streamSummaryEnumMap;
   private final Map<Stat, Lock> lockMap;
   private volatile int capacity = 1000;
   private volatile boolean enabled = false;

   public StreamLibContainer(String cacheName, String address) {
      this.cacheName = cacheName;
      this.address = address;
      streamSummaryEnumMap = Collections.synchronizedMap(new EnumMap<Stat, StreamSummary<Object>>(Stat.class));
      lockMap = new EnumMap<Stat, Lock>(Stat.class);

      for (Stat stat : Stat.values()) {
         lockMap.put(stat, new ReentrantLock());
      }

      resetAll(1);
   }

   public static StreamLibContainer getOrCreateStreamLibContainer(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      StreamLibContainer streamLibContainer = componentRegistry.getComponent(StreamLibContainer.class);
      if (streamLibContainer == null) {
         String cacheName = cache.getName();
         String address = String.valueOf(cache.getCacheManager().getAddress());
         componentRegistry.registerComponent(new StreamLibContainer(cacheName, address), StreamLibContainer.class);
      }
      return componentRegistry.getComponent(StreamLibContainer.class);
   }

   /**
    * @return {@code true} if the top-key collection is enabled, {@code false} otherwise.
    */
   public boolean isEnabled() {
      return enabled;
   }

   /**
    * Enables or disables the top-key collection
    *
    * @param enabled
    */
   public void setEnabled(boolean enabled) {
      if (!this.enabled && enabled) {
         resetAll(capacity);
      } else if (!enabled) {
         resetAll(1);
      }
      this.enabled = enabled;
   }

   /**
    * See {@link #setCapacity(int)}.
    *
    * @return the current top-key capacity
    */
   public int getCapacity() {
      return capacity;
   }

   /**
    * Sets the capacity of the top-key. The capacity defines the maximum number of keys that are tracked. Remember that
    * top-key is a probabilistic counter so the higher the number of keys, the more precise will be the counters
    *
    * @param capacity
    */
   public void setCapacity(int capacity) {
      if (capacity <= 0) {
         this.capacity = 1;
      } else {
         this.capacity = capacity;
      }
   }

   /**
    * Adds the key to the read top-key.
    *
    * @param key
    * @param remote {@code true} if the key is remote, {@code false} otherwise.
    */
   public void addGet(Object key, boolean remote) {
      if (!isEnabled()) {
         return;
      }
      syncOffer(remote ? Stat.REMOTE_GET : Stat.LOCAL_GET, key);
   }

   /**
    * Adds the key to the put top-key.
    *
    * @param key
    * @param remote {@code true} if the key is remote, {@code false} otherwise.
    */
   public void addPut(Object key, boolean remote) {
      if (!isEnabled()) {
         return;
      }

      syncOffer(remote ? Stat.REMOTE_PUT : Stat.LOCAL_PUT, key);
   }

   /**
    * Adds the lock information about the key, namely if the key suffer some contention and if the keys was locked or
    * not.
    *
    * @param key
    * @param contention {@code true} if the key was contented.
    * @param failLock   {@code true} if the key was not locked.
    */
   public void addLockInformation(Object key, boolean contention, boolean failLock) {
      if (!isEnabled()) {
         return;
      }

      syncOffer(Stat.MOST_LOCKED_KEYS, key);

      if (contention) {
         syncOffer(Stat.MOST_CONTENDED_KEYS, key);
      }
      if (failLock) {
         syncOffer(Stat.MOST_FAILED_KEYS, key);
      }
   }

   /**
    * Adds the key to the write skew failed top-key.
    *
    * @param key
    */
   public void addWriteSkewFailed(Object key) {
      syncOffer(Stat.MOST_WRITE_SKEW_FAILED_KEYS, key);
   }

   /**
    * See {@link #getTopKFrom(org.infinispan.stats.topK.StreamLibContainer.Stat, int)}.
    *
    * @param stat
    * @return the top-key referring to the stat for all the keys.
    */
   public Map<Object, Long> getTopKFrom(Stat stat) {
      return getTopKFrom(stat, capacity);
   }

   /**
    * @param stat
    * @param topK the topK-th first key.
    * @return the topK-th first key referring to the stat.
    */
   public Map<Object, Long> getTopKFrom(Stat stat, int topK) {
      try {
         lockMap.get(stat).lock();
         if (log.isTraceEnabled()) {
            log.tracef("Get top-k for [%s]", stat);
         }
         return getStatsFrom(streamSummaryEnumMap.get(stat), topK);
      } finally {
         lockMap.get(stat).unlock();
      }

   }

   /**
    * Resets all the top-key collected so far.
    */
   public void resetAll() {
      resetAll(capacity);
   }

   /**
    * Resets the top-key for stat.
    *
    * @param stat
    */
   public void resetStat(Stat stat) {
      resetStat(stat, capacity);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StreamLibContainer that = (StreamLibContainer) o;

      return !(address != null ? !address.equals(that.address) : that.address != null) && !(cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null);

   }

   @Override
   public int hashCode() {
      int result = cacheName != null ? cacheName.hashCode() : 0;
      result = 31 * result + (address != null ? address.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "StreamLibContainer{" +
            "cacheName='" + cacheName + '\'' +
            ", address='" + address + '\'' +
            '}';
   }

   private Map<Object, Long> getStatsFrom(StreamSummary<Object> ss, int topK) {
      List<Counter<Object>> counters = ss.topK(topK <= 0 ? 1 : topK);
      Map<Object, Long> results = new HashMap<Object, Long>(topK);

      for (Counter<Object> c : counters) {
         results.put(c.getItem(), c.getCount());
      }

      return results;
   }

   private void resetStat(Stat stat, int customCapacity) {
      try {
         lockMap.get(stat).lock();
         if (log.isTraceEnabled()) {
            log.tracef("Reset stat [%s]", stat);
         }
         streamSummaryEnumMap.put(stat, createNewStreamSummary(customCapacity));
      } finally {
         lockMap.get(stat).unlock();
      }
   }

   private StreamSummary<Object> createNewStreamSummary(int customCapacity) {
      return new StreamSummary<Object>(Math.min(MAX_CAPACITY, customCapacity));
   }

   private void resetAll(int customCapacity) {
      for (Stat stat : Stat.values()) {
         resetStat(stat, customCapacity);
      }
   }

   private void syncOffer(final Stat stat, Object key) {
      try {
         lockMap.get(stat).lock();
         if (log.isTraceEnabled()) {
            log.tracef("Offer key [%s] to stat [%s]", key, stat);
         }
         streamSummaryEnumMap.get(stat).offer(key);
      } finally {
         lockMap.get(stat).unlock();
      }
   }

   public static enum Stat {
      REMOTE_GET,
      LOCAL_GET,
      REMOTE_PUT,
      LOCAL_PUT,

      MOST_LOCKED_KEYS,
      MOST_CONTENDED_KEYS,
      MOST_FAILED_KEYS,
      MOST_WRITE_SKEW_FAILED_KEYS
   }
}
