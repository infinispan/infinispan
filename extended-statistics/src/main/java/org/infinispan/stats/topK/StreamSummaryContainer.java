package org.infinispan.stats.topK;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This contains all the stream lib top keys. Stream lib is a space efficient technique to obtains the top-most
 * counters.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public class StreamSummaryContainer {

   private static final int MAX_CAPACITY = 100000;
   private static final Log log = LogFactory.getLog(StreamSummaryContainer.class);
   private final String cacheName;
   private final String address;
   private final AtomicBoolean flushing;
   private final EnumMap<Stat, TopKeyWrapper> topKeyWrapper;
   private volatile int capacity = 1000;
   private volatile boolean enabled = false;
   private volatile boolean reset = false;

   public StreamSummaryContainer(String cacheName, String address) {
      this.cacheName = cacheName;
      this.address = address;
      flushing = new AtomicBoolean(false);
      topKeyWrapper = new EnumMap<Stat, TopKeyWrapper>(Stat.class);
      for (Stat stat : Stat.values()) {
         topKeyWrapper.put(stat, new TopKeyWrapper());
      }
      resetAll();
   }

   public static StreamSummaryContainer getOrCreateStreamLibContainer(Cache cache) {
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      StreamSummaryContainer streamLibContainer = componentRegistry.getComponent(StreamSummaryContainer.class);
      if (streamLibContainer == null) {
         String cacheName = cache.getName();
         String address = String.valueOf(cache.getCacheManager().getAddress());
         componentRegistry.registerComponent(new StreamSummaryContainer(cacheName, address), StreamSummaryContainer.class);
      }
      return componentRegistry.getComponent(StreamSummaryContainer.class);
   }

   /**
    * @return {@code true} if the top-key collection is enabled, {@code false} otherwise.
    */
   public boolean isEnabled() {
      return enabled;
   }

   /**
    * Enables or disables the top-key collection
    */
   public void setEnabled(boolean enabled) {
      if (!this.enabled && enabled) {
         resetAll();
      } else if (!enabled) {
         resetAll();
      }
      this.enabled = enabled;
   }

   public int getCapacity() {
      return capacity;
   }

   /**
    * Sets the capacity of the top-key. The capacity defines the maximum number of keys that are tracked. Remember that
    * top-key is a probabilistic counter so the higher the number of keys, the more precise will be the counters
    */
   public void setCapacity(int capacity) {
      if (capacity <= 0) {
         this.capacity = 1;
      } else {
         this.capacity = Math.min(capacity, MAX_CAPACITY);
      }
   }

   /**
    * Adds the key to the read top-key.
    *
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
    */
   public void addWriteSkewFailed(Object key) {
      syncOffer(Stat.MOST_WRITE_SKEW_FAILED_KEYS, key);
   }

   /**
    * See {@link #getTopKFrom(StreamSummaryContainer.Stat, int)}.
    *
    * @return the top-key referring to the stat for all the keys.
    */
   public Map<Object, Long> getTopKFrom(Stat stat) {
      return getTopKFrom(stat, capacity);
   }

   /**
    * @param topK the topK-th first key.
    * @return the topK-th first key referring to the stat.
    */
   public Map<Object, Long> getTopKFrom(Stat stat, int topK) {
      tryFlushAll();
      return topKeyWrapper.get(stat).topK(topK);
   }

   /**
    * Same as {@link #getTopKFrom(org.infinispan.stats.topK.StreamSummaryContainer.Stat)} but the keys are returned in
    * their String format.
    */
   public Map<String, Long> getTopKFromAsKeyString(Stat stat) {
      return getTopKFromAsKeyString(stat, capacity);
   }

   /**
    * Same as {@link #getTopKFrom(org.infinispan.stats.topK.StreamSummaryContainer.Stat, int)} but the keys are returned
    * in their String format.
    */
   public Map<String, Long> getTopKFromAsKeyString(Stat stat, int topK) {
      tryFlushAll();
      return topKeyWrapper.get(stat).topKAsString(topK);
   }

   /**
    * Resets all the top-key collected so far.
    */
   public final void resetAll() {
      reset = true;
      tryFlushAll();
   }

   /**
    * Tries to flush all the enqueue offers to be visible globally.
    */
   public final void tryFlushAll() {
      if (flushing.compareAndSet(false, true)) {
         if (reset) {
            for (Stat stat : Stat.values()) {
               topKeyWrapper.get(stat).reset(this, capacity);
            }
            reset = false;
         } else {
            for (Stat stat : Stat.values()) {
               topKeyWrapper.get(stat).flush();
            }
         }
         flushing.set(false);
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StreamSummaryContainer that = (StreamSummaryContainer) o;

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
      return "StreamSummaryContainer{" +
            "cacheName='" + cacheName + '\'' +
            ", address='" + address + '\'' +
            '}';
   }

   private StreamSummary<Object> createNewStreamSummary(int customCapacity) {
      return new StreamSummary<Object>(Math.min(MAX_CAPACITY, customCapacity));
   }

   private void syncOffer(final Stat stat, Object key) {
      if (log.isTraceEnabled()) {
         log.tracef("Offer key=%s to stat=%s in %s", key, stat, this);
      }
      topKeyWrapper.get(stat).offer(key);
      tryFlushAll();
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

   private class TopKeyWrapper {
      private final BlockingQueue<Object> pendingOffers = new LinkedBlockingQueue<Object>();
      private volatile StreamSummary<Object> streamSummary;

      private void offer(final Object element) {
         pendingOffers.add(element);
      }

      private void reset(StreamSummaryContainer container, int capacity) {
         pendingOffers.clear();
         streamSummary = container.createNewStreamSummary(capacity);
      }

      private void flush() {
         List<Object> keys = new ArrayList<Object>();
         pendingOffers.drainTo(keys);
         final StreamSummary<Object> summary = streamSummary;
         for (Object key : keys) {
            synchronized (this) {
               summary.offer(key);
            }
         }
      }

      private Map<Object, Long> topK(int k) {
         List<Counter<Object>> counterList;
         synchronized (this) {
            counterList = streamSummary.topK(k);
         }
         Map<Object, Long> map = new LinkedHashMap<Object, Long>();
         for (Counter<Object> counter : counterList) {
            map.put(counter.getItem(), counter.getCount());
         }
         if (log.isTraceEnabled()) {
            log.tracef(this + " top-k is " + map);
         }
         return map;
      }

      private Map<String, Long> topKAsString(int k) {
         List<Counter<Object>> counterList;
         synchronized (this) {
            counterList = streamSummary.topK(k);
         }
         Map<String, Long> map = new LinkedHashMap<String, Long>();
         for (Counter<Object> counter : counterList) {
            map.put(String.valueOf(counter.getItem()), counter.getCount());
         }
         return map;
      }
   }
}
