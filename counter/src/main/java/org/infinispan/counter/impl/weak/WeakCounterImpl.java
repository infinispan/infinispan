package org.infinispan.counter.impl.weak;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.function.AddFunction;
import org.infinispan.counter.impl.function.InitializeCounterFunction;
import org.infinispan.counter.impl.function.ResetFunction;
import org.infinispan.counter.impl.listener.CounterEventImpl;
import org.infinispan.counter.impl.listener.CounterFilterAndConvert;
import org.infinispan.counter.impl.listener.NotificationManager;
import org.infinispan.counter.logging.Log;
import org.infinispan.counter.util.Utils;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

/**
 * A weak consistent counter implementation.
 * <p>
 * Implementation: The counter is split in multiple keys and they are stored in the cache.
 * <p>
 * Write: A write operation will pick a key to update. If the node is a primary owner of one of the key, that key is
 * chosen based on thread-id. This will take advantage of faster write operations. If the node is not a primary owner,
 * one of the key in key set is chosen.
 * <p>
 * Read: A read operation needs to read all the key set (including the remote keys). This is slower than atomic
 * counter.
 * <p>
 * Weak Read: A snapshot of all the keys values is kept locally and they are updated via cluster listeners.
 * <p>
 * Reset: The reset operation is <b>not</b> atomic and intermediate results may be observed.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Listener(clustered = true, observation = Listener.Observation.POST, sync = true)
public class WeakCounterImpl implements WeakCounter {

   private static final Log log = LogFactory.getLog(WeakCounterImpl.class, Log.class);
   private static final AtomicReferenceFieldUpdater<Entry, CounterValue> L1_UPDATER =
         newUpdater(Entry.class, CounterValue.class, "snapshot");

   private final Entry[] entries;
   private final AdvancedCache<WeakCounterKey, CounterValue> cache;
   private final FunctionalMap.ReadWriteMap<WeakCounterKey, CounterValue> readWriteMap;
   private final NotificationManager notificationManager;
   private final CounterConfiguration configuration;
   private final KeySelector selector;

   public WeakCounterImpl(String counterName, AdvancedCache<WeakCounterKey, CounterValue> cache,
         CounterConfiguration configuration) {
      this.cache = cache;
      FunctionalMapImpl<WeakCounterKey, CounterValue> functionalMap = FunctionalMapImpl.create(cache)
            .withParams(Utils.getPersistenceMode(configuration.storage()));
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entries = initKeys(counterName, configuration.concurrencyLevel());
      this.selector = new KeySelector(entries);
      this.notificationManager = new NotificationManager();
      this.configuration = configuration;
   }

   private static <T> T get(int hash, T[] array) {
      return array[hash & (array.length - 1)];
   }

   private static Entry[] initKeys(String counterName, int concurrencyLevel) {
      ByteString name = ByteString.fromString(counterName);
      int size = Util.findNextHighestPowerOfTwo(concurrencyLevel);
      Entry[] entries = new Entry[size];
      for (int i = 0; i < size; ++i) {
         entries[i] = new Entry(new WeakCounterKey(name, i));
      }
      return entries;
   }

   /**
    * Initializes the key set.
    * <p>
    * Only one key will have the initial value and the remaining is zero.
    */
   public void init() {
      registerListener();
      initEntry(0, configuration);
      CounterConfiguration zeroConfig = CounterConfiguration.builder(CounterType.WEAK).initialValue(0)
            .storage(configuration.storage()).build();
      for (int i = 1; i < entries.length; ++i) {
         initEntry(i, zeroConfig);
      }
      selector.updatePreferredKeys(cache.getDistributionManager().getWriteConsistentHash());
   }

   private void initEntry(int index, CounterConfiguration configuration) {
      try {
         CounterValue existing = readWriteMap.eval(entries[index].key, new InitializeCounterFunction<>(configuration))
               .get();
         entries[index].init(existing);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CounterException(e);
      } catch (ExecutionException e) {
         throw Utils.rethrowAsCounterException(e);
      }

   }

   @Override
   public String getName() {
      return entries[0].key.getCounterName().toString();
   }

   @Override
   public long getValue() {
      return getCachedValue();
   }

   @Override
   public CompletableFuture<Void> add(long delta) {
      return readWriteMap.eval(findKey(), new AddFunction<>(delta)).thenApply(this::handleAddResult);
   }

   @Override
   public CompletableFuture<Void> reset() {
      final int size = entries.length;
      CompletableFuture[] futures = new CompletableFuture[size];
      for (int i = 0; i < size; ++i) {
         futures[i] = readWriteMap.eval(entries[i].key, ResetFunction.getInstance());
      }
      return CompletableFuture.allOf(futures);
   }

   @Override
   public <T extends CounterListener> Handle<T> addListener(T listener) {
      return notificationManager.addListener(listener);
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   @CacheEntryModified
   public void updateState(CacheEntryEvent<WeakCounterKey, CounterValue> event) {
      int index = event.getKey().getIndex();
      long base = getCachedValue(index);
      CounterValue snapshot = event.getValue();
      CounterValue old = updateCounterState(index, snapshot);
      notificationManager.notify(CounterEventImpl.create(base + old.getValue(), base + snapshot.getValue()));
   }


   /**
    * Debug only!
    */
   public WeakCounterKey[] getPreferredKeys() {
      return selector.preferredKeys;
   }

   /**
    * Debug only!
    */
   public WeakCounterKey[] getKeys() {
      WeakCounterKey[] keys = new WeakCounterKey[entries.length];
      for (int i = 0; i < keys.length; ++i) {
         keys[i] = entries[i].key;
      }
      return keys;
   }

   private long getCachedValue() {
      long value = 0;
      for (Entry e : entries) {
         long toAdd = e.snapshot.getValue();
         try {
            value = Math.addExact(value, toAdd);
         } catch (ArithmeticException ex) {
            return toAdd > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
         }
      }
      return value;
   }

   private long getCachedValue(int skipIndex) {
      long value = 0;
      for (int i = 0; i < entries.length; ++i) {
         if (i != skipIndex) {
            value += entries[i].snapshot.getValue();
         }
      }
      return value;
   }

   private CounterValue updateCounterState(int index, CounterValue entry) {
      return entries[index].update(entry);
   }

   private Void handleAddResult(CounterValue counterValue) {
      if (counterValue == null) {
         throw new CompletionException(log.counterDeleted());
      }
      return null;
   }

   private void registerListener() {
      CounterFilterAndConvert<WeakCounterKey> filter = new CounterFilterAndConvert<>(entries[0].key.getCounterName());
      cache.addListener(this, filter, filter);
      cache.addListener(selector);
   }

   private WeakCounterKey findKey() {
      return selector.findKey((int) Thread.currentThread().getId());
   }

   @Override
   public String toString() {
      return "UnboundedStrongCounter{" +
            "counterName=" + entries[0].key.getCounterName() +
            '}';
   }

   private static class Entry {
      public final WeakCounterKey key;
      volatile CounterValue snapshot = null;

      private Entry(WeakCounterKey key) {
         this.key = key;
      }

      private void init(CounterValue entry) {
         L1_UPDATER.compareAndSet(this, null, entry);
      }

      private CounterValue update(CounterValue entry) {
         return L1_UPDATER.getAndSet(this, entry);
      }
   }

   @Listener(sync = false)
   private class KeySelector {
      private final Entry[] entries;
      private volatile WeakCounterKey[] preferredKeys; //null when no keys available

      private KeySelector(Entry[] entries) {
         this.entries = entries;
      }

      private WeakCounterKey findKey(int hash) {
         WeakCounterKey[] copy = preferredKeys;
         if (copy == null) {
            return get(hash, entries).key;
         } else if (copy.length == 1) {
            return copy[0];
         } else {
            return get(hash, copy);
         }
      }

      private void updatePreferredKeys(ConsistentHash consistentHash) {
         ArrayList<WeakCounterKey> preferredKeys = new ArrayList<>(entries.length);
         Address localNode = cache.getRpcManager().getAddress();
         for (Entry entry : entries) {
            if (localNode.equals(consistentHash.locatePrimaryOwner(entry.key))) {
               preferredKeys.add(entry.key);
            }
         }
         this.preferredKeys = preferredKeys.isEmpty() ?
               null :
               preferredKeys.toArray(new WeakCounterKey[preferredKeys.size()]);
      }

      @TopologyChanged
      public void topologyChanged(TopologyChangedEvent<WeakCounterKey, CounterValue> event) {
         updatePreferredKeys(event.getWriteConsistentHashAtEnd());
      }

   }
}
