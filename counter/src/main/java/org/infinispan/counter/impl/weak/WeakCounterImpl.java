package org.infinispan.counter.impl.weak;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static org.infinispan.counter.impl.Util.awaitCounterOperation;
import static org.infinispan.counter.util.Utils.getPersistenceMode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Handle;
import org.infinispan.counter.api.SyncWeakCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.SyncWeakCounterAdapter;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.function.AddFunction;
import org.infinispan.counter.impl.function.CreateAndAddFunction;
import org.infinispan.counter.impl.function.ReadFunction;
import org.infinispan.counter.impl.function.RemoveFunction;
import org.infinispan.counter.impl.function.ResetFunction;
import org.infinispan.counter.impl.listener.CounterEventGenerator;
import org.infinispan.counter.impl.listener.CounterEventImpl;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.counter.impl.listener.TopologyChangeListener;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;

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
public class WeakCounterImpl implements WeakCounter, CounterEventGenerator, TopologyChangeListener {

   private static final AtomicReferenceFieldUpdater<Entry, Long> L1_UPDATER =
         newUpdater(Entry.class, Long.class, "snapshot");

   private final Entry[] entries;
   private final AdvancedCache<WeakCounterKey, CounterValue> cache;
   private final FunctionalMap.ReadWriteMap<WeakCounterKey, CounterValue> readWriteMap;
   private final CounterManagerNotificationManager notificationManager;
   private final FunctionalMap.ReadOnlyMap<WeakCounterKey, CounterValue> readOnlyMap;
   private final CounterConfiguration configuration;
   private final CounterConfiguration zeroConfiguration;
   private final KeySelector selector;

   public WeakCounterImpl(String counterName, AdvancedCache<WeakCounterKey, CounterValue> cache,
         CounterConfiguration configuration, CounterManagerNotificationManager notificationManager) {
      this.cache = cache;
      this.notificationManager = notificationManager;
      FunctionalMapImpl<WeakCounterKey, CounterValue> functionalMap = FunctionalMapImpl.create(cache)
            .withParams(getPersistenceMode(configuration.storage()));
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.readOnlyMap = ReadOnlyMapImpl.create(functionalMap);
      this.entries = initKeys(counterName, configuration.concurrencyLevel());
      this.selector = new KeySelector(entries);
      this.configuration = configuration;
      this.zeroConfiguration = CounterConfiguration.builder(CounterType.WEAK)
            .concurrencyLevel(configuration.concurrencyLevel()).storage(configuration.storage()).initialValue(0)
            .build();
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
    * It removes a weak counter from the {@code cache}, identified by the {@code counterName}.
    *
    * @param cache         The {@link Cache} to remove the counter from.
    * @param configuration The counter's configuration.
    * @param counterName   The counter's name.
    */
   public static void removeWeakCounter(Cache<WeakCounterKey, CounterValue> cache, CounterConfiguration configuration,
         String counterName) {
      ByteString counterNameByteString = ByteString.fromString(counterName);
      for (int i = 0; i < configuration.concurrencyLevel(); ++i) {
         cache.remove(new WeakCounterKey(counterNameByteString, i));
      }
   }

   /**
    * Initializes the key set.
    * <p>
    * Only one key will have the initial value and the remaining is zero.
    */
   public void init() {
      registerListener();
      for (int i = 0; i < entries.length; ++i) {
         final int index = i;
         awaitCounterOperation(readOnlyMap.eval(entries[index].key, ReadFunction.getInstance())
               .thenAccept(value -> initEntry(index, value)));
      }
      selector.updatePreferredKeys();
   }

   @Override
   public String getName() {
      return counterName().toString();
   }

   @Override
   public long getValue() {
      return getCachedValue();
   }

   @Override
   public CompletableFuture<Void> add(long delta) {
      WeakCounterKey key = findKey();
      return readWriteMap.eval(key, new AddFunction<>(delta))
            .thenCompose(counterValue -> handleAddResult(key, counterValue, delta));
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
      return notificationManager.registerUserListener(counterName(), listener);
   }

   @Override
   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   @Override
   public CounterEvent generate(CounterKey key, CounterValue value) {
      assert key instanceof WeakCounterKey;
      int index = ((WeakCounterKey) key).getIndex();
      long base = getCachedValue(index);
      long newValue = value == null ?
            (index == 0 ? configuration.initialValue() : 0) :
            value.getValue();
      long oldValue = updateCounterState(index, newValue);
      return CounterEventImpl.create(base + oldValue, base + newValue);
   }


   @Override
   public CompletableFuture<Void> remove() {
      final int size = entries.length;
      CompletableFuture[] futures = new CompletableFuture[size];
      for (int i = 0; i < size; ++i) {
         futures[i] = readWriteMap.eval(entries[i].key, RemoveFunction.getInstance());
      }
      return CompletableFuture.allOf(futures);
   }

   @Override
   public SyncWeakCounter sync() {
      return new SyncWeakCounterAdapter(this);
   }

   public void destroyAndRemove() {
      removeListener();
      awaitCounterOperation(remove());
   }

   @Override
   public void topologyChanged() {
      selector.updatePreferredKeys();
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

   @Override
   public String toString() {
      return "WeakCounter{" +
            "counterName=" + counterName() +
            '}';
   }

   private void initEntry(int index, Long value) {
      if (value == null) {
         value = index == 0 ? configuration.initialValue() : 0;
      }
      entries[index].init(value);
   }

   private long getCachedValue() {
      long value = 0;
      int index = 0;
      try {
         for (; index < entries.length; ++index) {
            value = Math.addExact(value, entries[index].snapshot);
         }
      } catch (ArithmeticException e) {
         return getCachedValue0(index, value, -1);
      }
      return value;
   }

   private long getCachedValue(int skipIndex) {
      long value = 0;
      int index = 0;
      try {
         for (; index < entries.length; ++index) {
            if (index == skipIndex) {
               continue;
            }
            value = Math.addExact(value, entries[index].snapshot);
         }
      } catch (ArithmeticException e) {
         return getCachedValue0(index, value, skipIndex);
      }
      return value;
   }

   private long getCachedValue0(int index, long value, int skipIndex) {
      BigInteger currentValue = BigInteger.valueOf(value);
      do {
         currentValue = currentValue.add(BigInteger.valueOf(entries[index++].snapshot));
         if (index == skipIndex) {
            index++;
         }
      } while (index < entries.length);
      try {
         return currentValue.longValue();
      } catch (ArithmeticException e) {
         return currentValue.signum() > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
      }
   }

   private long updateCounterState(int index, long newValue) {
      return entries[index].update(newValue);
   }

   private CompletableFuture<Void> handleAddResult(WeakCounterKey key, CounterValue value, long delta) {
      if (value == null) {
         //first time
         CounterConfiguration createConfiguration = key.getIndex() == 0 ?
               configuration :
               zeroConfiguration;
         return readWriteMap.eval(key, new CreateAndAddFunction<>(createConfiguration, delta))
               .thenApply(value1 -> null);
      } else {
         return CompletableFutures.completedNull();
      }
   }

   private void registerListener() {
      notificationManager.registerCounter(counterName(), this, this);
   }

   private void removeListener() {
      notificationManager.removeCounter(counterName());
   }

   private WeakCounterKey findKey() {
      return selector.findKey((int) Thread.currentThread().getId());
   }

   private ByteString counterName() {
      return entries[0].key.getCounterName();
   }

   private static class Entry {
      final WeakCounterKey key;
      @SuppressWarnings("unused")
      volatile Long snapshot;

      private Entry(WeakCounterKey key) {
         this.key = key;
      }

      private void init(long initialValue) {
         L1_UPDATER.compareAndSet(this, null, initialValue);
      }

      private long update(long value) {
         return L1_UPDATER.getAndSet(this, value);
      }
   }

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

      private void updatePreferredKeys() {
         ArrayList<WeakCounterKey> preferredKeys = new ArrayList<>(entries.length);
         LocalizedCacheTopology topology = cache.getDistributionManager().getCacheTopology();
         for (Entry entry : entries) {
            if (topology.getDistribution(entry.key).isPrimary()) {
               preferredKeys.add(entry.key);
            }
         }
         this.preferredKeys = preferredKeys.isEmpty() ?
               null :
               preferredKeys.toArray(new WeakCounterKey[preferredKeys.size()]);
      }
   }
}
