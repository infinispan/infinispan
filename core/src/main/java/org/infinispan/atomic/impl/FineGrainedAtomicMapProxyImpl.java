package org.infinispan.atomic.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.atomic.FineGrainedAtomicMap;
import org.infinispan.commons.CacheException;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.Param;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadOnlyMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Implementation of {@link FineGrainedAtomicMap} that uses {@link org.infinispan.distribution.group Grouping API}
 * to co-locate subkeys on the same node. Therefore the entries in this map are held as regular cache entries, but
 * in order to prevent the need for iterating all data in the owning node we also keep a set of keys under the map's
 * key.
 * <p>
 * The implementation requires to be executed on a transactional cache with grouping API enabled. Neither null keys nor
 * null values are supported.
 * <p>
 * This cached set implemented by {@link AtomicKeySetImpl} is accessed using functional API and can be modified without
 * acquiring its lock as long as we modify the same keys in that transaction.
 * <p>
 * Once the map is created or fully read ({@link #size(), {@link #keySet()}, {@link #values()} or {@link #entrySet()}),
 * the whole map (both keys and values) is loaded into context to guarantee repeatable reads semantics. {@link #clear()}
 * removes only those keys that are known - if the map is read, and another transaction adds a key afterwards, such
 * key may not be removed from the map.
 * <p>
 * The map cannot be safely removed (using {@link org.infinispan.atomic.AtomicMapLookup#removeAtomicMap(Cache, Object)}
 * concurrently to another modifications - such operation may result in leaked entries, map being cleared but not
 * removed, failures during commit phase or other undefined behaviour.
 */
public class FineGrainedAtomicMapProxyImpl<K, V, MK> extends AbstractMap<K, V> implements FineGrainedAtomicMap<K, V> {
   private static Log log = LogFactory.getLog(FineGrainedAtomicMap.class);
   private final Cache<Object, Object> cache;
   private final FunctionalMap.ReadOnlyMap<Object, Object> ro;
   private final FunctionalMap.ReadWriteMap<Object, Object> rw;
   private final MK group;
   private final InvocationContextFactory icf;
   private final EntryFactory entryFactory;
   private final TransactionHelper txHelper;

   public FineGrainedAtomicMapProxyImpl(Cache<Object, Object> cache,
                                        FunctionalMap.ReadOnlyMap<Object, Object> ro,
                                        FunctionalMap.ReadWriteMap<Object, Object> rw,
                                        MK group,
                                        InvocationContextFactory icf, EntryFactory entryFactory) {
      this.cache = cache;
      this.ro = ro;
      this.rw = rw;
      this.group = group;
      this.icf = icf;
      this.entryFactory = entryFactory;
      this.txHelper = new TransactionHelper(cache.getAdvancedCache());
   }

   public static <K, V, MK> FineGrainedAtomicMap<K, V> newInstance(Cache<Object, Object> cache, MK group, boolean createIfAbsent) {
      if (!cache.getCacheConfiguration().clustering().hash().groups().enabled()) {
         throw log.atomicFineGrainedNeedsGroups();
      } else if (!cache.getCacheConfiguration().transaction().transactionMode().isTransactional()) {
         throw log.atomicFineGrainedNeedsTransactions();
      }

      FunctionalMapImpl<Object, Object> fmap = FunctionalMapImpl.create(cache.getAdvancedCache());
      FunctionalMap.ReadOnlyMap<Object, Object> ro = ReadOnlyMapImpl.create(fmap);
      FunctionalMap.ReadWriteMap<Object, Object> rw = ReadWriteMapImpl.create(fmap).withParams(Param.LockingMode.SKIP);
      InvocationContextFactory icf = cache.getAdvancedCache().getComponentRegistry().getComponent(InvocationContextFactory.class);
      EntryFactory entryFactory = cache.getAdvancedCache().getComponentRegistry().getComponent(EntryFactory.class);
      // We have to first check if the map is present, because touch acquires a lock on the entry and we don't
      // want to block two concurrent map retrievals with createIfPresent
      // We cannot use plain cache.get() as marshalling of the data type is disabled, therefore we'll load the value
      // using functional command, passing only the keys themselves, and we'll inject the value into context.
      Set<K> keys = wait(ro.eval(group, AtomicKeySetImpl.ReadAll.<K>instance()));
      if (keys != null) {
         InvocationContext ctx = icf.createInvocationContext(false, 1);
         if (ctx.isInTxScope() && ctx.lookupEntry(group) == null) {
            CacheEntry<MK, Object> entry = new ImmortalCacheEntry(group, AtomicKeySetImpl.create(cache.getName(), group, keys));
            entryFactory.wrapExternalEntry(ctx, group, entry, true, false);
         }
         return new FineGrainedAtomicMapProxyImpl<>(cache, ro, rw, group, icf, entryFactory);
      } else if (createIfAbsent) {
         wait(rw.eval(group, new AtomicKeySetImpl.Touch(ByteString.fromString(cache.getName()))));
         return new FineGrainedAtomicMapProxyImpl<>(cache, ro, rw, group, icf, entryFactory);
      } else {
         return null;
      }
   }

   /**
    * Warning: with pessimistic locking/optimistic locking without WSC, when the map is removed and a new key is added
    * before the removal transaction commit, the map may be removed but the key left dangling.
    */
   public static void removeMap(Cache<Object, Object> cache, Object group) {
      FunctionalMapImpl<Object, Object> fmap = FunctionalMapImpl.create(cache.getAdvancedCache());
      FunctionalMap.ReadWriteMap<Object, Object> rw = ReadWriteMapImpl.create(fmap).withParams(Param.LockingMode.SKIP);
      new TransactionHelper(cache.getAdvancedCache()).run(() -> {
         Set<Object> keys = wait(rw.eval(group, AtomicKeySetImpl.RemoveMap.instance()));
         if (keys != null) {
            removeAll(cache, group, keys);
         }
         return null;
      });
   }

   private static <T> T wait(CompletableFuture<? extends T> cf) {
      try {
         return cf.join();
      } catch (CompletionException ce) {
         Throwable e = ce.getCause();
         while (e instanceof CacheException && e.getCause() != null) {
            e = e.getCause();
         }
         if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
         } else {
            throw ce; // we would wrap that anyway
         }
      }
   }

   private static <K> void removeAll(Cache<Object, Object> cache, Object group, Set<K> keys) {
      ArrayList<CompletableFuture<?>> cfs = new ArrayList<>(keys.size());
      Cache<Object, Object> noReturn = cache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
      for (K key : keys) {
         cfs.add(noReturn.removeAsync(new AtomicKeySetImpl.Key<>(group, key)));
      }
      wait(CompletableFuture.allOf(cfs.toArray(new CompletableFuture[cfs.size()])));
   }

   private Set<K> keys() {
      Set<K> keys = wait(ro.eval(group, AtomicKeySetImpl.ReadAll.instance()));
      if (keys == null) {
         throw log.atomicMapDoesNotExist();
      } else {
         InvocationContext ctx = icf.createInvocationContext(false, 1);
         if (ctx.isInTxScope() && ctx.lookupEntry(group) == null) {
            CacheEntry<MK, Collection<K>> entry = new ImmortalCacheEntry(group, AtomicKeySetImpl.create(cache.getName(), group, keys));
            entryFactory.wrapExternalEntry(ctx, group, entry, true, false);
         }
         return keys;
      }
   }

   private Set<AtomicKeySetImpl.Key<MK, K>> atomicKeys(Set<K> keys) {
      return keys.stream().map(k -> new AtomicKeySetImpl.Key<>(group, k)).collect(Collectors.toSet());
   }

   @Override
   public int size() {
      return keySet().size();
   }

   @Override
   public boolean containsKey(Object key) {
      return cache.containsKey(new AtomicKeySetImpl.Key<>(group, key));
   }

   @Override
   public V get(Object key) {
      return (V) cache.get(new AtomicKeySetImpl.Key<>(group, key));
   }

   @Override
   public V put(K key, V value) {
      return txHelper.run(() -> {
         V prev = (V) cache.put(new AtomicKeySetImpl.Key<>(group, key), value);
         wait(rw.eval(group, new AtomicKeySetImpl.Add<>(key)));
         return prev;
      });
   }

   @Override
   public V remove(Object key) {
      return txHelper.run(() -> {
         V prev = (V) cache.remove(new AtomicKeySetImpl.Key<>(group, key));
         wait(rw.eval(group, new AtomicKeySetImpl.Remove<>(key)));
         return prev;
      });
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      txHelper.run(() -> {
         cache.putAll(m.entrySet().stream().collect(Collectors.toMap(e -> new AtomicKeySetImpl.Key<>(group, e.getKey()), Map.Entry::getValue)));
         wait(rw.eval(group, new AtomicKeySetImpl.AddAll<>(m.keySet())));
         return null;
      });
   }

   @Override
   public void clear() {
      txHelper.run(() -> {
         Set<K> keys = wait(rw.eval(group, AtomicKeySetImpl.RemoveAll.<K>instance()));
         removeAll(cache, group, keys);
         return null;
      });
   }

   @Override
   public Set<K> keySet() {
      Set<K> keys = keys();
      if (keys.isEmpty()) {
         return Collections.emptySet();
      }
      // TODO: if the isolation level is read committed, we don't have to check values for null
      Stream<Entry<Object, Object>> entryStream = cache.getAdvancedCache().getAll(atomicKeys(keys)).entrySet().stream();
      return entryStream.filter(e -> e.getValue() != null).map(e -> ((AtomicKeySetImpl.Key<MK, K>) e.getKey()).key()).collect(Collectors.toSet());
   }

   @Override
   public Collection<V> values() {
      Set<K> keys = keys();
      if (keys.isEmpty()) {
         return Collections.emptyList();
      }
      Stream<Object> valuesStream = cache.getAdvancedCache().getAll(atomicKeys(keys)).values().stream().filter(Objects::nonNull);
      return ((Stream<V>) valuesStream).collect(Collectors.toList());
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      Set<K> keys = keys();
      if (keys.isEmpty()) {
         return Collections.emptySet();
      }
      Stream<Entry<Object, Object>> entryStream = cache.getAdvancedCache().getAll(atomicKeys(keys)).entrySet().stream().filter(e -> e.getValue() != null);
      return entryStream.collect(Collectors.toMap(e -> ((AtomicKeySetImpl.Key<MK, K>) e.getKey()).key(), e1 -> (V) e1.getValue())).entrySet();
   }

   @Override
   public String toString() {
      return "FineGrainedAtomicMapProxyImpl{group=" + group + "}";
   }
}
