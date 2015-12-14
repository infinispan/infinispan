package org.infinispan.atomic.impl;

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.atomic.AtomicMapLookup;
import org.infinispan.atomic.CopyableDeltaAware;
import org.infinispan.atomic.Delta;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The default implementation of {@link AtomicMap}.  Note that this map cannot be constructed directly, and callers
 * should obtain references to AtomicHashMaps via the {@link AtomicMapLookup} helper.  This helper will ensure proper
 * concurrent construction and registration of AtomicMaps in Infinispan's data container.  E.g.:
 * <br /><br />
 * <code>
 *    AtomicMap&lt;String, Integer&gt; map = AtomicMapLookup.getAtomicMap(cache, "my_atomic_map_key");
 * </code>
 * <br /><br />
 * Note that for replication to work properly, AtomicHashMap updates <b><i>must always</i></b> take place within the
 * scope of an ongoing JTA transaction or batch (see {@link Cache#startBatch()}).
 * <p/>
 *
 * @author (various)
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @see AtomicMap
 * @see AtomicMapLookup
 * @since 4.0
 */
@NotThreadSafe
public final class AtomicHashMap<K, V> implements AtomicMap<K, V>, CopyableDeltaAware, Cloneable {

   private static final Log log = LogFactory.getLog(AtomicHashMap.class);
   private static final boolean trace = log.isTraceEnabled();

   protected final FastCopyHashMap<K, V> delegate;
   private AtomicHashMapDelta delta = null;
   private volatile AtomicHashMapProxy<K, V> proxy;
   volatile boolean copied = false;
   volatile boolean removed = false;
   private final ProxyMode proxyMode;

   /**
    * Construction only allowed through this factory method.  This factory is intended for use internally by the
    * CacheDelegate.  User code should use {@link AtomicMapLookup#getAtomicMap(Cache, Object)}.
    */
   @SuppressWarnings("unchecked")
   public static <K, V> AtomicHashMap<K, V> newInstance(Cache<Object, Object> cache, Object cacheKey, ProxyMode proxyMode) {
      AtomicHashMap<K, V> value = new AtomicHashMap<>(proxyMode);
      Object oldValue = cache.putIfAbsent(cacheKey, value);
      if (oldValue != null) value = (AtomicHashMap<K, V>) oldValue;
      return value;
   }

   //used in tests only
   public AtomicHashMap() {
      this(new FastCopyHashMap<>(), ProxyMode.COARSE);
   }

   public AtomicHashMap(ProxyMode proxyMode) {
      this(new FastCopyHashMap<>(), Objects.requireNonNull(proxyMode));
   }

   private AtomicHashMap(FastCopyHashMap<K, V> delegate, ProxyMode proxyMode) {
      this.delegate = delegate;
      this.proxyMode = proxyMode;
   }

   public AtomicHashMap(boolean isCopy, ProxyMode proxyMode) {
      this(new FastCopyHashMap<>(), proxyMode);
      this.copied = isCopy;
   }

   private AtomicHashMap(FastCopyHashMap<K, V> newDelegate, AtomicHashMapProxy<K, V> proxy, ProxyMode proxyMode) {
      this.delegate = newDelegate;
      this.proxy = proxy;
      this.copied = true;
      this.proxyMode = proxyMode;
   }

   @Override
   public void commit() {
      copied = false;
      delta = null;
   }

   @Override
   public int size() {
      return delegate.size();
   }

   @Override
   public boolean isEmpty() {
      return delegate.isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return delegate.containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return delegate.containsValue(value);
   }

   @Override
   public V get(Object key) {
      V v = delegate.get(key);
      if (trace)
         log.tracef("Atomic hash map get(key=%s) returns %s", key, v);

      return v;
   }

   @Override
   public Set<K> keySet() {
      return delegate.keySet();
   }

   @Override
   public Collection<V> values() {
      return delegate.values();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return delegate.entrySet();
   }

   @Override
   public V put(K key, V value) {
      V oldValue = delegate.put(key, value);
      PutOperation<K, V> op = new PutOperation<>(key, oldValue, value);
      getDelta().addOperation(op);
      return oldValue;
   }

   @Override
   @SuppressWarnings("unchecked")
   public V remove(Object key) {
      V oldValue = delegate.remove(key);
      RemoveOperation<K, V> op = new RemoveOperation<>((K) key, oldValue);
      getDelta().addOperation(op);
      return oldValue;
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> t) {
      // this is crappy - need to do this more efficiently!
      for (Entry<? extends K, ? extends V> e : t.entrySet()) put(e.getKey(), e.getValue());
   }

   @Override
   @SuppressWarnings("unchecked")
   public void clear() {
      FastCopyHashMap<K, V> originalEntries = delegate.clone();
      ClearOperation<K, V> op = new ClearOperation<>(originalEntries);
      getDelta().addOperation(op);
      delegate.clear();
   }

   /**
    * Builds a thread-safe proxy for this instance so that concurrent reads are isolated from writes.
    * @return an instance of AtomicHashMapProxy
    */
   public AtomicHashMapProxy<K, V> getProxy(AdvancedCache<Object, Object> cache, Object mapKey) {
      // construct the proxy lazily
      if (proxy == null)  // DCL is OK here since proxy is volatile (and we live in a post-JDK 5 world)
      {
         synchronized (this) {
            if (proxy == null)
               switch (proxyMode) {
                  case FINE:
                     proxy = new FineGrainedAtomicHashMapProxy<>(cache, mapKey);
                     break;
                  case COARSE:
                     proxy = new AtomicHashMapProxy<>(cache, mapKey);
                     break;
                  default:
                     throw new IllegalStateException("Unknown proxy mode: " + proxyMode);
               }
         }
      }
      return proxy;
   }

   public void markRemoved(boolean b) {
      removed = b;
   }

   @Override
   public Delta delta() {
      Delta toReturn = delta == null ? new AtomicHashMapDelta(proxyMode) : delta;
      delta = null; // reset
      return toReturn;
   }

   @SuppressWarnings("unchecked")
   public AtomicHashMap<K, V> copy() {
      FastCopyHashMap<K, V> newDelegate = delegate.clone();
      return new AtomicHashMap(newDelegate, proxy, proxyMode);
   }

   @Override
   public String toString() {
      // Sanne: Avoid iterating on the delegate as that might lead to
      // exceptions from concurrent iterators: not nice to have during a toString!
      //
      // Galder: Sure, but we need a way to track the contents of the atomic
      // hash map somehow, so, we need to log each operation that affects its
      // contents, and when its state is restored.
      return "AtomicHashMap{size=" + size() + "}";
   }

   /**
    * Initializes the delta instance to start recording changes.
    */
   public void initForWriting() {
      delta = new AtomicHashMapDelta(proxyMode);
   }

   AtomicHashMapDelta getDelta() {
      if (delta == null) delta = new AtomicHashMapDelta(proxyMode);
      return delta;
   }

   public static class Externalizer extends AbstractExternalizer<AtomicHashMap> {
      @Override
      public void writeObject(ObjectOutput output, AtomicHashMap map) throws IOException {
         output.writeObject(map.delegate);
         output.writeByte(map.proxyMode.ordinal());
      }

      @Override
      @SuppressWarnings("unchecked")
      public AtomicHashMap readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         FastCopyHashMap<?, ?> delegate = (FastCopyHashMap<?, ?>) input.readObject();
         ProxyMode proxyMode = ProxyMode.CACHED_VALUES[input.readByte()];
         if (trace)
            log.tracef("Restore atomic hash map from %s", delegate);

         return new AtomicHashMap(delegate, proxyMode);
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_HASH_MAP;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends AtomicHashMap>> getTypeClasses() {
         return Util.<Class<? extends AtomicHashMap>>asSet(AtomicHashMap.class);
      }
   }

   public enum ProxyMode {
      FINE, COARSE;
      private static final ProxyMode[] CACHED_VALUES = values();

      public static ProxyMode valueOf(int ordinal) {
         return CACHED_VALUES[ordinal];
      }
   }
}
