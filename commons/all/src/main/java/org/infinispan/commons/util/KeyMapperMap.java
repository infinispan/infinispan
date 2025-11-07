package org.infinispan.commons.util;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A ConcurrentMap that wraps another ConcurrentMap, converting the key type on the fly. This Map requires both
 * key mapping and unmapping which allows for it to support both and read operations for all of its direct methods.
 * This Map, however, does not support insertions into its {@link #keySet()} or {@link #values()} collections as they
 * are read only!
 * <p>
 * This class currently only accepts {@link Function}s that also implements {@link InjectiveFunction} so that they can
 * guarantee the resulting mapped values are distinct from each other.  This is important as many operations become
 * very costly if this is not true.
 *
 * @param <K> the key type of the delegate map
 * @param <R> the key type of this map
 * @param <V> the value type
 */
public class KeyMapperMap<K, R, V> extends AbstractMap<R, V> implements ConcurrentMap<R, V> {

   private final Map<K, V> delegate;
   private final InjectiveFunction<? super K, ? extends R> keyMapper;
   private final InjectiveFunction<? super R, ? extends K> keyUnmapper;

   public KeyMapperMap(Map<K, V> delegate, Function<? super K, ? extends R> keyMapper, Function<? super R, ? extends K> keyUnmapper) {
      this.delegate = delegate;
      if (!(keyMapper instanceof InjectiveFunction)) {
         throw new IllegalArgumentException("keyMapper function must also provided distinct values as evidenced by implementing" +
               "the marker interface InjectiveFunction");
      }
      this.keyMapper = (InjectiveFunction<? super K, ? extends R>) keyMapper;
      if (!(keyUnmapper instanceof InjectiveFunction)) {
         throw new IllegalArgumentException("keyUnmapper function must also provided distinct values as evidenced by implementing" +
               "the marker interface InjectiveFunction");
      }
      this.keyUnmapper = (InjectiveFunction<? super R, ? extends K>) keyUnmapper;
   }

   @Override
   public V get(Object key) {
      K delegateKey = keyUnmapper.apply((R) key);
      return delegate.get(delegateKey);
   }

   @Override
   public V put(R key, V value) {
      K delegateKey = keyUnmapper.apply(key);
      return delegate.put(delegateKey, value);
   }

   @Override
   public V remove(Object key) {
      K delegateKey = keyUnmapper.apply((R) key);
      return delegate.remove(delegateKey);
   }

   @Override
   public boolean containsKey(Object key) {
      K delegateKey = keyUnmapper.apply((R) key);
      return delegate.containsKey(delegateKey);
   }

   @Override
   public V putIfAbsent(R key, V value) {
      K delegateKey = keyUnmapper.apply(key);
      return delegate.putIfAbsent(delegateKey, value);
   }

   @Override
   public boolean remove(Object key, Object value) {
      K delegateKey = keyUnmapper.apply((R) key);
      return delegate.remove(delegateKey, value);
   }

   @Override
   public boolean replace(R key, V oldValue, V newValue) {
      K delegateKey = keyUnmapper.apply(key);
      return delegate.replace(delegateKey, oldValue, newValue);
   }

   @Override
   public V replace(R key, V value) {
      K delegateKey = keyUnmapper.apply(key);
      return delegate.replace(delegateKey, value);
   }

   @Override
   public Set<Entry<R, V>> entrySet() {
      return new SetMapper<>(delegate.entrySet(), e -> new AbstractMap.SimpleEntry<>(keyMapper.apply(e.getKey()), e.getValue()));
   }

   @Override
   public void putAll(Map<? extends R, ? extends V> m) {
      Map<R, V> providedMap = (Map) m;
      Map<? extends K, ? extends V> map = new KeyMapperMap<>(providedMap, keyUnmapper, keyMapper);
      delegate.putAll(map);
   }

   @Override
   public void replaceAll(BiFunction<? super R, ? super V, ? extends V> function) {
      delegate.replaceAll((k, v) -> function.apply(keyMapper.apply(k), v));
   }

   @Override
   public V computeIfAbsent(R key, Function<? super R, ? extends V> mappingFunction) {
      return delegate.computeIfAbsent(keyUnmapper.apply(key), k -> mappingFunction.apply(keyMapper.apply(k)));
   }

   @Override
   public V computeIfPresent(R key, BiFunction<? super R, ? super V, ? extends V> remappingFunction) {
      return delegate.computeIfPresent(keyUnmapper.apply(key), (k, v) -> remappingFunction.apply(keyMapper.apply(k), v));
   }

   @Override
   public V compute(R key, BiFunction<? super R, ? super V, ? extends V> remappingFunction) {
      return delegate.compute(keyUnmapper.apply(key), (k, v) -> remappingFunction.apply(keyMapper.apply(k), v));
   }

   @Override
   public V merge(R key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return delegate.merge(keyUnmapper.apply(key), value, remappingFunction);
   }

   @Override
   public Set<R> keySet() {
      Set<K> keySet = delegate.keySet();
      return new SetMapper<>(keySet, keyMapper);
   }

   @Override
   public void clear() {
      delegate.clear();
   }


}
