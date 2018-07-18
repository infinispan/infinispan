package org.infinispan.atomic.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.atomic.AtomicMap;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This is the proxy class driving access to the entry in cache using functional API. The value in cache is a map
 * of type defined by {@link #dataClass()} which is accompanied by {@link #dataInstance()} and {@link #dataCopy(Map)}.
 * Currently it is implemented by {@link FastCopyHashMap} as we need to create a defensive copy of the value in context.
 *
 * The map is expected to be used with transactional caches; upon map creation or read, the whole map is cached
 * in the transactional context and subsequent reads are served from here. When the map is modified, the change is
 * applied to the local copy in the context. Modifications are added in the form of functional commands to the transaction
 * and executed opon commit.
 *
 * In non-transactional mode this map works correctly, but since any read results in retrieving the whole map, its use
 * might not be very efficient. This behaviour may be addressed in the future.
 *
 * Collections returned by {@link #keySet()}, {@link #values()} and {@link #entrySet()} are backed by the map.
 * Iterators work on a snapshot obtained at the moment when this iterator is created, and support
 * the {@link Iterator#remove()} operation.
 *
 * This implementation does not support fine-grained locking nor fine-grained write-skew check.
 *
 * @param <K>
 * @param <V>
 * @param <MK>
 */
public class AtomicMapProxyImpl<K, V, MK> implements AtomicMap<K, V> {
   private enum EF {
      TOUCH, PUT, REMOVE, PUT_ALL, CLEAR, REMOVE_VALUE, REMOVE_COND
   }
   private static final EF[] FUNCTIONS = EF.values();
   private static Log log = LogFactory.getLog(AtomicMap.class);
   private final Cache<MK, Object> cache;
   private final FunctionalMap.ReadWriteMap<MK, Object> rw;
   private final MK key;

   public AtomicMapProxyImpl(Cache<MK, Object> cache, FunctionalMap.ReadWriteMap<MK, Object> rw, MK key) {
      this.cache = cache;
      this.rw = rw;
      this.key = key;
   }

   public static <K, V, MK> AtomicMap<K, V> newInstance(Cache<MK, Object> cache, MK key, boolean createIfAbsent) {
      FunctionalMapImpl<MK, Object> fmap = FunctionalMapImpl.create(cache.getAdvancedCache());
      FunctionalMap.ReadWriteMap<MK, Object> rw = ReadWriteMapImpl.create(fmap);
      // We have to first check if the map is present, because touch acquires a lock on the entry and we don't
      // want to block two concurrent map retrievals with createIfPresent
      // Here we do common get() to put the cache into context if we're in transactional cache
      Object value = cache.get(key);
      if (value != null) {
         if (value.getClass() != dataClass()) {
            throw log.atomicMapHasWrongType(value, dataClass());
         }
         return new AtomicMapProxyImpl<>(cache, rw, key);
      } else if (createIfAbsent) {
         wait(rw.eval(key, Touch.instance()));
         return new AtomicMapProxyImpl<>(cache, rw, key);
      } else {
         return null;
      }
   }

   private static Class<? extends Map> dataClass() {
      // TODO: once we implement fine grained, we'll have to distinguish the implementation, too
      return FastCopyHashMap.class;
   }

   private static <K, V> Map<K, V> dataInstance() {
      return new FastCopyHashMap<>();
   }

   private static <K, V> Map<K, V> dataCopy(Map<K, V> original) {
      return new FastCopyHashMap<>(original);
   }

   private static IllegalStateException mapDoesNotExist() {
      return log.atomicMapDoesNotExist();
   }

   private static <K, V, MK> Map<K, V> getMap(EntryView.ReadEntryView<MK, Object> view) {
      return view.find().map(Map.class::cast).orElseThrow(AtomicMapProxyImpl::mapDoesNotExist);
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

   private Map<K, V> map() {
      Object value = cache.get(key);
      if (value == null) {
         throw log.atomicMapDoesNotExist();
      } else if (value instanceof Map) {
         return (Map<K, V>) value;
      } else {
         throw log.atomicMapHasWrongType(value, dataClass());
      }
   }

   @Override
   public int size() {
      return map().size();
   }

   @Override
   public boolean isEmpty() {
      return map().isEmpty();
   }

   @Override
   public boolean containsKey(Object subkey) {
      return map().containsKey(subkey);
   }

   @Override
   public boolean containsValue(Object value) {
      return map().containsValue(value);
   }

   @Override
   public V get(Object subkey) {
      return map().get(subkey);
   }

   @Override
   public V put(K subkey, V value) {
      return wait(rw.eval(this.key, new Put<>(subkey, value)));
   }

   @Override
   public V remove(Object subkey) {
      return wait(rw.eval(this.key, new Remove<V, MK>(subkey)));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      wait(rw.eval(this.key, new PutAll<>(m)));
   }

   @Override
   public void clear() {
      wait(rw.eval(this.key, Clear.instance()));
   }

   @Override
   public Set<K> keySet() {
      return new KeySetView();
   }

   @Override
   public Collection<V> values() {
      return new ValuesView();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return new EntrySetView();
   }

   @Override
   public String toString() {
      return "AtomicMapProxyImpl{" + key + "}";
   }

   private interface ExternalizableFunction {
      EF id();

      void writeParams(ObjectOutput output) throws IOException;
   }

   private static class Touch<MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, Boolean>, ExternalizableFunction {
      private static final Touch INSTANCE = new Touch();

      public static <MK> Touch<MK> instance() {
         return INSTANCE;
      }

      @Override
      public Boolean apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         if (view.find().isPresent()) {
            if (dataClass() != view.get().getClass()) {
               throw log.atomicMapHasWrongType(view.get(), dataClass());
            }
            return true;
         } else {
            view.set(dataInstance());
            return false;
         }
      }

      @Override
      public EF id() {
         return EF.TOUCH;
      }

      @Override
      public void writeParams(ObjectOutput output) throws IOException {
      }
   }

   private static class Put<K, V, MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, V>, ExternalizableFunction {
      private final K key;
      private final V value;

      public Put(K key, V value) {
         this.key = key;
         this.value = value;
      }

      public static <K, V, MK> Put<K, V, MK> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new Put<>((K) input.readObject(), (V) input.readObject());
      }

      @Override
      public V apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         Map<K, V> copy = dataCopy(getMap(view));
         view.set(copy);
         return copy.put(key, value);
      }

      @Override
      public EF id() {
         return EF.PUT;
      }

      @Override
      public void writeParams(ObjectOutput output) throws IOException {
         output.writeObject(key);
         output.writeObject(value);
      }
   }

   private static class Remove<V, MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, V>, ExternalizableFunction {
      private final Object key;

      public Remove(Object key) {
         this.key = key;
      }

      public static <V, MK> Remove<V, MK> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new Remove<>(input.readObject());
      }

      @Override
      public V apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         Map<?, V> copy = dataCopy(getMap(view));
         view.set(copy);
         return copy.remove(key);
      }

      @Override
      public EF id() {
         return EF.REMOVE;
      }

      @Override
      public void writeParams(ObjectOutput output) throws IOException {
         output.writeObject(key);
      }
   }

   private static class PutAll<K, V, MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, Void>, ExternalizableFunction {
      private final Map<? extends K, ? extends V> m;

      private PutAll(Map<? extends K, ? extends V> m) {
         this.m = m;
      }

      private static <K, V, MK> PutAll<K, V, MK> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new PutAll<>(MarshallUtil.unmarshallMap(input, HashMap::new));
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         Map<K, V> copy = dataCopy(getMap(view));
         copy.putAll(m);
         view.set(copy);
         return null;
      }

      @Override
      public EF id() {
         return EF.PUT_ALL;
      }

      @Override
      public void writeParams(ObjectOutput output) throws IOException {
         MarshallUtil.marshallMap(m, output);
      }
   }

   private static class Clear<MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, Void>, ExternalizableFunction {
      private static final Clear INSTANCE = new Clear();

      public static <MK> Clear<MK> instance() {
         return INSTANCE;
      }

      @Override
      public Void apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         getMap(view); // just check existence
         view.set(dataInstance());
         return null;
      }

      @Override
      public EF id() {
         return EF.CLEAR;
      }

      @Override
      public void writeParams(ObjectOutput output) throws IOException {
      }
   }

   private static class RemoveValue<MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, Boolean>, ExternalizableFunction {
      private final Object value;

      public static <MK> RemoveValue<MK> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new RemoveValue<>(input.readObject());
      }

      public RemoveValue(Object value) {
         this.value = value;
      }

      @Override
      public Boolean apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         Map<?, ?> map = dataCopy(getMap(view));
         return map.values().remove(value);
      }

      @Override
      public EF id() {
         return EF.REMOVE_VALUE;
      }

      @Override
      public void writeParams(ObjectOutput output) throws IOException {
         output.writeObject(value);
      }
   }

   private static class RemoveCond<MK> implements Function<EntryView.ReadWriteEntryView<MK, Object>, Boolean>, ExternalizableFunction {
      private final Object key;
      private final Object value;

      public static <MK> RemoveCond<MK> readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
         return new RemoveCond<>(input.readObject(), input.readObject());
      }

      public RemoveCond(Object key, Object value) {
         this.key = key;
         this.value = value;
      }

      @Override
      public Boolean apply(EntryView.ReadWriteEntryView<MK, Object> view) {
         Map<?, ?> map = dataCopy(getMap(view));
         return map.remove(key, value);
      }

      @Override
      public EF id() {
         return EF.REMOVE_COND;
      }

      @Override
      public void writeParams(ObjectOutput output) throws IOException {
         output.writeObject(key);
         output.writeObject(value);
      }
   }

   public static class Externalizer implements AdvancedExternalizer<ExternalizableFunction> {
      @Override
      public Set<Class<? extends ExternalizableFunction>> getTypeClasses() {
         return Util.asSet(Touch.class, Put.class, Remove.class, PutAll.class, Clear.class,
               RemoveValue.class, RemoveCond.class);
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_MAP_FUNCTIONS;
      }

      @Override
      public void writeObject(UserObjectOutput output, ExternalizableFunction object) throws IOException {
         output.writeByte(object.id().ordinal());
         object.writeParams(output);
      }

      @Override
      public ExternalizableFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         int id = input.read();
         switch (FUNCTIONS[id]) {
            case TOUCH:
               return Touch.instance();
            case PUT:
               return Put.readFrom(input);
            case REMOVE:
               return Remove.readFrom(input);
            case PUT_ALL:
               return PutAll.readFrom(input);
            case CLEAR:
               return Clear.instance();
            case REMOVE_VALUE:
               return RemoveValue.readFrom(input);
            case REMOVE_COND:
               return RemoveCond.readFrom(input);
            default:
               throw new IllegalArgumentException("Unknown function id " + id);
         }
      }
   }

   private class KeySetView extends AbstractSet<K> {
      private Set<K> keys() {
         return map().keySet();
      }

      @Override
      public int size() {
         return AtomicMapProxyImpl.this.size();
      }

      @Override
      public boolean contains(Object o) {
         return AtomicMapProxyImpl.this.containsKey(o);
      }

      @Override
      public Iterator<K> iterator() {
         return new KeyIterator(keys().iterator());
      }

      @Override
      public Object[] toArray() {
         return keys().toArray();
      }

      @Override
      public <T> T[] toArray(T[] a) {
         return keys().toArray(a);
      }

      @Override
      public boolean remove(Object o) {
         return AtomicMapProxyImpl.this.remove(o) != null;
      }

      @Override
      public boolean containsAll(Collection<?> c) {
         return keys().containsAll(c);
      }

      @Override
      public void clear() {
         AtomicMapProxyImpl.this.clear();
      }
   }

   private class KeyIterator implements Iterator<K> {
      private final Iterator<K> iterator;
      private K last;

      public KeyIterator(Iterator<K> iterator) {
         this.iterator = iterator;
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public K next() {
         return last = iterator.next();
      }

      @Override
      public void remove() {
         if (last == null) {
            throw new IllegalStateException();
         }
         AtomicMapProxyImpl.this.remove(last);
      }
   }

   private class ValuesView extends AbstractCollection<V> {
      private Collection<V> values() {
         return map().values();
      }

      @Override
      public int size() {
         return AtomicMapProxyImpl.this.size();
      }

      @Override
      public boolean contains(Object o) {
         return AtomicMapProxyImpl.this.containsValue(o);
      }

      @Override
      public Iterator<V> iterator() {
         return new ValuesIterator(values().iterator());
      }

      @Override
      public Object[] toArray() {
         return values().toArray();
      }

      @Override
      public <T> T[] toArray(T[] a) {
         return values().toArray(a);
      }

      @Override
      public boolean remove(Object o) {
         return AtomicMapProxyImpl.wait(rw.eval(key, new RemoveValue<>(o)));
      }

      @Override
      public boolean containsAll(Collection<?> c) {
         return values().containsAll(c);
      }

      @Override
      public void clear() {
         AtomicMapProxyImpl.this.clear();
      }

      private class ValuesIterator implements Iterator<V> {
         private final Iterator<V> iterator;
         private V last;

         public ValuesIterator(Iterator<V> iterator) {
            this.iterator = iterator;
         }

         @Override
         public boolean hasNext() {
            return iterator.hasNext();
         }

         @Override
         public V next() {
            return last = iterator.next();
         }

         @Override
         public void remove() {
            if (last == null) {
               throw new IllegalStateException();
            }
            AtomicMapProxyImpl.wait(rw.eval(key, new RemoveValue<>(last)));
         }
      }
   }

   private class EntrySetView extends AbstractSet<Entry<K, V>> {
      private Set<Map.Entry<K, V>> entrySet() {
         return map().entrySet();
      }

      @Override
      public int size() {
         return AtomicMapProxyImpl.this.size();
      }

      @Override
      public boolean contains(Object o) {
         if (o instanceof Map.Entry) {
            Entry<K, V> entry = (Entry<K, V>) o;
            V v = AtomicMapProxyImpl.this.get(entry.getKey());
            return Objects.equals(v, entry.getValue());
         } else {
            return false;
         }
      }

      @Override
      public Iterator<Entry<K, V>> iterator() {
         return new EntrySetIterator(entrySet().iterator());
      }

      @Override
      public Object[] toArray() {
         return entrySet().toArray();
      }

      @Override
      public <T> T[] toArray(T[] a) {
         return entrySet().toArray(a);
      }

      @Override
      public boolean remove(Object o) {
         if (o instanceof Map.Entry) {
            Entry<K, V> entry = (Entry<K, V>) o;
            return AtomicMapProxyImpl.wait(rw.eval(key, new RemoveCond<>(entry.getKey(), entry.getValue())));
         } else return false;
      }

      @Override
      public boolean containsAll(Collection<?> c) {
         return entrySet().containsAll(c);
      }

      @Override
      public void clear() {
         AtomicMapProxyImpl.this.clear();
      }
   }

   private class EntrySetIterator implements Iterator<Entry<K, V>> {
      private final Iterator<Entry<K, V>> iterator;
      private Entry<K, V> last;

      public EntrySetIterator(Iterator<Entry<K, V>> iterator) {
         this.iterator = iterator;
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public Entry<K, V> next() {
         return last = iterator.next();
      }

      @Override
      public void remove() {
         if (last == null) {
            throw new IllegalStateException();
         }
         AtomicMapProxyImpl.wait(rw.eval(key, new RemoveCond(last.getKey(), last.getValue())));
      }
   }
}
