package org.infinispan.client.hotrod.near;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;

public class MockNearCacheService<K, V> extends NearCacheService<K, V> {
   final BlockingQueue<MockEvent> events;

   MockNearCacheService(NearCacheConfiguration cfg, BlockingQueue<MockEvent> events, ClientListenerNotifier listenerNotifier) {
      super(cfg, listenerNotifier);
      this.events = events;
   }

   @Override
   protected NearCache<K, V> createNearCache(NearCacheConfiguration config, BiConsumer<K, MetadataValue<V>> biConsumer) {
      NearCache<K, V> delegate = super.createNearCache(config, biConsumer);
      return new MockNearCache<>(delegate, events);
   }

   abstract static class MockEvent {}
   abstract static class MockKeyValueEvent<K, V> extends MockEvent {
      final K key;
      final MetadataValue<V> value;
      MockKeyValueEvent(K key, MetadataValue<V> value) {
         this.key = key;
         this.value = value;
      }
      @Override
      public String toString() {
         return this.getClass().getName() + "{" + "key=" + key + ", value=" + value +'}';
      }
   }

   static class MockNearCache<K, V> implements NearCache<K, V> {
      final NearCache<K, V> delegate;
      final BlockingQueue<MockEvent> events;

      MockNearCache(NearCache<K, V> delegate, BlockingQueue<MockEvent> events) {
         this.delegate = delegate;
         this.events = events;
      }

      @Override
      public boolean putIfAbsent(K key, MetadataValue<V> value) {
         boolean put = delegate.putIfAbsent(key, value);
         events.add(new MockPutIfAbsentEvent<>(key, value));
         return put;
      }

      @Override
      public boolean replace(K key, MetadataValue<V> prevValue, MetadataValue<V> newValue) {
         boolean put = delegate.replace(key, prevValue, newValue);
         events.add(new MockReplaceEvent<>(key, prevValue, newValue));
         return put;
      }

      @Override
      public boolean remove(K key) {
         boolean removed = delegate.remove(key);
         events.add(new MockRemoveEvent<>(key));
         return removed;
      }

      @Override
      public boolean remove(K key, MetadataValue<V> value) {
         boolean removed = delegate.remove(key, value);
         if (removed) {
            // We only raise the event if the remove actually removed something. This is due to the fact that
            // optional remove is only done locally so its cost is negligible for a miss unlike the other
            // remove that is fired due to a remote event which is many orders of magnitude slower
            events.add(new MockRemoveEvent<>(key));
         }
         return removed;
      }

      @Override
      public MetadataValue<V> get(K key) {
         MetadataValue<V> value = delegate.get(key);
         events.add(new MockGetEvent<>(key, value));
         return value;
      }

      @Override
      public void clear() {
         delegate.clear();
         events.clear();
         events.add(new MockClearEvent());
      }

      @Override
      public int size() {
         return delegate.size();
      }

      @Override
      public Iterator<Map.Entry<K, MetadataValue<V>>> iterator() {
         return delegate.iterator();
      }
   }

   static class MockReplaceEvent<K, V> extends MockKeyValueEvent<K, V> {
      final MetadataValue<V> prevValue;
      MockReplaceEvent(K key, MetadataValue<V> prevValue, MetadataValue<V> newValue) {
         super(key, newValue);
         this.prevValue = prevValue;
      }

      @Override
      public String toString() {
         return this.getClass().getName() + "{" + "key=" + key + ", prevValue=" + prevValue + ", value=" + value +'}';
      }
   }
   static class MockPutIfAbsentEvent<K, V> extends MockKeyValueEvent<K, V> {
      MockPutIfAbsentEvent(K key, MetadataValue<V> value) {
         super(key, value);
      }
   }
   static class MockGetEvent<K, V> extends MockKeyValueEvent<K, V> {
      MockGetEvent(K key, MetadataValue<V> value) {
         super(key, value);
      }
   }
   static class MockRemoveEvent<K> extends MockEvent {
      final K key;
      MockRemoveEvent(K key) {
         this.key = key;
      }

      @Override
      public String toString() {
         return "MockRemoveEvent{key=" + key + '}';
      }
   }
   static class MockClearEvent extends MockEvent {}

}
