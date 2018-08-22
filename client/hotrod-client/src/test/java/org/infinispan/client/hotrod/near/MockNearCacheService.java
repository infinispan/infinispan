package org.infinispan.client.hotrod.near;

import java.util.concurrent.BlockingQueue;

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
   protected NearCache<K, V> createNearCache(NearCacheConfiguration config) {
      NearCache<K, V> delegate = super.createNearCache(config);
      return new MockNearCache<>(delegate, events);
   }

   static abstract class MockEvent {}
   static abstract class MockKeyValueEvent<K, V> extends MockEvent {
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
      public void put(K key, MetadataValue<V> value) {
         delegate.put(key, value);
         events.add(new MockPutEvent<K, V>(key, value));
      }

      @Override
      public void putIfAbsent(K key, MetadataValue<V> value) {
         delegate.putIfAbsent(key, value);
         events.add(new MockPutIfAbsentEvent<K, V>(key, value));
      }

      @Override
      public boolean remove(K key) {
         boolean removed = delegate.remove(key);
         events.add(new MockRemoveEvent<>(key));
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
   }

   static class MockPutEvent<K, V> extends MockKeyValueEvent<K, V> {
      MockPutEvent(K key, MetadataValue<V> value) {
         super(key, value);
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
   }
   static class MockClearEvent extends MockEvent {}

}
