package org.infinispan.client.hotrod.near;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.configuration.NearCacheConfiguration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

public class MockNearCacheService<K, V> extends NearCacheService<K, V> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
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
         MockPutEvent<K, V> mockPutEvent = new MockPutEvent<>(key, value);
         log.debugf("Adding put event: %s", mockPutEvent);
         events.add(mockPutEvent);
      }

      @Override
      public void putIfAbsent(K key, MetadataValue<V> value) {
         delegate.putIfAbsent(key, value);
         MockPutIfAbsentEvent<K, V> mockPutIfAbsentEvent = new MockPutIfAbsentEvent<>(key, value);
         log.debugf("Adding putIfAbsent event: %s", mockPutIfAbsentEvent);
         events.add(mockPutIfAbsentEvent);
      }

      @Override
      public boolean remove(K key) {
         boolean removed = delegate.remove(key);
         MockRemoveEvent<K> mockRemoveEvent = new MockRemoveEvent<>(key);
         log.debugf("Adding remove event: %s", mockRemoveEvent);
         events.add(mockRemoveEvent);
         return removed;
      }

      @Override
      public MetadataValue<V> get(K key) {
         MetadataValue<V> value = delegate.get(key);
         MockGetEvent<K, V> mockGetEvent = new MockGetEvent<>(key, value);
         log.debugf("Adding get event: %s", mockGetEvent);
         events.add(mockGetEvent);
         return value;
      }

      @Override
      public void clear() {
         delegate.clear();
         events.clear();
         MockClearEvent clearEvent = new MockClearEvent();
         log.debugf("Adding clear event: %s", clearEvent);
         events.add(clearEvent);
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

      @Override
      public String toString() {
         return "MockRemoveEvent{key=" + key + '}';
      }
   }
   static class MockClearEvent extends MockEvent {}

}
