package org.infinispan.test.fwk;

import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.InternalEntryFactoryImpl;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.MortalCacheValue;
import org.infinispan.container.entries.TransientCacheValue;
import org.infinispan.container.entries.TransientMortalCacheValue;

import static org.infinispan.test.AbstractInfinispanTest.TIME_SERVICE;

/**
 * A factory for internal entries for the test suite
 */
public class TestInternalCacheEntryFactory {

   private static final InternalEntryFactory FACTORY = new InternalEntryFactoryImpl();

   static {
      ((InternalEntryFactoryImpl) FACTORY).injectTimeService(TIME_SERVICE);
   }

   public static InternalCacheEntry create(Object key, Object value) {
      return new ImmortalCacheEntry(key, value);
   }

   public static InternalCacheValue create(Object value) {
      return new ImmortalCacheValue(value);
   }

   public static InternalCacheEntry create(Object key, Object value, long lifespan) {
      return create(FACTORY, key, value, lifespan);
   }

   public static <K,V> InternalCacheEntry<K,V> create(InternalEntryFactory factory, K key, V value, long lifespan) {
      //noinspection unchecked
      return factory.create(key, value, null, lifespan, -1);
   }

   public static InternalCacheEntry create(Object key, Object value, long lifespan, long maxIdle) {
      return create(FACTORY, key, value, lifespan, maxIdle);
   }

   public static <K,V> InternalCacheEntry<K,V> create(InternalEntryFactory factory, K key, V value, long lifespan, long maxIdle) {
      //noinspection unchecked
      return factory.create(key, value, null, lifespan, maxIdle);
   }

   public static InternalCacheEntry create(Object key, Object value, long created, long lifespan, long lastUsed, long maxIdle) {
      return FACTORY.create(key, value, new EmbeddedMetadata.Builder().build(),
            created, lifespan, lastUsed, maxIdle);
   }

   public static InternalCacheValue createValue(Object v, long created, long lifespan, long lastUsed, long maxIdle) {
      if (lifespan < 0 && maxIdle < 0) return new ImmortalCacheValue(v);
      if (lifespan > -1 && maxIdle < 0) return new MortalCacheValue(v, created, lifespan);
      if (lifespan < 0 && maxIdle > -1) return new TransientCacheValue(v, maxIdle, lastUsed);
      return new TransientMortalCacheValue(v, created, lifespan, maxIdle, lastUsed);
   }

}
