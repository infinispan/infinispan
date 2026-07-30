package org.infinispan.cli.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.util.Util;

/**
 * @since 12.0
 **/
public abstract class HotRodBenchmark implements BenchmarkTask {
   private final String uri;
   private final String cacheName;
   private final int keySize;
   private final int valueSize;
   private final int keySetSize;

   RemoteCacheManager cm;
   RemoteCache<byte[], byte[]> cache;
   byte[] value;
   List<byte[]> keySet;
   AtomicInteger nextIndex;

   private HotRodBenchmark(String uri, String cacheName, int keySize, int valueSize, int keySetSize) {
      this.uri = uri;
      this.cacheName = cacheName;
      this.keySize = keySize;
      this.valueSize = valueSize;
      this.keySetSize = keySetSize;
   }

   @Override
   public void setup() {
      cm = new RemoteCacheManager(uri);
      cache = cm.getCache(cacheName);
      if (cache == null) {
         Util.close(cm);
         throw new IllegalArgumentException("Could not find cache " + cacheName);
      }
      value = new byte[valueSize];
      keySet = new ArrayList<>(keySetSize);
      Random r = new Random(17);
      for (int i = 0; i < keySetSize; i++) {
         byte[] key = new byte[keySize];
         r.nextBytes(key);
         keySet.add(key);
         cache.put(key, value);
      }
      nextIndex = new AtomicInteger();
   }

   @Override
   public void teardown() {
      Util.close(cm);
   }

   byte[] nextKey() {
      return keySet.get(nextIndex.getAndIncrement() % keySetSize);
   }

   public static HotRodBenchmark get(String uri, String cacheName, int keySize, int valueSize, int keySetSize) {
      return new HotRodBenchmark(uri, cacheName, keySize, valueSize, keySetSize) {
         @Override
         public void run() {
            cache.get(nextKey());
         }

         @Override
         public String name() {
            return "HotRodBenchmark.get";
         }
      };
   }

   public static HotRodBenchmark put(String uri, String cacheName, int keySize, int valueSize, int keySetSize) {
      return new HotRodBenchmark(uri, cacheName, keySize, valueSize, keySetSize) {
         @Override
         public void run() {
            cache.put(nextKey(), value);
         }

         @Override
         public String name() {
            return "HotRodBenchmark.put";
         }
      };
   }
}
