package org.infinispan.cli.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.util.Util;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
@State(Scope.Thread)
public class HotRodBenchmark {
   RemoteCacheManager cm;
   RemoteCache cache;

   @Param("hotrod://127.0.0.1")
   public String uri;

   @Param("benchmark")
   public String cacheName;

   @Param("16")
   public int keySize;

   @Param("1000")
   public int valueSize;

   @Param("1000")
   public int keySetSize;

   byte[] value;
   List<byte[]> keySet;
   AtomicInteger nextIndex;

   @Setup
   public void setup() {
      cm = new RemoteCacheManager(uri);
      cache = cm.getCache(cacheName);
      if (cache == null) {
         throw new IllegalArgumentException("Could not find cache " + cacheName);
      }
      value = new byte[valueSize];
      keySet = new ArrayList<>(keySetSize);
      Random r = new Random(17); // We always use the same seed to make things repeatable
      for (int i = 0; i < keySetSize; i++) {
         byte[] key = new byte[keySize];
         r.nextBytes(key);
         keySet.add(key);
         cache.put(key, value);
      }
      nextIndex = new AtomicInteger();
   }

   @Benchmark
   public void get(Blackhole bh) {
      bh.consume(cache.get(nextKey()));
   }

   @Benchmark
   public void put() {
      cache.put(nextKey(), value);
   }

   @TearDown
   public void teardown() {
      Util.close(cm);
   }

   private byte[] nextKey() {
      return keySet.get(nextIndex.getAndIncrement() % keySetSize);
   }
}
