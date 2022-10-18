package org.infinispan.cli.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.Util;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 15.0
 **/
@State(Scope.Thread)
public class RespBenchmark {
   RedisClient client;

   RedisCommands<byte[], byte[]> connection;

   @Param("redis://127.0.0.1:11222")
   public String uri;

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
      client = RedisClient.create(uri);
      connection = client.connect(ByteArrayCodec.INSTANCE).sync();
      value = new byte[valueSize];
      keySet = new ArrayList<>(keySetSize);
      Random r = new Random(17); // We always use the same seed to make things repeatable
      for (int i = 0; i < keySetSize; i++) {
         byte[] key = new byte[keySize];
         r.nextBytes(key);
         keySet.add(key);
         connection.set(key, value);
      }
      nextIndex = new AtomicInteger();
   }

   @Benchmark
   public void get(Blackhole bh) {
      bh.consume(connection.get(nextKey()));
   }

   @Benchmark
   public void put() {
      connection.set(nextKey(), value);
   }

   @TearDown
   public void teardown() {
      Util.close(client);
   }

   private byte[] nextKey() {
      return keySet.get(nextIndex.getAndIncrement() % keySetSize);
   }
}
