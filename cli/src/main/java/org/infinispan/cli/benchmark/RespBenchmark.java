package org.infinispan.cli.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.util.Util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;

/**
 * @since 15.0
 **/
public abstract class RespBenchmark implements BenchmarkTask {
   private final String uri;
   private final int keySize;
   private final int valueSize;
   private final int keySetSize;

   RedisClient client;
   RedisCommands<byte[], byte[]> connection;
   byte[] value;
   List<byte[]> keySet;
   AtomicInteger nextIndex;

   private RespBenchmark(String uri, int keySize, int valueSize, int keySetSize) {
      this.uri = uri;
      this.keySize = keySize;
      this.valueSize = valueSize;
      this.keySetSize = keySetSize;
   }

   @Override
   public void setup() {
      client = RedisClient.create(uri);
      connection = client.connect(ByteArrayCodec.INSTANCE).sync();
      value = new byte[valueSize];
      keySet = new ArrayList<>(keySetSize);
      Random r = new Random(17);
      for (int i = 0; i < keySetSize; i++) {
         byte[] key = new byte[keySize];
         r.nextBytes(key);
         keySet.add(key);
         connection.set(key, value);
      }
      nextIndex = new AtomicInteger();
   }

   @Override
   public void teardown() {
      Util.close(client);
   }

   byte[] nextKey() {
      return keySet.get(nextIndex.getAndIncrement() % keySetSize);
   }

   public static RespBenchmark get(String uri, int keySize, int valueSize, int keySetSize) {
      return new RespBenchmark(uri, keySize, valueSize, keySetSize) {
         @Override
         public void run() {
            connection.get(nextKey());
         }

         @Override
         public String name() {
            return "RespBenchmark.get";
         }
      };
   }

   public static RespBenchmark put(String uri, int keySize, int valueSize, int keySetSize) {
      return new RespBenchmark(uri, keySize, valueSize, keySetSize) {
         @Override
         public void run() {
            connection.set(nextKey(), value);
         }

         @Override
         public String name() {
            return "RespBenchmark.put";
         }
      };
   }
}
