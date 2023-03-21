package org.infinispan.server.resp.test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class CommonRespTests {
   public static void testPipeline(StatefulRedisConnection<String, String> redisConnection) throws ExecutionException,
         InterruptedException, TimeoutException {
      int commandCount = 35;
      AtomicReference<Throwable> error = new AtomicReference<>();
      AtomicInteger setCompleted = new AtomicInteger();
      RedisAsyncCommands<String, String> redis = redisConnection.async();
      for (int i = 0; i < commandCount; ++i) {
         redis.set("key-" + i, "value-" + i).whenComplete((___, t) -> {
            if (t != null) {
               error.set(t);
            }
            setCompleted.incrementAndGet();
         });
      }
      // This will effectively wait for all set commands above to be completed
      redis.ping().get(10, TimeUnit.SECONDS);

      Throwable throwable = error.get();
      if (throwable != null) {
         throw new AssertionError(throwable);
      }

      AtomicReferenceArray<Map.Entry<String, String>> atomicReferenceArray = new AtomicReferenceArray<>(commandCount + 1);
      for (int i = 0; i < commandCount; ++i) {
         String key = "key-" + i;

         if (i == 13) {
            redis.get("not-present")
                  .whenComplete((v, t) -> {
                     if (t != null) {
                        error.set(t);
                     } else {
                        atomicReferenceArray.set(commandCount, new AbstractMap.SimpleEntry<>("key-" + commandCount, v));
                     }
                  });
         }
         int j = i;
         redis.get(key).whenComplete((v, t) -> {
            if (t != null) {
               error.set(t);
            } else {
               atomicReferenceArray.set(j, new AbstractMap.SimpleEntry<>(key, v));
            }
         });
      }

      redis.ping().get(10, TimeUnit.SECONDS);

      throwable = error.get();
      if (throwable != null) {
         throw new AssertionError(throwable);
      }

      for (int i = 0; i < atomicReferenceArray.length(); ++i) {
         Map.Entry<String, String> entry = atomicReferenceArray.get(i);
         assertEquals("key-" + i, entry.getKey());
         if (i == commandCount) {
            assertNull(entry.getValue());
         } else {
            assertEquals("value-" + i, entry.getValue());
         }
      }
   }
}
