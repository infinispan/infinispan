package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.resp.commands.list.blocking.BPOP;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Class for blocking commands tests.
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
@Test(groups = "functional", testName = "server.resp.RespBxPOPTest")
public class RespBxPOPTest extends SingleNodeRespBaseTest {

   private CacheMode cacheMode = CacheMode.LOCAL;
   private boolean simpleCache;
   private boolean right;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RespBxPOPTest(),
            new RespBxPOPTest().simpleCache(),
            new RespBxPOPTest().right(),
            new RespBxPOPTest().simpleCache().right()
      };
   }

   RespBxPOPTest simpleCache() {
      this.cacheMode = CacheMode.LOCAL;
      this.simpleCache = true;
      return this;
   }

   // Set test for BRPOP. Default is BLPOP
   RespBxPOPTest right() {
      this.right = true;
      return this;
   }

   boolean isRight() {
      return this.right;
   }

   @Override
   protected String parameters() {
      return "[simpleCache=" + simpleCache + ", cacheMode=" + cacheMode + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      if (simpleCache) {
         configurationBuilder.clustering().cacheMode(CacheMode.LOCAL).simpleCache(true);
      } else {
         configurationBuilder.clustering().cacheMode(cacheMode);
      }
   }

   RedisFuture<KeyValue<String, String>> registerListener(Supplier<RedisFuture<KeyValue<String, String>>> redisOp) {
      return registerListener(cache, redisOp);
   }

   static RedisFuture<KeyValue<String, String>> registerListener(Cache<Object, Object> cache,
         Supplier<RedisFuture<KeyValue<String, String>>> redisOp) {
      var cni = (CacheNotifierImpl<?, ?>) TestingUtil.extractComponent(cache, CacheNotifier.class);
      long pre = cni.getListeners().stream()
            .filter(l -> l instanceof BPOP.PubSubListener || l instanceof RespBxPOPTest.FailingListener)
            .count();
      RedisFuture<KeyValue<String, String>> rf = redisOp.get();
      // If there's a listener ok otherwise
      // if rf is done an error during listener registration has happend
      // no need to wait anymore. test will fail
      eventually(() -> (cni.getListeners().stream()
            .filter(l -> l instanceof BPOP.PubSubListener || l instanceof RespBxPOPTest.FailingListener)
            .count() == pre + 1)
            || rf.isDone());
      return rf;
   }

   void verifyListenerUnregistered() {
      verifyListenerUnregistered(cache);
   }

   static void verifyListenerUnregistered(Cache<Object, Object> cache) {
      CacheNotifierImpl<?, ?> cni = (CacheNotifierImpl<?, ?>) TestingUtil.extractComponent(cache, CacheNotifier.class);
      // Check listener is unregistered
      eventually(() -> cni.getListeners().stream().noneMatch(
            l -> l instanceof BPOP.PubSubListener || l instanceof RespBxPOPTest.FailingListener));
   }

   @Test
   public void testBxpop()
         throws InterruptedException, ExecutionException, TimeoutException, java.util.concurrent.TimeoutException {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(0, "keyZ"));
         redis.lpush("keyZ", "firstZ", "secondZ");
         var res = cf.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("keyZ");
         // Expected results depends on which pop is actually tested
         String expectPop = isRight() ? "firstZ" : "secondZ";
         String expectRemain = isRight() ? "secondZ" : "firstZ";
         assertThat(res.getValue()).isEqualTo(expectPop);
         // Check brpop (feeded by listener) removed just one element
         assertThat(redis.lrange("keyZ", 0, -1))
               .containsExactly(expectRemain);

         String[] values = { "first", "second", "third" };
         redis.rpush("key1", values);
         res = bxPop(0, "key1");
         // Expected results depends on which pop is actually tested
         final String expectPop1 = isRight() ? "third" : "first";
         // remove the popped and get the remainings
         String[] remaining = Arrays.stream(values).filter((arg) -> arg != expectPop1).toArray(String[]::new);
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo(expectPop1);
         // Check brpop (feeded by poll) removed just one element
         assertThat(redis.lrange("key1", 0, -1))
               .containsExactlyInAnyOrder(remaining);

         res = bxPop(0, "key2", "key1");
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo("second");
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxPopMultipleListenersTwoKeysTwoEvents()
         throws InterruptedException, ExecutionException, TimeoutException {
      try {
         RedisCommands<String, String> redis = redisConnection.sync();
         var cf = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         var cf2 = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         var cf3 = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         if (isRight()) {
            redis.lpush("key1", "value1a", "value1b");
            redis.lpush("key2", "value2a", "value2b");
         } else {
            redis.rpush("key1", "value1a", "value1b");
            redis.rpush("key2", "value2a", "value2b");
         }
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         var res3 = cf3.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo("value1a");
         assertThat(res2.getKey()).isEqualTo("key1");
         assertThat(res2.getValue()).isEqualTo("value1b");
         assertThat(res3.getKey()).isEqualTo("key2");
         assertThat(res3.getValue()).isEqualTo("value2a");
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxPopTwoListenersWithValues()
         throws InterruptedException, ExecutionException, TimeoutException, java.util.concurrent.TimeoutException {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(0, "key"));
         var cf2 = registerListener(() -> bxPopAsync(0, "key"));
         if (isRight()) {
            redis.lpush("key", "first", "second", "third");
         } else {
            redis.rpush("key", "first", "second", "third");
         }
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key");
         assertThat(res2.getKey()).isEqualTo("key");
         assertThat(redis.lrange("key", 0, -1))
               .containsExactly("third");
         assertThat(Arrays.asList(res.getValue(), res2.getValue())).containsExactlyInAnyOrder("first", "second");
         // Check brpop (feeded by events) removed two events
         assertThat(redis.lrange("key", 0, -1))
               .containsExactly("third");
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoKeys() throws InterruptedException, ExecutionException, TimeoutException {
      try {
         RedisCommands<String, String> redis = redisConnection.sync();
         var data = new String[] { "value1a", "value1b" };
         redis.rpush("key1", data);
         redis.rpush("key2", "value2a", "value2b");
         RedisFuture<KeyValue<String, String>> cf = bxPopAsync(0, "key1", "key2");
         var res = cf.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo(head(data));
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoKeysOneEvent() throws InterruptedException, ExecutionException, TimeoutException {
      try {
         RedisCommands<String, String> redis = redisConnection.sync();
         var data = new String[] { "value2a", "value2b" };
         redis.rpush("key2", "value2a", "value2b");
         var cf = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         redis.rpush("key1", "value1a", "value1b");
         var res = cf.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key2");
         assertThat(res.getValue()).isEqualTo(head(data));
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoKeysTwoEvents() throws InterruptedException, ExecutionException, TimeoutException {
      try {
         var data = new String[] { "value1a", "value1b" };
         RedisCommands<String, String> redis = redisConnection.sync();
         var cf = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         redis.rpush("key1", data);
         redis.rpush("key2", "value2a", "value2b");
         var res = cf.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo(head(data));
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoListenersTwoKeys() throws InterruptedException, ExecutionException, TimeoutException {
      try {
         var data = new String[] { "value1a", "value1b" };
         RedisCommands<String, String> redis = redisConnection.sync();
         redis.rpush("key1", data);
         redis.rpush("key2", "value2a", "value2b");
         var cf = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         var cf2 = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo(head(data));
         assertThat(res2.getKey()).isEqualTo("key1");
         assertThat(res2.getValue()).isEqualTo(fromHead(data, 1));
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoListenersTwoKeys2() throws InterruptedException, ExecutionException, TimeoutException {
      try {
         var data = new String[] { "value2a", "value2b" };
         RedisCommands<String, String> redis = redisConnection.sync();
         redis.rpush("key1", "value1a");
         redis.rpush("key2", data);
         var cf = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         var cf2 = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo("value1a");
         assertThat(res2.getKey()).isEqualTo("key2");
         assertThat(res2.getValue()).isEqualTo(head(data));
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoListenersTwoKeysOneEvent()
         throws InterruptedException, ExecutionException, TimeoutException {
      try {
         var data = new String[] { "value2a", "value2b" };
         RedisCommands<String, String> redis = redisConnection.sync();
         redis.rpush("key1", "value1a");
         var cf = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         var cf2 = registerListener(() -> bxPopAsync(0, "key1", "key2"));
         redis.rpush("key2", data);
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key1");
         assertThat(res.getValue()).isEqualTo("value1a");
         assertThat(res2.getKey()).isEqualTo("key2");
         assertThat(res2.getValue()).isEqualTo(head(data));
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoListenersOneTimeout()
         throws InterruptedException, ExecutionException, TimeoutException, java.util.concurrent.TimeoutException {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(10, "key"));
         var cf2 = registerListener(() -> bxPopAsync(10, "key"));
         redis.lpush("key", "first");
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);

         assertThat(res.getKey()).isEqualTo("key");
         assertThat(res.getValue()).isEqualTo("first");
         assertThat(res2).isNull();
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoListenersTwoProducers()
         throws InterruptedException, ExecutionException, TimeoutException, java.util.concurrent.TimeoutException {
      RedisCommands<String, String> redis1 = redisConnection.sync();
      RedisCommands<String, String> redis2 = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(0, "key"));
         var cf2 = registerListener(() -> bxPopAsync(0, "key"));
         fork(() -> redis1.lpush("key", "first"));
         fork(() -> redis2.lpush("key", "second", "third"));
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key");
         assertThat(res2.getKey()).isEqualTo("key");
         var rest = redis1.lrange("key", 0, -1);
         assertThat(rest.size()).isEqualTo(1);
         assertThat(Arrays.asList(res.getValue(), res2.getValue(), rest.get(0)))
               .containsExactlyInAnyOrder("first", "second", "third");
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopThreeListenersOneTimesOutTwoProducers()
         throws InterruptedException, ExecutionException, TimeoutException, java.util.concurrent.TimeoutException {
      RedisCommands<String, String> redis1 = redisConnection.sync();
      RedisCommands<String, String> redis2 = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(3, "key"));
         var cf2 = registerListener(() -> bxPopAsync(3, "key"));
         var cf3 = registerListener(() -> bxPopAsync(3, "key"));
         fork(() -> redis1.lpush("key", "first"));
         fork(() -> redis2.lpush("key", "second"));
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         var res3 = cf3.get(10, TimeUnit.SECONDS);
         assertThat(Arrays.asList(extractValue(res), extractValue(res2), extractValue(res3)))
               .containsExactlyInAnyOrder("first", "second", null);
         assertThat(redis1.lrange("key", 0, -1)).isEmpty();
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopThreeListenersTwoProducers()
         throws InterruptedException, ExecutionException, TimeoutException, java.util.concurrent.TimeoutException {
      RedisCommands<String, String> redis1 = redisConnection.sync();
      RedisCommands<String, String> redis2 = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(10, "key"));
         var cf2 = registerListener(() -> bxPopAsync(10, "key"));
         var cf3 = registerListener(() -> bxPopAsync(10, "key"));
         fork(() -> xPush(redis1, "key", "first"));
         fork(() -> xPush(redis2, "key", "second", "third", "fourth"));
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         var res3 = cf3.get(10, TimeUnit.SECONDS);
         List<String> results = Arrays.asList(res.getValue(), res2.getValue(), res3.getValue());
         results.sort(null);
         List<String> expected1 = Arrays.asList("first", "fourth", "third");
         List<String> expected2 = Arrays.asList("fourth", "second", "third");
         assertThat(results.size()).isEqualTo(3);
         assertThat(results.containsAll(expected1) ||
               results.containsAll(expected2)).isTrue();
         var rest = redis1.lrange("key", 0, -1);
         assertThat(rest.size()).isEqualTo(1);
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTimeout() throws InterruptedException, ExecutionException {
      RedisCommands<String, String> redis = redisConnection.sync();
      RedisAsyncCommands<String, String> redisAsync = client.connect().async();

      redis.rpush("key1", "first", "second", "third");

      assertThatThrownBy(() -> bxPopAsync(-1, "keyZ").get(10, TimeUnit.SECONDS))
            .cause()
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");
      var res = bxPopAsync(1, "keyZ");
      // Ensure bxpop is expired
      eventually(res::isDone);
      redis.lpush("keyZ", "firstZ");
      assertThat(res.get()).isNull();

      try {
         var cf = registerListener(() -> redisAsync.brpop(0, "keyY"));
         redis.lpush("keyY", "valueY");
         assertThat(cf.get().getKey()).isEqualTo("keyY");
         assertThat(cf.get().getValue()).isEqualTo("valueY");
      } finally {
         verifyListenerUnregistered();
      }
   }

   private String extractValue(KeyValue<String, String> kv) {
      return kv == null ? null : kv.getValue();
   }

   private RedisFuture<KeyValue<String, String>> bxPopAsync(long to, String... keys) {
      RedisAsyncCommands<String, String> redisAsync = client.connect().async();
      return registerListener(() -> right ? redisAsync.brpop(to, keys)
            : redisAsync.blpop(to, keys));
   }

   private KeyValue<String, String> bxPop(long to, String... keys) {
      RedisCommands<String, String> redis = client.connect().sync();
      return right ? redis.brpop(to, keys)
            : redis.blpop(to, keys);
   }

   private Long xPush(RedisCommands<String, String> redis, String key, String... values) {
      return right ? redis.rpush(key, values) : redis.lpush(key, values);
   }

   // Get left or right head of a string array
   String head(String[] a) {
      if (right)
         return a[a.length - 1];
      return a[0];
   }

   String fromHead(String[] a, int c) {
      if (right)
         return a[a.length - c - 1];
      return a[c];
   }

   @Listener(clustered = true)
   public static class FailingListener {
      BPOP.PubSubListener blpop;

      public FailingListener(BPOP.PubSubListener arg) {
         blpop = arg;
      }

      @CacheEntryCreated
      @CacheEntryModified
      public CompletionStage<Void> onEvent(CacheEntryEvent<Object, Object> entryEvent) {
         blpop.getFuture().completeExceptionally(
               new RuntimeException("Injected failure in OnEvent"));
         return CompletableFutures.completedNull();
      }
   }

   @Test
   public void testBxpopAsync()
         throws InterruptedException, ExecutionException, TimeoutException {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.lpush("keyY", "firstY");
      try {
         var cf = registerListener(() -> bxPopAsync(0, "keyZ"));
         // Ensure lpush is after bxpop
         redis.lpush("keyZ", "firstZ");
         var response = cf.get(10, TimeUnit.SECONDS);
         assertThat(response.getKey()).isEqualTo("keyZ");
         assertThat(response.getValue()).isEqualTo("firstZ");
         assertThat(redis.lpop("keyZ")).isNull();
      } finally {
         verifyListenerUnregistered();
      }
   }

}
