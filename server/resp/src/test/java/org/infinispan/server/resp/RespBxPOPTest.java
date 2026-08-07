package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
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
import org.infinispan.server.resp.commands.list.blocking.AbstractBlockingPop;
import org.infinispan.server.resp.commands.list.blocking.BLMOVEM;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import io.lettuce.core.KeyValue;
import io.lettuce.core.LMPopArgs;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

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

   @Override
   public Object[] factory() {
      return new Object[]{
         new RespBxPOPTest(),
         new RespBxPOPTest().right(),
         new RespBxPOPTest().simpleCache(),
         new RespBxPOPTest().withAuthorization(),
         new RespBxPOPTest().simpleCache().right(),
         new RespBxPOPTest().simpleCache().withAuthorization(),
         new RespBxPOPTest().right().withAuthorization(),
         new RespBxPOPTest().simpleCache().right().withAuthorization()
      };
   }

   RespBxPOPTest simpleCache() {
      this.cacheMode = CacheMode.LOCAL;
      this.simpleCache = true;
      return this;
   }

   // Set test for BRPOP. Default is BLPOP
   protected RespBxPOPTest right() {
      this.right = true;
      return this;
   }

   protected boolean isRight() {
      return this.right;
   }

   @Override
   protected String parameters() {
      return "[simpleCache=" + simpleCache + ", cacheMode=" + cacheMode + ", right=" + right + ", authz=" + this.isAuthorizationEnabled() + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      if (simpleCache) {
         configurationBuilder.clustering().cacheMode(CacheMode.LOCAL).simpleCache(true);
      } else {
         configurationBuilder.clustering().cacheMode(cacheMode);
      }
   }

   <T> RedisFuture<T> registerListener(Supplier<RedisFuture<T>> redisOp) {
      return registerListener(cache, redisOp);
   }

   protected final <T> RedisFuture<T> registerListener(Cache<Object, Object> cache, Supplier<RedisFuture<T>> redisOp) {
      Predicate<Object> p = l -> l instanceof AbstractBlockingPop.PubSubListener || l instanceof BLMOVEM.MoveListener || l instanceof RespBxPOPTest.FailingListener;
      CacheNotifierImpl<?, ?> cni = (CacheNotifierImpl<?, ?>) TestingUtil.extractComponent(cache, CacheNotifier.class);
      long pre = cni.getListeners().stream().filter(p).count();
      RedisFuture<T> rf = redisOp.get();

      // If there's a listener ok otherwise
      // if rf is done an error during listener registration has happend
      // no need to wait anymore. test will fail
      eventually(() -> (cni.getListeners().stream().filter(p).count() == pre + 1) || rf.isDone());
      return rf;
   }

   void verifyListenerUnregistered() {
      verifyListenerUnregistered(cache);
   }

   static void verifyListenerUnregistered(Cache<Object, Object> cache) {
      CacheNotifierImpl<?, ?> cni = (CacheNotifierImpl<?, ?>) TestingUtil.extractComponent(cache, CacheNotifier.class);
      // Check listener is unregistered
      eventually(() -> cni.getListeners().stream().noneMatch(
         l -> l instanceof AbstractBlockingPop.PubSubListener || l instanceof BLMOVEM.MoveListener || l instanceof RespBxPOPTest.FailingListener));
   }

   @Test
   public void testBxpop() throws Exception {
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

         String[] values = {"first", "second", "third"};
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
         List<KeyValue<String, String>> events = List.of(
            cf.get(10, TimeUnit.SECONDS),
            cf2.get(10, TimeUnit.SECONDS),
            cf3.get(10, TimeUnit.SECONDS));

         assertThat(events)
            .hasSize(3)
            .containsExactlyInAnyOrder(
               KeyValue.just("key1", "value1a"),
               KeyValue.just("key1", "value1b"),
               KeyValue.just("key2", "value2a")
            );
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxPopTwoListenersWithValues() throws Exception {
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
         var data = new String[]{"value1a", "value1b"};
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
         var data = new String[]{"value2a", "value2b"};
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
         var data = new String[]{"value1a", "value1b"};
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
         var data = new String[]{"value1a", "value1b"};
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
         var data = new String[]{"value2a", "value2b"};
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
   public void testBxpopTwoListenersTwoKeysOneEvent() throws Exception {
      try {
         var data = new String[]{"value2a", "value2b"};
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
   public void testBxpopTwoListenersOneTimeout() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(5, "key"));
         var cf2 = registerListener(() -> bxPopAsync(5, "key"));
         redis.lpush("key", "first");
         timeService.advance(TimeUnit.SECONDS.toMillis(6));
         var res = cf.get(15, TimeUnit.SECONDS);
         var res2 = cf2.get(15, TimeUnit.SECONDS);

         KeyValue<String, String> completed = res != null ? res : res2;
         assertThat(completed).isNotNull();
         assertThat(completed.getKey()).isEqualTo("key");
         assertThat(completed.getValue()).isEqualTo("first");
         assertThat(res == null || res2 == null).isTrue();
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTwoListenersTwoProducers() throws Exception {
      RedisCommands<String, String> redis1 = redisConnection.sync();
      RedisCommands<String, String> redis2 = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(0, "key"));
         var cf2 = registerListener(() -> bxPopAsync(0, "key"));

         List<Future<?>> pushes = new ArrayList<>();
         pushes.add(fork(() -> redis1.lpush("key", "first")));
         pushes.add(fork(() -> redis2.lpush("key", "second", "third")));
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         assertThat(res.getKey()).isEqualTo("key");
         assertThat(res2.getKey()).isEqualTo("key");

         for (Future<?> f : pushes) {
            f.get(10, TimeUnit.SECONDS);
         }

         var rest = redis1.lrange("key", 0, -1);
         assertThat(rest).hasSize(1);
         assertThat(Arrays.asList(res.getValue(), res2.getValue(), rest.get(0)))
            .containsExactlyInAnyOrder("first", "second", "third");
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopThreeListenersOneTimesOutTwoProducers() throws Exception {
      RedisCommands<String, String> redis1 = redisConnection.sync();
      RedisCommands<String, String> redis2 = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(3, "key"));
         var cf2 = registerListener(() -> bxPopAsync(3, "key"));
         var cf3 = registerListener(() -> bxPopAsync(3, "key"));
         fork(() -> redis1.lpush("key", "first"));
         fork(() -> redis2.lpush("key", "second"));
         timeService.advance(TimeUnit.SECONDS.toMillis(4));
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
   public void testBxpopThreeListenersTwoProducers() throws Exception {
      RedisCommands<String, String> redis1 = redisConnection.sync();
      RedisCommands<String, String> redis2 = redisConnection.sync();
      try {
         var cf = registerListener(() -> bxPopAsync(10, "key"));
         var cf2 = registerListener(() -> bxPopAsync(10, "key"));
         var cf3 = registerListener(() -> bxPopAsync(10, "key"));

         List<Future<?>> pushes = List.of(
            fork(() -> xPush(redis1, "key", "first")),
            fork(() -> xPush(redis2, "key", "second", "third", "fourth"))
         );
         var res = cf.get(10, TimeUnit.SECONDS);
         var res2 = cf2.get(10, TimeUnit.SECONDS);
         var res3 = cf3.get(10, TimeUnit.SECONDS);
         List<String> results = Arrays.asList(res.getValue(), res2.getValue(), res3.getValue());
         results.sort(null);
         List<String> expected1 = Arrays.asList("first", "fourth", "third");
         List<String> expected2 = Arrays.asList("fourth", "second", "third");
         assertThat(results)
            .hasSize(3)
            .satisfiesAnyOf(
               ignore -> assertThat(results).containsExactlyElementsOf(expected1),
               ignore -> assertThat(results).containsExactlyElementsOf(expected2));

         for (Future<?> future : pushes) {
            future.get(10, TimeUnit.SECONDS);
         }

         var rest = redis1.lrange("key", 0, -1);
         assertThat(rest.size()).isEqualTo(1);
      } finally {
         verifyListenerUnregistered();
      }
   }

   @Test
   public void testBxpopTimeout() throws InterruptedException, ExecutionException {
      RedisCommands<String, String> redis = redisConnection.sync();
      RedisAsyncCommands<String, String> redisAsync = newConnection().async();

      redis.rpush("key1", "first", "second", "third");

      assertThatThrownBy(() -> bxPopAsync(-1, "keyZ").get(10, TimeUnit.SECONDS))
         .cause()
         .isInstanceOf(RedisCommandExecutionException.class)
         .hasMessageContaining("ERR value is out of range, must be positive");
      var res = bxPopAsync(1, "keyZ");
      // Advance time past the timeout to expire the blocking pop
      timeService.advance(TimeUnit.SECONDS.toMillis(2));
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
      RedisAsyncCommands<String, String> redisAsync = newConnection().async();
      return registerListener(() -> right ? redisAsync.brpop(to, keys)
         : redisAsync.blpop(to, keys));
   }

   private RedisFuture<KeyValue<String, List<String>>> blmpop(long timeout, int count, String... keys) {
      RedisAsyncCommands<String, String> async = newConnection().async();
      LMPopArgs args = (right ? LMPopArgs.Builder.right() : LMPopArgs.Builder.left())
         .count(count);
      return registerListener(() -> async.blmpop(timeout, args, keys));
   }

   private KeyValue<String, String> bxPop(long to, String... keys) {
      RedisCommands<String, String> redis = newConnection().sync();
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
      AbstractBlockingPop.PubSubListener blpop;

      public FailingListener(AbstractBlockingPop.PubSubListener arg) {
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


   public void testBLMPOP() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "key-blmpop";
      redis.lpush(key, "v1", "v2", "v3", "v4", "v5");

      try {
         RedisFuture<KeyValue<String, List<String>>> rf = blmpop(0, 3, key);
         KeyValue<String, List<String>> response = rf.get(10, TimeUnit.SECONDS);
         List<String> expected = right
            ? List.of("v1", "v2", "v3")
            : List.of("v5", "v4", "v3");

         assertThat(response.getKey()).isEqualTo(key);
         assertThat(response.getValue()).containsExactlyElementsOf(expected);

         String[] remaining = right
            ? new String[]{"v5", "v4"}
            : new String[]{"v2", "v1"};
         assertThat(redis.lrange(key, 0, -1)).containsExactly(remaining);
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testSplitBLMPOP() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "key-blmpop-split";

      try {
         RedisFuture<KeyValue<String, List<String>>> rf = blmpop(0, 3, key);

         redis.lpush(key, "v1");

         // Must wait for BLMPOP.
         // Otherwise, remaining pushes might happen concurrently with poll and remove more than one element.
         KeyValue<String, List<String>> response = rf.get(10, TimeUnit.SECONDS);

         redis.lpush(key, "v2");
         redis.lpush(key, "v3", "v4", "v5");

         assertThat(response.getKey()).isEqualTo(key);
         assertThat(response.getValue()).containsExactly("v1");
         assertThat(redis.lrange(key, 0, -1)).containsExactly("v5", "v4", "v3", "v2");
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testSplitAndSpreadBLMPOP() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key1 = "key-blmpop-split-1";
      String key2 = "key-blmpop-split-2";

      try {
         RedisFuture<KeyValue<String, List<String>>> rf = blmpop(0, 3, key1, key2);

         redis.lpush(key1, "v1-1", "v2-1");
         redis.lpush(key2, "v1-2", "v2-2", "v3-2");

         KeyValue<String, List<String>> response = rf.get(10, TimeUnit.SECONDS);

         assertThat(response.getKey()).isEqualTo(key1);
         assertThat(response.getValue()).containsExactlyInAnyOrder("v2-1", "v1-1");
         assertThat(redis.lrange(key1, 0, -1)).isEmpty();
         assertThat(redis.lrange(key2, 0, -1)).containsExactly("v3-2", "v2-2", "v1-2");
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testTwoListenersBLMPOP() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "key-blmpop-two";

      try {
         RedisFuture<KeyValue<String, List<String>>> rf1 = blmpop(0, 2, key);
         RedisFuture<KeyValue<String, List<String>>> rf2 = blmpop(0, 2, key);

         redis.lpush(key, "v1", "v2", "v3", "v4", "v5");

         KeyValue<String, List<String>> res1 = rf1.get(10, TimeUnit.SECONDS);
         KeyValue<String, List<String>> res2 = rf2.get(10, TimeUnit.SECONDS);

         assertThat(res1.getKey()).isEqualTo(key);
         assertThat(res2.getKey()).isEqualTo(key);

         String[] exp1 = right
            ? new String[]{"v1", "v2"}
            : new String[]{"v5", "v4"};
         String[] exp2 = right
            ? new String[]{"v3", "v4"}
            : new String[]{"v3", "v2"};

         assertThat(res1.getValue()).hasSize(2)
            .satisfiesAnyOf(
               l -> assertThat((List<String>) l).containsExactlyInAnyOrder(exp1),
               l -> assertThat((List<String>) l).containsExactlyInAnyOrder(exp2));
         assertThat(res2.getValue()).hasSize(2)
            .satisfiesAnyOf(
               l -> assertThat((List<String>) l).containsExactlyInAnyOrder(exp1),
               l -> assertThat((List<String>) l).containsExactlyInAnyOrder(exp2));
         assertThat(res1.getValue()).doesNotContainAnyElementsOf(res2.getValue());
         assertThat(redis.lrange(key, 0, -1)).containsExactly(right ? "v5" : "v1");
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMPOPListenerTimeout() throws Exception {
      try (StatefulRedisConnection<String, String> conn = newConnection()) {
         LMPopArgs args = LMPopArgs.Builder.left().count(3);
         RedisFuture<KeyValue<String, List<String>>> rf = registerListener(() -> conn.async().blmpop(1, args, "whatever"));
         timeService.advance(TimeUnit.SECONDS.toMillis(2));
         eventually(rf::isDone);
         KeyValue<String, List<String>> response = rf.get();

         assertThat(response).isNull();
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMPOPTimeoutWhenNotAList() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "key-blmpop-string";

      try {
         RedisFuture<KeyValue<String, List<String>>> rf = blmpop(3, 1, key);
         assertThat(rf.isDone()).isFalse();
         redis.set(key, "some-value");
         timeService.advance(TimeUnit.SECONDS.toMillis(4));

         KeyValue<String, List<String>> response = rf.get(10, TimeUnit.SECONDS);
         assertThat(response).isNull();
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMPOPWhenKeyNotAList() throws Exception {
      String key = "blmpop-string";
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.set(key, "something"), () -> redis.blmpop(0, LMPopArgs.Builder.left().count(1), key));
   }

   // BLMOVEM tests

   private static final ProtocolKeyword BLMOVEM_CMD = new ProtocolKeyword() {
      private final byte[] bytes = "BLMOVEM".getBytes(StandardCharsets.US_ASCII);

      @Override
      public byte[] getBytes() {
         return bytes;
      }

      @Override
      public String name() {
         return "BLMOVEM";
      }
   };

   @SuppressWarnings("unchecked")
   private RedisFuture<List<Object>> blmovemAsync(long timeout, String source, String dest,
                                                   String srcDir, String dstDir, String... extraArgs) {
      RedisAsyncCommands<String, String> async = newConnection().async();
      RedisCodec<String, String> codec = StringCodec.UTF8;
      CommandArgs<String, String> args = new CommandArgs<>(codec)
            .addKey(source).addKey(dest)
            .add(srcDir).add(dstDir)
            .add(String.valueOf(timeout));
      for (String arg : extraArgs) {
         args.add(arg);
      }
      return registerListener(() -> async.dispatch(BLMOVEM_CMD, new ArrayOutput<>(codec), args));
   }

   @SuppressWarnings("unchecked")
   private List<String> toStringList(List<Object> raw) {
      if (raw == null) return null;
      return raw.stream().map(o -> (String) o).toList();
   }

   public void testBLMOVEM() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = blmovemAsync(0, "blmm-src", "blmm-dst",
               isRight() ? "RIGHT" : "LEFT", isRight() ? "LEFT" : "RIGHT");
         redis.rpush("blmm-src", "a", "b", "c");
         List<String> result = toStringList(cf.get(10, TimeUnit.SECONDS));
         String expected = isRight() ? "c" : "a";
         assertThat(result).containsExactly(expected);
         assertThat(redis.lrange("blmm-dst", 0, -1)).containsExactly(expected);
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMWithExistingData() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.rpush("blmm-exist-src", "a", "b", "c");
      try {
         RedisAsyncCommands<String, String> async = newConnection().async();
         RedisCodec<String, String> codec = StringCodec.UTF8;
         CommandArgs<String, String> args = new CommandArgs<>(codec)
               .addKey("blmm-exist-src").addKey("blmm-exist-dst")
               .add("LEFT").add("RIGHT")
               .add("0");
         RedisFuture<List<Object>> cf = async.dispatch(BLMOVEM_CMD, new ArrayOutput<>(codec), args);
         List<String> result = toStringList(cf.get(10, TimeUnit.SECONDS));
         assertThat(result).containsExactly("a");
         assertThat(redis.lrange("blmm-exist-src", 0, -1)).containsExactly("b", "c");
         assertThat(redis.lrange("blmm-exist-dst", 0, -1)).containsExactly("a");
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMTimeout() throws Exception {
      try {
         RedisAsyncCommands<String, String> async = newConnection().async();
         RedisCodec<String, String> codec = StringCodec.UTF8;
         CommandArgs<String, String> args = new CommandArgs<>(codec)
               .addKey("blmm-to-src").addKey("blmm-to-dst")
               .add("LEFT").add("RIGHT")
               .add("1");
         RedisFuture<List<Object>> cf = registerListener(() ->
               async.dispatch(BLMOVEM_CMD, new ArrayOutput<>(codec), args));
         timeService.advance(TimeUnit.SECONDS.toMillis(2));
         eventually(cf::isDone);
         assertThat(cf.get()).isNull();
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMCount() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = blmovemAsync(0, "blmm-cnt-src", "blmm-cnt-dst",
               "LEFT", "RIGHT", "COUNT", "3", "OBO");
         redis.rpush("blmm-cnt-src", "a", "b", "c", "d", "e");
         List<String> result = toStringList(cf.get(10, TimeUnit.SECONDS));
         assertThat(result).containsExactly("a", "b", "c");
         assertThat(redis.lrange("blmm-cnt-src", 0, -1)).containsExactly("d", "e");
         assertThat(redis.lrange("blmm-cnt-dst", 0, -1)).containsExactly("a", "b", "c");
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMNegativeTimeout() {
      RedisCommands<String, String> redis = redisConnection.sync();
      RedisCodec<String, String> codec = StringCodec.UTF8;
      assertThatThrownBy(() -> redis.dispatch(BLMOVEM_CMD, new ArrayOutput<>(codec),
            new CommandArgs<>(codec).addKey("s").addKey("d").add("LEFT").add("RIGHT").add("-1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range, must be positive");
   }

   public void testBLMOVEMCountPartialUnblock() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = blmovemAsync(0, "blmm-partial-src", "blmm-partial-dst",
               "LEFT", "RIGHT", "COUNT", "5", "BULK");
         redis.rpush("blmm-partial-src", "a", "b");
         List<String> result = toStringList(cf.get(10, TimeUnit.SECONDS));
         assertThat(result).containsExactly("a", "b");
         assertThat(redis.lrange("blmm-partial-dst", 0, -1)).containsExactly("a", "b");
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMExactlyBlocksUntilEnough() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      try {
         var cf = blmovemAsync(0, "blmm-exact-src", "blmm-exact-dst",
               "LEFT", "RIGHT", "EXACTLY", "3", "BULK");

         redis.rpush("blmm-exact-src", "1", "2");
         // Not enough yet: should stay blocked, source unchanged
         assertThat(cf.isDone()).isFalse();
         eventually(() -> redis.lrange("blmm-exact-src", 0, -1).size() == 2);
         assertThat(redis.exists("blmm-exact-dst")).isEqualTo(0);

         redis.rpush("blmm-exact-src", "3");
         List<String> result = toStringList(cf.get(10, TimeUnit.SECONDS));
         assertThat(result).containsExactly("1", "2", "3");
         assertThat(redis.lrange("blmm-exact-dst", 0, -1)).containsExactly("1", "2", "3");
         assertThat(redis.exists("blmm-exact-src")).isEqualTo(0);
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMExactlyWokenByLinsert() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.rpush("blmm-ins-src", "1", "2");
      try {
         var cf = blmovemAsync(0, "blmm-ins-src", "blmm-ins-dst",
               "LEFT", "RIGHT", "EXACTLY", "3", "BULK");

         redis.linsert("blmm-ins-src", true, "1", "0");
         List<String> result = toStringList(cf.get(10, TimeUnit.SECONDS));
         assertThat(result).containsExactly("0", "1", "2");
         assertThat(redis.lrange("blmm-ins-dst", 0, -1)).containsExactly("0", "1", "2");
         assertThat(redis.exists("blmm-ins-src")).isEqualTo(0);
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMExactlyWokenByLmove() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.rpush("blmm-lm-src", "1", "2");
      redis.rpush("blmm-lm-feed", "9");
      try {
         var cf = blmovemAsync(0, "blmm-lm-src", "blmm-lm-dst",
               "LEFT", "RIGHT", "EXACTLY", "3", "BULK");

         redis.lmove("blmm-lm-feed", "blmm-lm-src", io.lettuce.core.LMoveArgs.Builder.leftRight());
         List<String> result = toStringList(cf.get(10, TimeUnit.SECONDS));
         assertThat(result).containsExactly("1", "2", "9");
         assertThat(redis.lrange("blmm-lm-dst", 0, -1)).containsExactly("1", "2", "9");
         assertThat(redis.exists("blmm-lm-src")).isEqualTo(0);
      } finally {
         verifyListenerUnregistered();
      }
   }

   public void testBLMOVEMInsideMulti() {
      RedisCommands<String, String> redis = redisConnection.sync();
      RedisCodec<String, String> codec = StringCodec.UTF8;
      redis.multi();
      redis.dispatch(BLMOVEM_CMD, new ArrayOutput<>(codec),
            new CommandArgs<>(codec).addKey("blmm-multi-src").addKey("blmm-multi-dst")
                  .add("LEFT").add("RIGHT").add("0").add("EXACTLY").add("3").add("BULK"));
      io.lettuce.core.TransactionResult execResult = redis.exec();
      assertThat(execResult).hasSize(1);
      assertThat((Object) execResult.get(0)).isNull();
   }
}
