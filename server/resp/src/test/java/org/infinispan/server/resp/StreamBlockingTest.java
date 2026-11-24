package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.server.resp.commands.stream.XREAD;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

/**
 * RESP blocking XREAD testing.
 *
 * <p>
 * The tests mirror the blocking part of the Redis test suite file {@code tests/unit/type/stream.tcl} from the Redis
 * unstable branch. Test methods are named after the upstream tests. The blocked command is issued through
 * {@link #registerListener(Supplier)}, which waits until the server installs the blocking listener, replacing the
 * upstream {@code wait_for_blocked_client} helper.
 * </p>
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "server.resp.StreamBlockingTest")
public class StreamBlockingTest extends SingleNodeRespBaseTest {

   private static final Predicate<Object> IS_XREAD_LISTENER = l -> l instanceof XREAD.XReadListener;

   private StatefulRedisConnection<String, String> connection;
   private StatefulRedisConnection<String, String> second;

   @Override
   public Object[] factory() {
      return new Object[] {
            new StreamBlockingTest(),
            new StreamBlockingTest().withAuthorization(),
      };
   }

   @BeforeMethod
   public void initConnection() {
      connection = redisConnection;
      second = newConnection();
   }

   @AfterMethod(alwaysRun = true)
   protected void forceRemoveBlockingListeners() {
      CacheNotifierImpl<?, ?> cni = (CacheNotifierImpl<?, ?>) TestingUtil.extractComponent(cache, CacheNotifier.class);
      cni.getListeners().stream().filter(IS_XREAD_LISTENER)
            .forEach(cni::removeListenerAsync);
   }

   // Upstream: Blocking XREAD waiting new data
   public void testBlockingXreadWaitingNewData() {
      var redis = connection.sync();
      redis.del("s1", "s2", "s3");
      redis.xadd("s2", "old", "abcd1234");

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(20000),
                  XReadArgs.StreamOffset.latest("s1"),
                  XReadArgs.StreamOffset.latest("s2"),
                  XReadArgs.StreamOffset.latest("s3")));
      redis.xadd("s2", "new", "abcd1234");

      List<io.lettuce.core.StreamMessage<String, String>> res = join(future);
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getStream()).isEqualTo("s2");
      assertThat(res.get(0).getBody().get("new")).isEqualTo("abcd1234");
   }

   // Upstream: Blocking XREAD waiting old data
   public void testBlockingXreadWaitingOldData() {
      var redis = connection.sync();
      redis.del("s1", "s2", "s3");

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(20000),
                  XReadArgs.StreamOffset.latest("s1"),
                  XReadArgs.StreamOffset.from("s2", "0-0"),
                  XReadArgs.StreamOffset.latest("s3")));
      redis.xadd("s2", "old", "abcd1234");

      List<io.lettuce.core.StreamMessage<String, String>> res = join(future);
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getStream()).isEqualTo("s2");
      assertThat(res.get(0).getBody().get("old")).isEqualTo("abcd1234");
   }

   // Upstream: Blocking XREAD will not reply with an empty array
   public void testBlockingXreadWillNotReplyWithAnEmptyArray() {
      var redis = connection.sync();
      redis.del("s1");
      redis.xadd("s1", new XAddArgs().id("666-0"), "f", "v");
      redis.xadd("s1", new XAddArgs().id("667-0"), "f2", "v2");
      redis.xdel("s1", "667-0");

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(500), XReadArgs.StreamOffset.from("s1", "666-0")));
      // The Lettuce client represents the nil reply as an empty list.
      assertThat(join(future)).isEmpty();
   }

   // Upstream: Blocking XREAD for stream that ran dry (issue #5299)
   public void testBlockingXreadForStreamThatRanDry() {
      var redis = connection.sync();
      redis.del("mystream");
      redis.xadd("mystream", new XAddArgs().id("666-0"), "key", "value");
      redis.xdel("mystream", "666-0");

      // An ID smaller than the stream's last ID, released on timeout.
      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(300), XReadArgs.StreamOffset.from("mystream", "665-0")));
      assertThat(join(future)).isEmpty();

      // An error if the ID is equal or smaller than the last ID.
      assertThatThrownBy(() -> redis.xadd("mystream", new XAddArgs().id("665-0"), "key", "value"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
      assertThatThrownBy(() -> redis.xadd("mystream", new XAddArgs().id("666-0"), "key", "value"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");

      // Enter the blocking state, then release with a new entry.
      future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(0), XReadArgs.StreamOffset.from("mystream", "665-0")));
      redis.xadd("mystream", new XAddArgs().id("667-0"), "key", "value");
      List<io.lettuce.core.StreamMessage<String, String>> res = join(future);
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getStream()).isEqualTo("mystream");
      assertThat(res.get(0).getId()).isEqualTo("667-0");
      assertThat(res.get(0).getBody().get("key")).isEqualTo("value");
   }

   // Upstream: XREAD last element blocking from empty stream
   public void testXreadLastElementBlockingFromEmptyStream() {
      var redis = connection.sync();
      redis.del("lestream");

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(20000), XReadArgs.StreamOffset.last("lestream")));
      redis.xadd("lestream", new XAddArgs().id("1-0"), "k1", "v1");

      List<io.lettuce.core.StreamMessage<String, String>> res = join(future);
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getId()).isEqualTo("1-0");
      assertThat(res.get(0).getBody().get("k1")).isEqualTo("v1");
   }

   // Upstream: XREAD last element blocking from non-empty stream
   public void testXreadLastElementBlockingFromNonEmptyStream() {
      var redis = connection.sync();
      redis.del("lestream");
      redis.xadd("lestream", new XAddArgs().id("1-0"), "k1", "v1");
      redis.xadd("lestream", new XAddArgs().id("2-0"), "k2", "v2");
      redis.xadd("lestream", new XAddArgs().id("3-0"), "k3", "v3");

      List<io.lettuce.core.StreamMessage<String, String>> res = redis.xread(
            new XReadArgs().block(1000000), XReadArgs.StreamOffset.last("lestream"));
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getId()).isEqualTo("3-0");
   }

   // Upstream: XREAD streamID edge (blocking)
   public void testXreadStreamIdEdgeBlocking() {
      var redis = connection.sync();
      redis.del("x");

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(0), XReadArgs.StreamOffset.from("x", "1-18446744073709551615")));
      redis.xadd("x", new XAddArgs().id("1-1"), "f", "v");
      redis.xadd("x", new XAddArgs().id("1-18446744073709551615"), "f", "v");
      redis.xadd("x", new XAddArgs().id("2-1"), "f", "v");

      List<io.lettuce.core.StreamMessage<String, String>> res = join(future);
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getId()).isEqualTo("2-1");
   }

   // Upstream: XREAD with same stream name multiple times should work
   public void testXreadWithSameStreamNameMultipleTimes() {
      var redis = connection.sync();
      redis.del("s2");
      redis.xadd("s2", "old", "abcd1234");

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(20000),
                  XReadArgs.StreamOffset.latest("s2"),
                  XReadArgs.StreamOffset.latest("s2"),
                  XReadArgs.StreamOffset.latest("s2")));
      redis.xadd("s2", "new", "abcd1234");

      List<io.lettuce.core.StreamMessage<String, String>> res = join(future);
      assertThat(res).isNotEmpty();
      assertThat(res.get(0).getStream()).isEqualTo("s2");
      assertThat(res.get(0).getBody().get("new")).isEqualTo("abcd1234");
   }

   /**
    * Upstream: XREAD + multiple XADD inside transaction. Adapted: the transaction commands are not applied atomically
    * (see {@code RespTransactionHandler}), so the blocked reader may receive the entries across multiple replies.
    * The test verifies all the entries are received in order, continuing from the last ID received.
    */
   public void testXreadMultipleXaddInsideTransaction() {
      var redis = connection.sync();
      redis.del("s2");
      redis.xadd("s2", "old", "abcd1234");

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(20000), XReadArgs.StreamOffset.latest("s2")));

      redis.multi();
      redis.xadd("s2", "field", "one");
      redis.xadd("s2", "field", "two");
      redis.xadd("s2", "field", "three");
      redis.exec();

      List<io.lettuce.core.StreamMessage<String, String>> res = join(future);
      List<io.lettuce.core.StreamMessage<String, String>> all = new java.util.ArrayList<>(res);
      String lastId = all.get(all.size() - 1).getId();
      // Continue reading from the last ID received until all the entries are retrieved.
      for (int i = 0; i < 10 && all.size() < 3; i++) {
         List<io.lettuce.core.StreamMessage<String, String>> next = second.sync()
               .xread(new XReadArgs().block(1000), XReadArgs.StreamOffset.from("s2", lastId));
         if (next != null && !next.isEmpty()) {
            all.addAll(next);
            lastId = next.get(next.size() - 1).getId();
         }
      }
      assertThat(all).hasSize(3);
      assertThat(all.get(0).getBody().get("field")).isEqualTo("one");
      assertThat(all.get(1).getBody().get("field")).isEqualTo("two");
      assertThat(all.get(2).getBody().get("field")).isEqualTo("three");
   }

   // XREAD BLOCK timing out with no data returns a nil reply.
   public void testBlockingXreadTimeout() {
      var redis = connection.sync();
      redis.del("timeout-stream");
      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> future = registerListener(() -> second.async()
            .xread(new XReadArgs().block(200), XReadArgs.StreamOffset.latest("timeout-stream")));
      // The Lettuce client represents the nil reply as an empty list.
      assertThat(join(future)).isEmpty();
   }

   // A client disconnecting while blocked with BLOCK 0 releases the listener and the blocked-client metrics.
   public void testDisconnectCancelsBlockedXread() {
      var redis = connection.sync();
      redis.del("leakstream");
      CacheNotifierImpl<?, ?> cni = (CacheNotifierImpl<?, ?>) TestingUtil.extractComponent(cache, CacheNotifier.class);
      long listeners = cni.getListeners().stream().filter(IS_XREAD_LISTENER).count();

      registerListener(() -> second.async()
            .xread(new XReadArgs().block(0), XReadArgs.StreamOffset.latest("leakstream")));
      second.close();

      eventually(() -> cni.getListeners().stream().filter(IS_XREAD_LISTENER).count() == listeners);
   }

   // Multiple blocked clients are all served with the new data: the entries are not consumed.
   public void testMultipleBlockedClientsReceiveSameData() {
      var redis = connection.sync();
      redis.del("fanout");
      RedisAsyncCommands<String, String> async1 = second.async();
      StatefulRedisConnection<String, String> third = newConnection();
      RedisAsyncCommands<String, String> async2 = third.async();

      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> f1 = registerListener(() ->
            async1.xread(new XReadArgs().block(20000), XReadArgs.StreamOffset.latest("fanout")));
      RedisFuture<List<io.lettuce.core.StreamMessage<String, String>>> f2 = registerListener(() ->
            async2.xread(new XReadArgs().block(20000), XReadArgs.StreamOffset.latest("fanout")));

      redis.xadd("fanout", "f", "v");

      List<io.lettuce.core.StreamMessage<String, String>> res1 = join(f1);
      List<io.lettuce.core.StreamMessage<String, String>> res2 = join(f2);
      assertThat(res1).hasSize(1);
      assertThat(res2).hasSize(1);
      assertThat(res1.get(0).getId()).isEqualTo(res2.get(0).getId());
   }

   /**
    * Issues the blocked command and waits until the server installs the blocking listener, or the command completes.
    * Replaces the upstream {@code wait_for_blocked_client} helper.
    */
   private <T> RedisFuture<T> registerListener(Supplier<RedisFuture<T>> redisOp) {
      CacheNotifierImpl<?, ?> cni = (CacheNotifierImpl<?, ?>) TestingUtil.extractComponent(cache, CacheNotifier.class);
      long pre = cni.getListeners().stream().filter(IS_XREAD_LISTENER).count();
      RedisFuture<T> rf = redisOp.get();

      // If the future is done, the command completed without blocking (or failed during the listener registration,
      // making the test fail), no need to wait.
      eventually(() -> (cni.getListeners().stream().filter(IS_XREAD_LISTENER).count() == pre + 1) || rf.isDone());
      return rf;
   }

   private static <T> T join(RedisFuture<T> future) {
      try {
         return future.get(30, TimeUnit.SECONDS);
      } catch (Exception e) {
         throw new AssertionError(e);
      }
   }
}
