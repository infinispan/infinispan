package org.infinispan.server.security.authorization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.ErrorType;
import io.vertx.redis.client.impl.types.MultiType;

@ExtendWith(VertxExtension.class)
abstract class RESPAuthorizationTest {

   protected final InfinispanServerExtension ext;

   protected final boolean cert;
   protected final boolean useSsl;
   protected final Function<TestUser, String> serverPrincipal;
   protected final Map<TestUser, RedisOptions> respBuilders;

   public RESPAuthorizationTest(InfinispanServerExtension ext) {
      this(ext, false, false, TestUser::getUser, user ->
            new RedisOptions().setPoolName("pool-" + user));
   }

   public RESPAuthorizationTest(InfinispanServerExtension ext, boolean cert, boolean useSsl, Function<TestUser, String> serverPrincipal, Function<TestUser, RedisOptions> respBuilder) {
      this.ext = ext;
      this.cert = cert;
      this.useSsl = useSsl;
      this.serverPrincipal = serverPrincipal;
      this.respBuilders = Stream.of(TestUser.values()).collect(Collectors.toMap(user -> user, respBuilder));
   }

   @Test
   public void testSetGetDelete(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(TestUser.ADMIN, vertx);
      redis.set(List.of("k1", "v1"))
            .onFailure(ctx::failNow)
            .compose(ignore -> redis.get("k1").onFailure(ctx::failNow))
            .onSuccess(v -> ctx.verify(() -> assertThat(v.toString()).isEqualTo("v1")))
            .compose(ignore -> redis.del(List.of("k1")).onFailure(ctx::failNow))
            .compose(ignore -> redis.get("k1").onFailure(ctx::failNow))
            .onFailure(ctx::failNow)
            .onSuccess(v -> {
               ctx.verify(() -> assertThat(v).isNull());
               ctx.completeNow();
            });
   }

   @Test
   public void testOnlyAdminConnectAndOperate(Vertx vertx, VertxTestContext ctx) {
      List<Future<?>> futures = new ArrayList<>();
      for (TestUser user: TestUser.values()) {
         if (user == TestUser.ANONYMOUS) {
            assertAnonymous(createClient(user, vertx), r -> r.ping(Collections.emptyList()), ctx);
            continue;
         }

         if (user != TestUser.ADMIN) {
            assertUnauthorized(user, vertx, r -> r.ping(Collections.emptyList()), ctx);
            continue;
         }

         RedisAPI redis = createConnection(user, vertx);
         Future<Response> f = redis.ping(Collections.emptyList())
               .onFailure(ctx::failNow)
               .onSuccess(res -> ctx.verify(() -> assertThat(res.toString()).isEqualTo("PONG")));
         futures.add(f);
      }

      Future.all(futures)
            .onFailure(ctx::failNow)
            .onComplete(ignore -> ctx.completeNow());
   }

   @Test
   public void testRPUSH(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(TestUser.ADMIN, vertx);
      redis.rpush(List.of("people", "tristan"))
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(1));
               return redis.rpush(List.of("people", "william"));
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toLong()).isEqualTo(2));
               return redis.rpush(List.of("people", "william", "jose", "pedro"));
            })
            .onSuccess(v -> ctx.verify(() -> assertThat(v.toLong()).isEqualTo(5)))
            .andThen(ignore -> redis.set(List.of("leads", "tristan")))
            .compose(ignore -> ctx.assertFailure(redis.rpush(List.of("leads", "william")))
                  .onFailure(t -> ctx.verify(() -> assertThat(t)
                        .isInstanceOf(ErrorType.class)
                        .hasMessageContaining("WRONGTYPE")))
            ).onComplete(ignore -> ctx.completeNow());
   }

   @Test
   public void testInfoCommand(Vertx vertx, VertxTestContext ctx) {
      List<Future<?>> futures = new ArrayList<>();

      for (TestUser user: TestUser.values()) {
         if (user == TestUser.ANONYMOUS) {
            assertAnonymous(createClient(user, vertx), r -> r.info(Collections.emptyList()), ctx);
            continue;
         }

         if (user != TestUser.ADMIN) {
            assertUnauthorized(user, vertx, r -> r.info(Collections.emptyList()), ctx);
            continue;
         }

         RedisAPI redis = createConnection(user, vertx);
         Future<?> f = redis.info(Collections.emptyList())
               .onFailure(ctx::failNow)
               .onSuccess(r -> ctx.verify(() -> assertThat(r.toString()).contains("redis_version")));
         futures.add(f);
      }

      Future.all(futures)
            .onFailure(ctx::failNow)
            .onComplete(ignore -> ctx.completeNow());
   }

   @Test
   public void testClusterSHARDS(Vertx vertx, VertxTestContext ctx) {
      List<Future<?>> futures = new ArrayList<>();

      for (TestUser user: TestUser.values()) {
         if (user == TestUser.ANONYMOUS) {
            assertAnonymous(createClient(user, vertx), r -> r.cluster(List.of("SHARDS")), ctx);
            continue;
         }

         if (user != TestUser.ADMIN) {
            assertUnauthorized(user, vertx, r -> r.cluster(List.of("SHARDS")), ctx);
            continue;
         }

         RedisAPI redis = createConnection(user, vertx);
         Future<?> f = redis.cluster(List.of("SHARDS"))
               .onFailure(ctx::failNow)
               .onSuccess(r -> ctx.verify(() -> assertThat(r)
                     .isExactlyInstanceOf(MultiType.class)
                     .size().isEqualTo(2)));
         futures.add(f);
      }

      Future.all(futures)
            .onFailure(ctx::failNow)
            .onComplete(ignore -> ctx.completeNow());
   }

   @Test
   public void testHMSETCommand(Vertx vertx, VertxTestContext ctx) {
      RedisAPI redis = createConnection(TestUser.ADMIN, vertx);

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      List<String> args = new ArrayList<>();
      args.add("HMSET");
      args.addAll(map.entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue())).collect(Collectors.toList()));

      redis.hmset(args)
            .onFailure(ctx::failNow)
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("OK"));
               return redis.hget("HMSET", "key1");
            })
            .compose(v -> {
               ctx.verify(() -> assertThat(v.toString()).isEqualTo("value1"));
               return redis.hget("HMSET", "unknown");
            })
            .onSuccess(v -> ctx.verify(() -> assertThat(v).isNull()))
            .andThen(ignore -> redis.set(List.of("plain", "string")))
            .compose(ignore -> ctx.assertFailure(redis.hmset(List.of("plain", "k1", "v1")))
                  .onFailure(t -> ctx.verify(() -> assertThat(t)
                        .isInstanceOf(ErrorType.class)
                        .hasMessageContaining("WRONGTYPE"))))
            .onComplete(ignore -> ctx.completeNow());
   }

   @Test
   public void testClusterNodes(Vertx vertx, VertxTestContext ctx) {
      List<Future<?>> futures = new ArrayList<>();

      for (TestUser user: TestUser.values()) {
         if (user == TestUser.ANONYMOUS) {
            assertAnonymous(createClient(user, vertx), r -> r.cluster(List.of("NODES")), ctx);
            continue;
         }

         if (user != TestUser.ADMIN) {
            assertUnauthorized(user, vertx, r -> r.cluster(List.of("NODES")), ctx);
            continue;
         }

         RedisAPI redis = createConnection(user, vertx);
         Future<?> f = redis.cluster(List.of("NODES"))
               .onFailure(ctx::failNow)
               .onSuccess(r -> ctx.verify(() -> assertThat(r.toString().split("\n")).hasSize(2)));
         futures.add(f);
      }

      Future.all(futures)
            .onFailure(ctx::failNow)
            .onComplete(ignore -> ctx.completeNow());
   }

   private void assertAnonymous(Redis redis, Function<RedisAPI, Future<?>> consumer, VertxTestContext ctx) {
      // If using USERNAME + PASSWORD, redis accepts anonymous connections.
      // But following requests will fail, as user needs to be authenticated.
      RedisAPI client = createConnection(redis);
      ctx.assertFailure(consumer.apply(client))
            .onComplete(r -> {
               if (r.succeeded()) {
                  ctx.failNow("Exception not thrown!");
                  return;
               }

               ctx.verify(() -> assertThat(r.cause())
                     .isInstanceOf(ConnectException.class)
                     .hasMessageStartingWith("Cannot connect to any of the provided endpoints"));
            });
   }

   private void assertUnauthorized(TestUser user, Vertx vertx, Function<RedisAPI, Future<?>> consumer, VertxTestContext ctx) {
      RedisAPI client = createConnection(user, vertx);
      ctx.assertFailure(consumer.apply(client))
            .onComplete(r -> {
               if (r.succeeded()) {
                  ctx.failNow(String.format("User '%s' should fail submitting operation", user));
                  return;
               }

               ctx.verify(() -> assertThat(r.cause())
                     .isInstanceOf(ConnectException.class)
                     .hasMessageContaining("WRONGPASS"));
            });
   }

   private Redis createClient(TestUser user, Vertx vertx) {
      RedisOptions config = respBuilders.get(user);

      if (config == null) {
         fail(this.getClass().getSimpleName() + " does not define configuration for user " + user);
      }

      RespTestClientDriver driver = ext.resp()
            .withOptions(config)
            .withVertx(vertx);
      if (useSsl) {
         driver = driver.requireSsl(user != TestUser.ANONYMOUS);
      } else {
         driver = driver.withUser(user);
      }
      return driver.get();
   }

   private RedisAPI createConnection(TestUser user, Vertx vertx) {
      return RedisAPI.api(createClient(user, vertx));
   }

   private RedisAPI createConnection(Redis client) {
      return RedisAPI.api(client);
   }
}
