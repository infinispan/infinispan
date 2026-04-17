package org.infinispan.server.functional.resp;

import static org.infinispan.testing.Eventually.eventually;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisConnection;
import io.vertx.redis.client.RedisOptions;

@ExtendWith(VertxExtension.class)
public abstract class AbstractRespTest {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   protected final RedisAPI createConnection(Vertx vertx) {
      return RedisAPI.api(createBaseClient(vertx));
   }

   protected final Redis createBaseClient(Vertx vertx) {
      return SERVERS.resp().withVertx(vertx).get();
   }

   protected final RedisAPI createDirectConnection(int index, Vertx vertx, RedisOptions options) {
      return RedisAPI.api(SERVERS.resp().withOptions(options).withVertx(vertx).get(index));
   }

   protected RedisAPI createDirectConnection(int index, Vertx vertx) {
      Redis redis = SERVERS.resp().withVertx(vertx).get(index);
      Future<RedisConnection> f = redis.connect();
      eventually(f::succeeded);
      return RedisAPI.api(f.result());
   }
}
