package org.infinispan.server.functional.resp;

import java.util.concurrent.ExecutionException;

import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisConnection;

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

   protected RedisAPI createDirectConnection(int index, Vertx vertx) {
       try {
         RedisConnection conn = CompletionStages.await(SERVERS.resp().withVertx(vertx).get(index).connect().toCompletionStage());
         return RedisAPI.api(conn);
      } catch (ExecutionException | InterruptedException e) {
         throw new RuntimeException(e);
      }
   }
}
