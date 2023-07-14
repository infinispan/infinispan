package org.infinispan.server.functional.resp;

import java.net.InetSocketAddress;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;

@ExtendWith(VertxExtension.class)
public abstract class AbstractRespTest {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   private static RedisOptions options;

   @BeforeAll
   static void beforeAll() {
      int size = SERVERS.getServerDriver().getConfiguration().numServers();
      RedisOptions opts = new RedisOptions()
            .setPoolName("resp-tests-pool");

      if (size > 1) {
         opts = opts.setType(RedisClientType.CLUSTER);
      } else {
         opts = opts.setType(RedisClientType.STANDALONE);
      }

      for (int i = 0; i < size; i++) {
         InetSocketAddress serverSocket = SERVERS.getServerDriver().getServerSocket(i, 11222);
         String uri = "redis://" + serverSocket.getHostString() + ":" + serverSocket.getPort();
         opts = opts.addConnectionString(uri);
      }

      AbstractRespTest.options = opts;
   }

   protected final RedisAPI createConnection(Vertx vertx) {
      return RedisAPI.api(createBaseClient(vertx));
   }

   protected final Redis createBaseClient(Vertx vertx) {
      return SERVERS.resp().withOptions(options).withVertx(vertx).get();
   }

   protected final RedisAPI createConnection(Vertx vertx, RedisOptions options) {
      return RedisAPI.api(SERVERS.resp().withOptions(options).withVertx(vertx).get());
   }

   protected RedisAPI createDirectConnection(int index, Vertx vertx) {
      InetSocketAddress address = SERVERS.getServerDriver().getServerSocket(index, 11222);

      RedisOptions options = new RedisOptions()
            .setType(RedisClientType.STANDALONE)
            .setMaxPoolWaiting(-1)
            .addConnectionString("redis://" + address.getHostString() + ":" + address.getPort());

      return createConnection(vertx, options);
   }
}
