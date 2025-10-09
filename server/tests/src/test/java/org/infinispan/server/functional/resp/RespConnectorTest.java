package org.infinispan.server.functional.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.k;

import java.util.List;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.commons.configuration.StringConfiguration;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.RedisAPI;

public class RespConnectorTest extends AbstractRespTest {

   @Test
   public void testDefaultCachePerConnector(Vertx vertx, VertxTestContext ctx) {
      // Get the default resp connection
      RedisAPI redis1 = createConnection(vertx);
      // Set k() in the default cache
      redis1.set(List.of(k(), "v1"))
            .onFailure(ctx::failNow)
            .compose(ignore -> redis1.get(k()).onFailure(ctx::failNow))
            .onSuccess(v -> ctx.verify(() -> {
               assertThat(v.toString()).isEqualTo("v1");
               ctx.completeNow();
            }));

      // Create a cache with alias `3` that will be used by the dedicated resp connector
      RemoteCacheManager rcm = SERVERS.hotrod().createRemoteCacheManager();
      RemoteCacheManagerAdmin admin = rcm.administration();
      admin.createCache("anotherRespCache",
            new StringConfiguration("""
                  {
                     "distributed-cache" : {
                        "aliases": ["3"]
                     }
                  }
                  """));

      RedisAPI redis2 = RedisAPI.api(SERVERS.resp().withPort(6380).withVertx(vertx).get());
      // Verify that this connector doesn't see the k() from the default connector
      redis2.get(k())
            .onFailure(ctx::failNow)
            .compose(ignore -> redis1.get(k()).onFailure(ctx::failNow))
            .onSuccess(v -> ctx.verify(() -> {
               assertThat(v).isNull();
               ctx.completeNow();
            }));
      // Set k() in the dedicated connector to a different value
      redis2.set(List.of(k(), "v2"))
            .onFailure(ctx::failNow)
            .compose(ignore -> redis2.get(k()).onFailure(ctx::failNow))
            .onSuccess(v -> ctx.verify(() -> {
               assertThat(v.toString()).isEqualTo("v2");
               ctx.completeNow();
            }));
      // Ensure that the default connector still sees its value
      redis1.get(k())
            .onFailure(ctx::failNow)
            .onSuccess(v -> ctx.verify(() -> {
               assertThat(v.toString()).isEqualTo("v1");
               ctx.completeNow();
            }));
      // Select the `3` database from the default connector and check that we can see the entry inserted by the dedicated connector
      redis1.select("3")
            .onFailure(ctx::failNow)
            .compose(ignore -> redis1.get(k()).onFailure(ctx::failNow))
            .onSuccess(v -> ctx.verify(() -> {
               assertThat(v.toString()).isEqualTo("v2");
               ctx.completeNow();
            }));
   }
}
