package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.AfterClass;

import io.lettuce.core.RedisClient;
import io.lettuce.core.StatefulRedisConnectionImpl;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.ProtocolVersion;

public abstract class SingleNodeRespBaseTest extends AbstractRespTest {
   protected RedisClient client;
   protected RespServer server;
   protected StatefulRedisConnection<String, String> redisConnection;
   private final List<StatefulRedisConnection<?, ?>> connections = new ArrayList<>();
   protected Cache<Object, Object> cache;

   @Override
   protected void afterSetupFinished() {
      EmbeddedCacheManager cacheManager = manager(0);
      server = server(0);
      client = client(0);
      redisConnection = newConnection();
      cache = cacheManager.getCache(server.getConfiguration().defaultCacheName());
   }

   @Override
   protected TestSetup setup() {
      return TestSetup.singleNodeTestSetup();
   }

   protected RedisCodec<String, String> newCodec() {
      return StringCodec.UTF8;
   }

   protected final StatefulRedisConnection<String, String> newConnection() {
      StatefulRedisConnection<String, String> conn = client.connect(newCodec());
      connections.add(conn);
      return conn;
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      for (StatefulRedisConnection<?, ?> connection : connections) {
         if (connection instanceof StatefulRedisConnectionImpl<?, ?> srci) {
            assertThat(srci.getConnectionState().getNegotiatedProtocolVersion())
                  .isEqualTo(ProtocolVersion.RESP3);
         }
      }
      connections.forEach(StatefulRedisConnection::close);
      super.destroy();
   }
}
