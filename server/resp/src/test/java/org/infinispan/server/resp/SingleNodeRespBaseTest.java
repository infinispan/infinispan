package org.infinispan.server.resp;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.test.TestSetup;
import org.testng.annotations.AfterClass;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

public abstract class SingleNodeRespBaseTest extends AbstractRespTest {
   protected RedisClient client;
   protected RespServer server;
   protected StatefulRedisConnection<String, String> redisConnection;
   private final List<StatefulRedisConnection<String, String>> connections = new ArrayList<>();
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

   protected final StatefulRedisConnection<String, String> newConnection() {
      StatefulRedisConnection<String, String> conn = client.connect();
      connections.add(conn);
      return conn;
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      connections.forEach(StatefulRedisConnection::close);
      super.destroy();
   }
}
