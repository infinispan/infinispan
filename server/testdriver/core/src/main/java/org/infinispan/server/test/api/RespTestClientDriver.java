package org.infinispan.server.test.api;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.resource.ClientResources;

public class RespTestClientDriver extends BaseTestClientDriver<RespTestClientDriver> {

   private static final Logger log = LoggerFactory.getLogger(RespTestClientDriver.class);

   private final TestServer testServer;
   private final TestClient testClient;
   private LettuceConfiguration configuration;

   public RespTestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;
   }

   @Override
   public RespTestClientDriver self() {
      return this;
   }

   public RespTestClientDriver withConfiguration(LettuceConfiguration configuration) {
      this.configuration = configuration;
      return self();
   }

   public RedisClient get() {
      ClientResources resources = configuration.clientResources.build();
      RedisClient client = RedisClient.create(resources, configuration.redisURI);
      client.setOptions(configuration.clientOptions);
      testClient.registerResource(() -> {
         try {
            resources.shutdown(0, 15, TimeUnit.SECONDS).getNow();
            client.shutdownAsync(0, 15, TimeUnit.SECONDS).get(15, TimeUnit.SECONDS);
         } catch (InterruptedException | TimeoutException | ExecutionException e) {
            log.error("Timed out waiting RESP client to shutdown", e);
         }
      });
      return client;
   }

   public StatefulRedisConnection<String, String> connect(RedisClient client) {
      StatefulRedisConnection<String, String> connection = client.connect();
      testClient.registerResource(connection::close);
      return connection;
   }

   public StatefulRedisConnection<String, String> getConnection() {
      return connect(get());
   }

   public static class LettuceConfiguration {
      private final ClientResources.Builder clientResources;
      private final ClientOptions clientOptions;
      private final RedisURI redisURI;

      public LettuceConfiguration(ClientResources.Builder clientResourcesBuilder, ClientOptions clientOptions, RedisURI redisURI) {
         this.clientResources = clientResourcesBuilder;
         this.clientOptions = clientOptions;
         this.redisURI = redisURI;
      }
   }
}
