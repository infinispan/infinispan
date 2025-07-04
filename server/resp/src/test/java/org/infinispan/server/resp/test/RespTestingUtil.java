package org.infinispan.server.resp.test;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import javax.security.auth.Subject;

/**
 * Utils for RESP tests.
 *
 * @author William Burns
 * @since 14.0
 */
public class RespTestingUtil {
   private static final Log log = LogFactory.getLog(RespTestingUtil.class, Log.class);

   public static final Subject ADMIN;
   public static final String USERNAME = "default";
   public static final String PASSWORD = "password";
   public static final String OK = "OK";
   public static final String PONG = "PONG";
   public static final String HOST = "127.0.0.1";

   static {
      Map<AuthorizationPermission, Subject> subjects = TestingUtil.makeAllSubjects();
      ADMIN = subjects.get(AuthorizationPermission.ALL);
   }

   public static RedisClient createClient(long timeout, int port) {
      RedisClient client = RedisClient.create(RedisURI.Builder.redis(HOST).withPort(port)
            .build());
      client.setDefaultTimeout(Duration.ofMillis(timeout));
      return client;
   }

   public static RespServer startServer(EmbeddedCacheManager cacheManager) {
      return startServer(cacheManager, UniquePortThreadLocal.INSTANCE.get());
   }

   public static RespServer startServer(EmbeddedCacheManager cacheManager, int port) {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      return startServer(cacheManager, new RespServerConfigurationBuilder().name(serverName).host(HOST).port(port)
            .build());
   }

   public static RespServer startServer(EmbeddedCacheManager cacheManager, RespServerConfiguration configuration) {
      RespServer server = new RespServer();
      server.start(configuration, cacheManager);
      server.postStart();
      return server;
   }

   public static RespServer startServer(EmbeddedCacheManager cacheManager, String cacheName) {
      return startServer(cacheManager, UniquePortThreadLocal.INSTANCE.get(), cacheName);
   }

   public static RespServer startServer(EmbeddedCacheManager cacheManager, int port, String cacheName) {
      return startServer(cacheManager, port, cacheName, MediaType.APPLICATION_OCTET_STREAM);
   }

   public static RespServer startServer(EmbeddedCacheManager cacheManager, int port, String cacheName,
         MediaType valueMediaType) {
      RespServer server = new RespServer() {
         @Override
         protected void startCaches() {
            getCacheManager().getCache(cacheName);
         }
      };
      String serverName = TestResourceTracker.getCurrentTestShortName();
      server.start(new RespServerConfigurationBuilder().name(serverName).host(HOST).port(port).build(), cacheManager);
      server.postStart();
      return server;
   }

   public static void killClient(AbstractRedisClient client) {
      try {
         if (client != null)
            client.shutdown();
      } catch (Throwable t) {
         log.error("Error stopping client", t);
      }
   }

   public static void killServer(RespServer server) {
      if (server != null)
         server.stop();
   }

   public static int port() {
      return UniquePortThreadLocal.INSTANCE.get();
   }

   private static final class UniquePortThreadLocal extends ThreadLocal<Integer> {

      static UniquePortThreadLocal INSTANCE = new UniquePortThreadLocal();

      private static final AtomicInteger uniqueAddr = new AtomicInteger(
            1600 + ThreadLocalRandom.current().nextInt(1024));

      @Override
      protected Integer initialValue() {
         return uniqueAddr.getAndAdd(100);
      }
   }

   /**
    * Common assertion to check the Resp Wrong Type Error.
    * This occurs when, for example, when setting a key string and then trying to
    * apply a list command to that key.
    *
    * @param command1, sets a key value with a command
    * @param command2, runs another command type on the same key of command1
    */
   public static void assertWrongType(Runnable command1, Runnable command2) {
      command1.run();
      assertThatThrownBy(() -> {
         command2.run();
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }
}
