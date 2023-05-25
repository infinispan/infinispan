package org.infinispan.server.resp.test;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.logging.Log;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.data.Person;

import io.lettuce.core.RedisClient;

/**
 * Utils for RESP tests.
 *
 * @author William Burns
 * @since 14.0
 */
public class RespTestingUtil {
   public static final String NAMES_KEY = "names";
   public static final String EMPTY_KEY = "";
   public static final String NULL_KEY = null;
   public static final Person JULIEN = new Person("Julien");
   public static final Person OIHANA = new Person("Oihana");
   public static final Person RAMON = new Person("Ramon");
   public static final Person KOLDO = new Person("Koldo");
   public static final Person ELAIA = new Person("Elaia");
   public static final Person FELIX = new Person("Felix");
   public static final SuperPerson PEPE = new SuperPerson("Pepe");
   public static final SuperPerson NULL_USER = null;

   private static final Log log = LogFactory.getLog(RespTestingUtil.class, Log.class);

   public static final String OK = "OK";
   public static final String PONG = "PONG";
   public static final String HOST = "127.0.0.1";

   public static RedisClient createClient(long timeout, int port) {
      RedisClient client = RedisClient.create("redis://" + HOST + ":" + port);
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
      server.start(new RespServerConfigurationBuilder().name(serverName).host(HOST).port(port).build(),
            cacheManager);
      return server;
   }

   public static void killClient(RedisClient client) {
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
}
