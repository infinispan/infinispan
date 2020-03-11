package org.infinispan.server.memcached.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.memcached.MemcachedDecoder;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.server.memcached.logging.Log;

import io.netty.channel.ChannelInboundHandler;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;

/**
 * Utils for Memcached tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public class MemcachedTestingUtil {
   private static final Log log = LogFactory.getLog(MemcachedTestingUtil.class, Log.class);

   private static final String host = "127.0.0.1";

   public static MemcachedClient createMemcachedClient(long timeout, int port) throws IOException {
      DefaultConnectionFactory d = new DefaultConnectionFactory() {
         @Override
         public long getOperationTimeout() {
            return timeout;
         }
      };
      return new MemcachedClient(d, Collections.singletonList(new InetSocketAddress(host, port)));
   }

   public static MemcachedServer startMemcachedTextServer(EmbeddedCacheManager cacheManager) {
      return startMemcachedTextServer(cacheManager, UniquePortThreadLocal.INSTANCE.get());
   }

   public static MemcachedServer startMemcachedTextServer(EmbeddedCacheManager cacheManager, int port) {
      return startMemcachedTextServer(cacheManager, port, MediaType.APPLICATION_OCTET_STREAM);
   }

   public static MemcachedServer startMemcachedTextServer(EmbeddedCacheManager cacheManager, int port, MediaType valueMediaType) {
      MemcachedServer server = new MemcachedServer();
      String serverName = TestResourceTracker.getCurrentTestShortName();
      server.start(new MemcachedServerConfigurationBuilder().name(serverName).host(host).port(port).clientEncoding(valueMediaType).build(),
            cacheManager);
      return server;
   }

   public static MemcachedServer startMemcachedTextServer(EmbeddedCacheManager cacheManager, String cacheName) {
      return startMemcachedTextServer(cacheManager, UniquePortThreadLocal.INSTANCE.get(), cacheName);
   }

   public static MemcachedServer startMemcachedTextServer(EmbeddedCacheManager cacheManager, int port, String cacheName) {
      return startMemcachedTextServer(cacheManager, port, cacheName, MediaType.APPLICATION_OCTET_STREAM);
   }

   public static MemcachedServer startMemcachedTextServer(EmbeddedCacheManager cacheManager, int port, String cacheName, MediaType valueMediaType) {
      MemcachedServer server = new MemcachedServer() {
         @Override
         public ChannelInboundHandler getDecoder() {
            Cache<byte[], byte[]> cache = getCacheManager().getCache(cacheName);
            return new MemcachedDecoder(cache.getAdvancedCache(), scheduler, transport, s -> false, valueMediaType);
         }

         @Override
         protected void startCaches() {
            getCacheManager().getCache(cacheName);
         }

      };
      String serverName = TestResourceTracker.getCurrentTestShortName();
      server.start(new MemcachedServerConfigurationBuilder().name(serverName).host(host).port(port).build(),
            cacheManager);
      return server;
   }

   public static void killMemcachedClient(MemcachedClient client) {
      try {
         if (client != null) client.shutdown();
      }
      catch (Throwable t) {
         log.error("Error stopping client", t);
      }
   }

   public static void killMemcachedServer(MemcachedServer server) {
      if (server != null) server.stop();
   }

   private static final class UniquePortThreadLocal extends ThreadLocal<Integer> {

      static UniquePortThreadLocal INSTANCE = new UniquePortThreadLocal();

      private static final AtomicInteger uniqueAddr = new AtomicInteger(16211);

      @Override
      protected Integer initialValue() {
         return uniqueAddr.getAndAdd(100);
      }
   }
}
