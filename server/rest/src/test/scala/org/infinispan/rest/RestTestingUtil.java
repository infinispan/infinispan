package org.infinispan.rest;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;

public class RestTestingUtil {
   static final Log log = LogFactory.getLog(RestTestingUtil.class);

   public static EmbeddedRestServer startRestServer(EmbeddedCacheManager manager) {
      return startRestServer(manager, UniquePortThreadLocal.get().intValue());
   }

   public static EmbeddedRestServer startRestServer(EmbeddedCacheManager manager, int port) {
      return startRestServer(manager, port, new RestServerConfigurationBuilder());
   }

   public static EmbeddedRestServer startRestServer(EmbeddedCacheManager manager, int port, RestServerConfigurationBuilder builder) {
      EmbeddedRestServer server = new EmbeddedRestServer(port, manager, builder.build());
      try {
         server.start();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      return server;
   }

   public static void killServers(EmbeddedRestServer... servers) {
      if (servers != null) {
         for (EmbeddedRestServer server : servers) {
            try {
               if (server != null)
                  server.stop();
            } catch (Throwable t) {
               log.warn("Error stopping Rest server", t);
            }
         }
      }
   }

   static ThreadLocal<Integer> UniquePortThreadLocal = new ThreadLocal<Integer>() {
      private AtomicInteger uniquePort = new AtomicInteger(18080);

      @Override
      public Integer initialValue() {
         return uniquePort.getAndAdd(100);
      }
   };
}
