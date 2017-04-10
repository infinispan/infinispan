package org.infinispan.rest.server.operations;

import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.MimeMetadataBuilder;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.server.CacheControl;

public class CacheOperationsHelper {
   private CacheOperationsHelper() {
   }

   public static Metadata createMetadata(Configuration cfg, String dataType, Optional<Long> ttl, Optional<Long> idleTime) {
      MimeMetadataBuilder metadata = new MimeMetadataBuilder();
      metadata.contentType(dataType);

      if(ttl.isPresent()) {
         metadata.lifespan(ttl.get(), TimeUnit.SECONDS);
      } else {
         metadata.lifespan(cfg.expiration().lifespan(), TimeUnit.MILLISECONDS);
      }

      if(idleTime.isPresent()) {
         metadata.maxIdle(idleTime.get(), TimeUnit.SECONDS);
      } else {
         metadata.maxIdle(cfg.expiration().maxIdle(), TimeUnit.MILLISECONDS);
      }

      return metadata.build();
   }

   public static boolean supportsExtendedHeaders(RestServerConfiguration restServerConfiguration, String extended) {
      switch (restServerConfiguration.extendedHeaders()) {
         case NEVER:
            return false;
         case ON_DEMAND:
            return extended != null;
         default:
            return false;
      }
   }

   public static CacheControl calcCacheControl(Date expires) {
      if (expires == null) {
         return null;
      }
      int maxAgeSeconds = calcFreshness(expires);
      if (maxAgeSeconds > 0)
         return CacheControl.maxAge(maxAgeSeconds);
      else
         return CacheControl.noCache();
   }


   public static boolean entryFreshEnough(Date entryExpires, OptionalInt minFresh) {
      return !minFresh.isPresent() || minFresh.getAsInt() < calcFreshness(entryExpires);
   }

   public static int calcFreshness(Date expires) {
      if (expires == null) {
         return Integer.MAX_VALUE;
      } else {
         return ((int) (expires.getTime() - new Date().getTime()) / 1000);
      }
   }

   public static OptionalInt minFresh(String cacheControl) {
      return Arrays.stream(cacheControl.split(","))
            .filter(s -> s.contains("min-fresh"))
            .map(s -> {
               String[] equals = s.split("=");
               return OptionalInt.of(Integer.parseInt(equals[equals.length - 1].trim()));
            })
            .findFirst()
            .orElse(OptionalInt.empty());
   }

   public static <K, V> Date lastModified(InternalCacheEntry<K, V> ice) {
      return new Date(ice.getCreated() / 1000 * 1000);
   }

}
