package org.infinispan.rest.operations;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.CacheControl;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.operations.exceptions.WrongDateFormatException;

public class CacheOperationsHelper {

   private CacheOperationsHelper() {
   }

   public static Metadata createMetadata(Configuration cfg, Optional<Long> ttl, Optional<Long> idleTime) {
      EmbeddedMetadata.Builder metadata = new EmbeddedMetadata.Builder();

      if (ttl.isPresent()) {
         long ttlValue = ttl.get();
         if (ttlValue < 0) {
            metadata.lifespan(-1);
         } else if (ttlValue == 0) {
            metadata.lifespan(cfg.expiration().lifespan(), TimeUnit.MILLISECONDS);
         } else {
            metadata.lifespan(ttlValue, TimeUnit.SECONDS);
         }
      } else {
         metadata.lifespan(cfg.expiration().lifespan(), TimeUnit.MILLISECONDS);
      }

      if (idleTime.isPresent()) {
         long idleValue = idleTime.get();
         if (idleValue < 0) {
            metadata.maxIdle(-1);
         } else if (idleValue == 0) {
            metadata.maxIdle(cfg.expiration().maxIdle(), TimeUnit.MILLISECONDS);
         } else {
            metadata.maxIdle(idleValue, TimeUnit.SECONDS);
         }
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

   public static boolean ifUnmodifiedIsBeforeEntryModificationDate(Optional<String> ifUnmodifiedSince, Date lastMod) throws WrongDateFormatException {
      if (ifUnmodifiedSince.isPresent()) {
         try {
            ZonedDateTime clientTime = ZonedDateTime.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(ifUnmodifiedSince.get()));
            ZonedDateTime modificationTime = ZonedDateTime.ofInstant(lastMod.toInstant(), ZoneId.systemDefault());
            return modificationTime.isAfter(clientTime);
         } catch (DateTimeParseException e) {
            throw new WrongDateFormatException("Could not parse date " + ifUnmodifiedSince.get());
         }
      }
      return false;
   }

   public static boolean ifMatchDoesntMatchEtag(Optional<String> etagIfMatch, String etag) {
      if (etagIfMatch.isPresent()) {
         return !etagIfMatch.get().equals(etag);
      }
      return false;
   }

   public static boolean ifNoneMatchMathesEtag(Optional<String> etagIfNoneMatch, String etag) {
      if (etagIfNoneMatch.isPresent()) {
         return etagIfNoneMatch.get().equals(etag);
      }
      return false;
   }

   public static boolean ifModifiedIsAfterEntryModificationDate(Optional<String> etagIfModifiedSince, Date lastMod) throws WrongDateFormatException {
      if (etagIfModifiedSince.isPresent()) {
         try {
            ZonedDateTime clientTime = ZonedDateTime.from(DateTimeFormatter.RFC_1123_DATE_TIME.parse(etagIfModifiedSince.get()));
            ZonedDateTime modificationTime = ZonedDateTime.ofInstant(lastMod.toInstant(), ZoneId.systemDefault());
            return clientTime.isAfter(modificationTime) || clientTime.isEqual(modificationTime);
         } catch (DateTimeParseException e) {
            throw new WrongDateFormatException("Could not parse date " + etagIfModifiedSince.get());
         }
      }
      return false;
   }
}
