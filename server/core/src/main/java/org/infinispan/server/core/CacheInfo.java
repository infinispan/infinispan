package org.infinispan.server.core;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.SpanCategory;
import org.infinispan.telemetry.impl.CacheSpanAttribute;
import org.infinispan.util.KeyValuePair;

/**
 * @since 12.0
 */
public class CacheInfo<K, V> {
   private final Map<KeyValuePair<MediaType, MediaType>, AdvancedCache<K, V>> encodedCaches = new ConcurrentHashMap<>();
   protected final AdvancedCache<K, V> cache;
   private final InfinispanSpanAttributes attributes;
   private final VersionGenerator versionGenerator;

   public CacheInfo(AdvancedCache<K, V> cache) {
      this.cache = cache;

      ComponentRegistry cr = SecurityActions.getCacheComponentRegistry(cache);
      this.attributes = cr.getComponent(CacheSpanAttribute.class).getAttributes(SpanCategory.CONTAINER);
      this.versionGenerator = cr.getComponent(BasicComponentRegistry.class)
            .getComponent(KnownComponentNames.HOT_ROD_VERSION_GENERATOR, VersionGenerator.class)
            .running();
   }

   public AdvancedCache<K, V> getCache(KeyValuePair<MediaType, MediaType> mediaTypes, Subject subject) {
      AdvancedCache<K, V> encodedCache = encodedCaches.get(mediaTypes);
      if (encodedCache == null) {
         encodedCache = cache.withMediaType(mediaTypes.getKey(), mediaTypes.getValue());
         encodedCaches.put(mediaTypes, encodedCache);
      }
      if (subject == null) {
         return encodedCache;
      } else {
         return encodedCache.withSubject(subject);
      }
   }

   public AdvancedCache<K, V> getCache() {
      return cache;
   }

   public InfinispanSpanAttributes getInfinispanSpanAttributes() {
      return attributes;
   }

   public final VersionGenerator versionGenerator() {
      return versionGenerator;
   }
}
