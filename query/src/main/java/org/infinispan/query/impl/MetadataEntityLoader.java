package org.infinispan.query.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public final class MetadataEntityLoader<E> extends BaseEntityLoader<E, CacheEntry<Object, Object>> {

   MetadataEntityLoader(AdvancedCache<Object, Object> cache, LocalQueryStatistics queryStatistics) {
      super(cache, queryStatistics);
   }

   @Override
   Map<Object, CacheEntry<Object, Object>> loadEntries(List<?> keys) {
      return cache.withStorageMediaType().getAllCacheEntries(Set.copyOf(keys));
   }

   @Override
   EntityLoaded<E> toEntityLoader(CacheEntry<Object, Object> entry) {
      return entry == null || entry.getValue() == null ? null : new EntityLoaded<>(fromStorage(entry.getValue()), entry.getMetadata());
   }
}
