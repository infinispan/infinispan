package org.infinispan.query.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public final class EntityLoader<E> extends BaseEntityLoader<E, Object> {


   EntityLoader(AdvancedCache<Object, Object> cache, LocalQueryStatistics queryStatistics) {
      super(cache, queryStatistics);
   }

   @Override
   Map<Object, Object> loadEntries(List<?> keys) {
      return cache.withStorageMediaType().getAll(Set.copyOf(keys));
   }

   @Override
   EntityLoaded<E> toEntityLoader(Object value) {
      return value == null ? null : new EntityLoaded<>(fromStorage(value), null);
   }
}
