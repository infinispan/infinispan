package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.hibernate.search.engine.common.timing.Deadline;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public final class MetadataEntityLoader<E> implements PojoSelectionEntityLoader<EntityLoaded<E>> {

   private final AdvancedCache<?, E> cache;
   private final LocalQueryStatistics queryStatistics;
   private final QueryKeyConverter queryKeyConverter;

   MetadataEntityLoader(AdvancedCache<?, E> cache, LocalQueryStatistics queryStatistics) {
      this.cache = cache;
      this.queryStatistics = queryStatistics;
      queryKeyConverter = new QueryKeyConverter(cache);
   }

   @Override
   public List<EntityLoaded<E>> loadBlocking(List<?> identifiers, Deadline deadline) {
      if (identifiers.isEmpty()) return Collections.emptyList();

      Set<Object> keys = queryKeyConverter.convertKeys(identifiers);
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      // getAll instead of multiple gets to get all the results in the same call
      Map<?, ? extends CacheEntry<?, E>> entries = cache.getAllCacheEntries(keys);
      Map<?, ? extends CacheEntry<?, E>> values = queryKeyConverter.convertEntries(entries);

      if (queryStatistics.isEnabled()) queryStatistics.entityLoaded(System.nanoTime() - start);

      ArrayList<EntityLoaded<E>> result = new ArrayList<>(keys.size());
      for (Object key : keys) {
         // if the entity was present at indexing time and
         // it is not present anymore now at searching time,
         // we will add a null here
         CacheEntry<?, E> cacheEntry = values.get(key);
         if (cacheEntry != null) {
            result.add(new EntityLoaded<>(cacheEntry.getValue(), cacheEntry.getMetadata()));
         } else {
            result.add(null);
         }
      }

      return result;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MetadataEntityLoader<?> that = (MetadataEntityLoader<?>) o;
      return Objects.equals(cache, that.cache) && Objects.equals(queryStatistics, that.queryStatistics);
   }

   @Override
   public int hashCode() {
      return Objects.hash(cache, queryStatistics);
   }
}
