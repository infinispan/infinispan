package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.hibernate.search.engine.common.timing.Deadline;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.infinispan.AdvancedCache;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public final class EntityLoader<E> implements PojoSelectionEntityLoader<E> {

   private final AdvancedCache<?, E> cache;
   private final LocalQueryStatistics queryStatistics;

   public EntityLoader(AdvancedCache<?, E> cache, LocalQueryStatistics queryStatistics) {
      this.cache = cache;
      this.queryStatistics = queryStatistics;
   }

   @Override
   public List<E> loadBlocking(List<?> identifiers, Deadline deadline) {
      if (identifiers.isEmpty()) return Collections.emptyList();

      int entitiesSize = identifiers.size();
      LinkedHashSet<Object> keys = new LinkedHashSet<>(entitiesSize);
      for (Object identifier : identifiers) {
         keys.add(cache.getKeyDataConversion().fromStorage(identifier));
      }

      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      // getAll instead of multiple gets to get all the results in the same call
      Map<?, E> values = cache.getAll(keys);

      if (queryStatistics.isEnabled()) queryStatistics.entityLoaded(System.nanoTime() - start);

      ArrayList<E> result = new ArrayList<>(entitiesSize);
      for (Object key : keys) {
         // if the entity was present at indexing time and
         // it is not present anymore now at searching time,
         // we will add a null here
         result.add(values.get(key));
      }

      return result;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EntityLoader<?> that = (EntityLoader<?>) o;
      return Objects.equals(cache, that.cache) && Objects.equals(queryStatistics, that.queryStatistics);
   }

   @Override
   public int hashCode() {
      return Objects.hash(cache, queryStatistics);
   }
}
