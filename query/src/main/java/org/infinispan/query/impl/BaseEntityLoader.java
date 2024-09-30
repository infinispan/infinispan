package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.engine.common.timing.Deadline;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.infinispan.AdvancedCache;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.security.actions.SecurityActions;

abstract class BaseEntityLoader<E, T> implements PojoSelectionEntityLoader<EntityLoaded<E>> {

   protected final AdvancedCache<Object, Object> cache;
   private final LocalQueryStatistics queryStatistics;

   BaseEntityLoader(AdvancedCache<Object, Object> cache, LocalQueryStatistics queryStatistics) {
      this.cache = Objects.requireNonNull(cache);
      this.queryStatistics = Objects.requireNonNull(queryStatistics);
   }

   @Override
   public final List<EntityLoaded<E>> loadBlocking(List<?> identifiers, Deadline deadline) {
      assert identifiers != null;
      if (identifiers.isEmpty()) {
         return List.of();
      }

      // getAll instead of multiple gets to get all the results in the same call.
      Map<Object, T> values = queryStatistics.isEnabled() ?
            loadEntriesMeasuringDuration(identifiers) :
            loadEntries(identifiers);

      ArrayList<EntityLoaded<E>> result = new ArrayList<>(identifiers.size());
      for (Object key : identifiers) {
         // if the entity was present at indexing time and
         // it is not present anymore now at searching time,
         // we will add a null here
         result.add(toEntityLoader(values.get(key)));
      }

      return result;
   }

   private Map<Object, T> loadEntriesMeasuringDuration(List<?> keys) {
      var timeService = SecurityActions.getCacheComponentRegistry(cache).getTimeService();
      var start = timeService.time();
      try {
         return loadEntries(keys);
      } finally {
         queryStatistics.entityLoaded(timeService.timeDuration(start, TimeUnit.NANOSECONDS));
      }
   }

   @SuppressWarnings("unchecked")
   protected final E fromStorage(Object value) {
      assert value != null;
      return (E) cache.getValueDataConversion().fromStorage(value);
   }

   /**
    * Fetches the values (or entries) from the cache in the storage format.
    */
   abstract Map<Object, T> loadEntries(List<?> keys);

   /**
    * Converts the value (or entry) from {@link #loadEntries(List)}, in storage format, to the {@link EntityLoaded} in
    * user format.
    * <p>
    * If the value is {@code null}, this method should return {@code null} too.
    */
   abstract EntityLoaded<E> toEntityLoader(T value);

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BaseEntityLoader<?, ?> that = (BaseEntityLoader<?, ?>) o;
      return cache.equals(that.cache) && queryStatistics.equals(that.queryStatistics);
   }

   @Override
   public int hashCode() {
      int result = cache.hashCode();
      result = 31 * result + queryStatistics.hashCode();
      return result;
   }
}
