package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.hibernate.search.engine.common.timing.spi.Deadline;
import org.infinispan.AdvancedCache;
import org.infinispan.encoding.DataConversion;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.search.mapper.common.EntityReference;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public final class EntityLoader<E> implements QueryResultLoader<E> {

   private final LocalQueryStatistics queryStatistics;
   private final AdvancedCache<?, E> cache;
   private final KeyTransformationHandler keyTransformationHandler;
   private final DataConversion keyDataConversion;

   public EntityLoader(LocalQueryStatistics queryStatistics, AdvancedCache<?, E> cache,
                       KeyTransformationHandler keyTransformationHandler) {
      this.cache = cache;
      this.keyTransformationHandler = keyTransformationHandler;
      this.keyDataConversion = cache.getKeyDataConversion();
      this.queryStatistics = queryStatistics;
   }

   private Object decodeKey(EntityReference entityReference) {
      return keyDataConversion.fromStorage(entityReference.key());
   }

   @Override
   public E loadBlocking(EntityReference entityReference) {
      return cache.get(decodeKey(entityReference));
   }

   @Override
   public List<E> loadBlocking(List<EntityReference> entityReferences, Deadline deadline) {
      if (entityReferences.isEmpty()) return Collections.emptyList();

      int entitiesSize = entityReferences.size();
      LinkedHashSet<Object> keys = new LinkedHashSet<>(entitiesSize);
      for (EntityReference entityReference : entityReferences) {
         keys.add(decodeKey(entityReference));
      }

      // getAll instead of multiple gets to get all the results in the same call
      long start = 0;
      if (queryStatistics.isEnabled()) start = System.nanoTime();

      Map<?, E> values = cache.getAll(keys);

      if (queryStatistics.isEnabled()) queryStatistics.entityLoaded(System.nanoTime() - start);

      ArrayList<E> result = new ArrayList<>(entityReferences.size());
      for (Object key : keys) {
         // if the entity was present at indexing time and
         // it is not present anymore now at searching time,
         // we will add a null here
         result.add(values.get(key));
      }

      return result;
   }
}
