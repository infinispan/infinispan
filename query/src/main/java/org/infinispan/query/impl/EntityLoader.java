package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.search.mapper.common.EntityReference;

import org.infinispan.AdvancedCache;
import org.infinispan.encoding.DataConversion;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public final class EntityLoader<E> implements QueryResultLoader<E> {

   private final AdvancedCache<?, E> cache;
   private final KeyTransformationHandler keyTransformationHandler;
   private final DataConversion keyDataConversion;

   public EntityLoader(AdvancedCache<?, E> cache, KeyTransformationHandler keyTransformationHandler) {
      this.cache = cache;
      this.keyTransformationHandler = keyTransformationHandler;
      this.keyDataConversion = cache.getKeyDataConversion();
   }

   private Object decodeKey(EntityReference entityReference) {
      return keyDataConversion.fromStorage(keyTransformationHandler.stringToKey((String) entityReference.getId()));
   }

   @Override
   public E loadBlocking(EntityReference entityReference) {
      return cache.get(decodeKey(entityReference));
   }

   @Override
   public List<E> loadBlocking(List<EntityReference> entityReferences) {
      int entitiesSize = entityReferences.size();
      LinkedHashSet<Object> keys = new LinkedHashSet<>(entitiesSize);
      for (EntityReference entityReference : entityReferences) {
         keys.add(decodeKey(entityReference));
      }

      // getAll instead of multiple gets to get all the results in the same call
      Map<?, E> values = cache.getAll(keys);
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
