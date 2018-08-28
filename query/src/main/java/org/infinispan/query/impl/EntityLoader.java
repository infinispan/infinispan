package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
final class EntityLoader implements QueryResultLoader {

   private final AdvancedCache<?, ?> cache;
   private final KeyTransformationHandler keyTransformationHandler;

   EntityLoader(AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler) {
      this.keyTransformationHandler = keyTransformationHandler;
      this.cache = cache;
   }

   private Object decodeKey(EntityInfo entityInfo) {
      return keyTransformationHandler.stringToKey(entityInfo.getId().toString(), cache.getClassLoader());
   }

   @Override
   public Object load(EntityInfo entityInfo) {
      return cache.get(decodeKey(entityInfo));
   }

   @Override
   public List<Object> load(List<EntityInfo> entityInfos) {
      int entitiesSize = entityInfos.size();
      LinkedHashSet<Object> keys = new LinkedHashSet<>(entitiesSize);
      for (EntityInfo e : entityInfos) {
         keys.add(decodeKey(e));
      }
      // The entries will be in the same order as requested in keys LinkedHashSet because internally we preserve order by
      // using a LinkedHashMap for the result of getAll
      Map<?, ?> entries = cache.getAll(keys);
      return new ArrayList<>(entries.values());
   }
}
