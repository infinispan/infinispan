package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.search.query.engine.spi.EntityInfo;
import org.infinispan.AdvancedCache;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 5.0
 */
public class EntityLoader implements QueryResultLoader {

   private final AdvancedCache<?, ?> cache;
   private final KeyTransformationHandler keyTransformationHandler;

   public EntityLoader(AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler) {
      this.keyTransformationHandler = keyTransformationHandler;
      this.cache = cache;
   }

   private Object decodeKey(EntityInfo entityInfo) {
      return keyTransformationHandler.stringToKey(entityInfo.getId().toString(), cache.getClassLoader());
   }

   public Object load(EntityInfo entityInfo) {
      return cache.get(decodeKey(entityInfo));
   }

   public List<Object> load(List<EntityInfo> entityInfos) {
      int entitiesSize = entityInfos.size();
      Set<Object> keys = new LinkedHashSet<>(entitiesSize);
      for (EntityInfo e : entityInfos) {
         keys.add(decodeKey(e));
      }
      Map<?, ?> entries = cache.getAll(keys);
      return new ArrayList<>(entries.values());
   }

}
