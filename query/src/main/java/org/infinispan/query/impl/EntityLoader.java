package org.infinispan.query.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

   public Object load(EntityInfo entityInfo) {
      Object cacheKey = keyTransformationHandler.stringToKey(entityInfo.getId().toString(), cache.getClassLoader());
      return cache.get(cacheKey);
   }

   public List<Object> load(Collection<EntityInfo> entityInfos) {
      ArrayList<Object> list = new ArrayList<Object>(entityInfos.size());
      for (EntityInfo e : entityInfos) {
         Object entity = load(e);
         if (entity != null) {
            list.add(entity);
         }
      }
      return list;
   }

}
