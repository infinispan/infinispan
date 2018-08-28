package org.infinispan.query.impl;

import static java.util.Arrays.stream;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.search.query.engine.spi.EntityInfo;

/**
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
final class ProjectionLoader implements QueryResultLoader {

   private final ProjectionConverter projectionConverter;
   private final EntityLoader entityLoader;

   ProjectionLoader(ProjectionConverter projectionConverter, EntityLoader entityLoader) {
      this.projectionConverter = projectionConverter;
      this.entityLoader = entityLoader;
   }

   @Override
   public List<Object> load(List<EntityInfo> entityInfos) {
      List<Object> list = new ArrayList<>(entityInfos.size());
      for (EntityInfo entityInfo : entityInfos) {
         list.add(load(entityInfo));
      }
      return list;
   }

   @Override
   public Object[] load(EntityInfo entityInfo) {
      Object[] projection = entityInfo.getProjection();
      if (stream(projection).anyMatch(o -> o == EntityInfo.ENTITY_PLACEHOLDER)) {
         entityInfo.populateWithEntityInstance(entityLoader.load(entityInfo));
      }
      return projectionConverter.convert(projection);
   }
}
