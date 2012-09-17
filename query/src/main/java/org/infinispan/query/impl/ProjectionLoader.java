package org.infinispan.query.impl;

import org.hibernate.search.query.engine.spi.EntityInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 *
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public class ProjectionLoader implements QueryResultLoader {

   private final ProjectionConverter projectionConverter;

   public ProjectionLoader(ProjectionConverter projectionConverter) {
      this.projectionConverter = projectionConverter;
   }

   @Override
   public List<Object> load(Collection<EntityInfo> entityInfos) {
      List<Object> list = new ArrayList<Object>(entityInfos.size());
      for (EntityInfo entityInfo : entityInfos) {
         list.add( load( entityInfo ) );
      }
      return list;
   }

   public Object[] load(EntityInfo entityInfo) {
      Object[] projection = entityInfo.getProjection();
      return projectionConverter.convert(projection);
   }
}
