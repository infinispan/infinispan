package org.infinispan.query.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.query.ProjectionConstants;
import org.infinispan.query.backend.KeyTransformationHandler;

import java.util.LinkedList;
import java.util.List;

/**
 * Converts between Infinispan and HSearch projection fields.
 *
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public class ProjectionConverter {

   private final AdvancedCache<?, ?> cache;
   private final KeyTransformationHandler keyTransformationHandler;
   private final String[] hibernateSearchFields;
   private final List<Integer> indexesOfKey = new LinkedList<Integer>();

   public ProjectionConverter(String[] fields, AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler) {
      this.cache = cache;
      this.keyTransformationHandler = keyTransformationHandler;

      hibernateSearchFields = fields.clone();
      for (int i = 0; i < hibernateSearchFields.length; i++) {
         String field = hibernateSearchFields[i];
         if (field.equals( ProjectionConstants.KEY )) {
            hibernateSearchFields[i] = ProjectionConstants.ID;
            indexesOfKey.add( i );
         }
      }
   }

   public String[] getHSearchProjection() {
      return hibernateSearchFields;
   }

   public Object[] convert(Object[] projection) {
      for (Integer index : indexesOfKey) {
         projection[index] = keyTransformationHandler.stringToKey( (String) projection[index], cache.getClassLoader() );
      }
      return projection;
   }
}
