package org.infinispan.query.impl;

import java.util.ArrayList;

import org.infinispan.query.ProjectionConstants;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * Converts between Infinispan and HSearch projection fields.
 *
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
final class ProjectionConverter {

   private final KeyTransformationHandler keyTransformationHandler;
   private final String[] hibernateSearchFields;
   private final int[] indexesOfKey;

   ProjectionConverter(String[] fields, KeyTransformationHandler keyTransformationHandler) {
      this.keyTransformationHandler = keyTransformationHandler;

      hibernateSearchFields = fields.clone();
      ArrayList<Integer> positions = new ArrayList<>();
      for (int i = 0; i < hibernateSearchFields.length; i++) {
         if (ProjectionConstants.KEY.equals(hibernateSearchFields[i])) {
            hibernateSearchFields[i] = ProjectionConstants.ID;
            positions.add(i);
         }
      }
      if (positions.isEmpty()) {
         indexesOfKey = null;
      } else {
         indexesOfKey = new int[positions.size()];
         for (int i = 0; i < indexesOfKey.length; i++) {
            indexesOfKey[i] = positions.get(i);
         }
      }
   }

   public String[] getHSearchProjection() {
      return hibernateSearchFields;
   }

   public Object[] convert(Object[] projection) {
      if (indexesOfKey != null) {
         for (int i : indexesOfKey) {
            projection[i] = keyTransformationHandler.stringToKey((String) projection[i]);
         }
      }
      return projection;
   }
}
