package org.infinispan.query.dsl;

/**
 * @author anistor@redhat.com
 * @since 9.4
 */
public interface ProjectionConstants {

   // same as org.infinispan.query.ProjectionConstants.KEY
   String KEY = "__ISPN_Key";

   // same as org.hibernate.search.engine.ProjectionConstants.THIS
   String VALUE = "__HSearch_This";

   // same as org.hibernate.search.engine.ProjectionConstants.SPATIAL_DISTANCE;
   String SPATIAL_DISTANCE = "_HSearch_SpatialDistance";
}
