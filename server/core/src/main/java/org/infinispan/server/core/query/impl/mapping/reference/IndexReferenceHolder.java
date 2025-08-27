package org.infinispan.server.core.query.impl.mapping.reference;

import java.util.Map;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.IndexObjectFieldReference;
import org.hibernate.search.engine.spatial.GeoPoint;

public class IndexReferenceHolder {

   private final Map<String, IndexFieldReference<?>> fieldReferenceMap;
   private final Map<String, IndexObjectFieldReference> objectReferenceMap;
   private final Map<String, GeoIndexFieldReference> geoReferenceMap;

   public IndexReferenceHolder(Map<String, IndexFieldReference<?>> fieldReferenceMap,
                               Map<String, IndexObjectFieldReference> objectReferenceMap,
                               Map<String, GeoIndexFieldReference> geoReferenceMap) {
      this.fieldReferenceMap = fieldReferenceMap;
      this.objectReferenceMap = objectReferenceMap;
      this.geoReferenceMap = geoReferenceMap;
   }

   public IndexFieldReference<?> getFieldReference(String absoluteFieldPath) {
      return fieldReferenceMap.get(absoluteFieldPath);
   }

   public IndexObjectFieldReference getObjectReference(String absoluteObjectFieldPath) {
      return objectReferenceMap.get(absoluteObjectFieldPath);
   }

   public GeoIndexFieldReference getGeoReference(String absoluteFieldPath) {
      return geoReferenceMap.get(absoluteFieldPath);
   }

   public record GeoIndexFieldReference(IndexReferenceHolder.GeoIndexFieldReference.Role role,
                                        IndexFieldReference<GeoPoint> fieldReference,
                                        String indexFieldName) {
         public enum Role {LON, LAT}
   }
}
