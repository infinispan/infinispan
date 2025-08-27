package org.infinispan.server.core.query.impl.mapping.reference;

import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.backend.document.model.dsl.IndexSchemaElement;
import org.hibernate.search.engine.backend.types.Aggregable;
import org.hibernate.search.engine.backend.types.Projectable;
import org.hibernate.search.engine.backend.types.Searchable;
import org.hibernate.search.engine.backend.types.Sortable;
import org.hibernate.search.engine.spatial.GeoPoint;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.server.core.query.impl.indexing.SpatialFieldMapping;

public class SpatialReferenceProvider {

   // Data (Proto) << longitude >>
   private final String longitudeName;

   // Data (Proto) << longitude >>
   private final String latitudeName;

   // Index (Lucene)
   private final String indexName;
   private final Searchable searchable;
   private final Projectable projectable;
   private final Aggregable aggregable;
   private final Sortable sortable;

   public SpatialReferenceProvider(SpatialFieldMapping spatialFieldMapping) {
      FieldDescriptor longitude = spatialFieldMapping.getLongitude();
      FieldDescriptor latitude = spatialFieldMapping.getLatitude();
      longitudeName = (longitude == null) ? spatialFieldMapping.fieldName() + ".longitude" : longitude.getName();
      latitudeName = (latitude == null) ? spatialFieldMapping.fieldName() + ".latitude" : latitude.getName();

      indexName = spatialFieldMapping.fieldName();
      projectable = (spatialFieldMapping.projectable()) ? Projectable.YES : Projectable.NO;

      // TODO ISPN-8238 add spatialFieldMapping#searchable
      searchable = Searchable.YES;
      // TODO ISPN-8238 add spatialFieldMapping#aggregable
      aggregable = Aggregable.NO;

      sortable = (spatialFieldMapping.sortable()) ? Sortable.YES : Sortable.NO;
   }

   public String longitudeName() {
      return longitudeName;
   }

   public String latitudeName() {
      return latitudeName;
   }

   public String indexName() {
      return indexName;
   }

   public IndexFieldReference<GeoPoint> bind(IndexSchemaElement indexSchemaElement) {
      if (nothingToBind()) {
         return null;
      }

      return indexSchemaElement.field(indexName, f -> f.asGeoPoint()
                  .searchable(searchable).projectable(projectable).aggregable(aggregable).sortable(sortable))
            .toReference();
   }

   public boolean nothingToBind() {
      return Searchable.NO.equals(searchable) && Projectable.NO.equals(projectable) &&
            Aggregable.NO.equals(aggregable) && Sortable.NO.equals(sortable);
   }
}
