package org.infinispan.server.core.query.impl.indexing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.search.engine.backend.document.DocumentElement;
import org.hibernate.search.engine.backend.document.IndexFieldReference;
import org.hibernate.search.engine.cfg.spi.ConvertUtils;
import org.hibernate.search.engine.spatial.GeoPoint;
import org.infinispan.protostream.MessageContext;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.FieldDescriptor;
import org.infinispan.server.core.query.impl.indexing.aggregator.TypeAggregator;
import org.infinispan.server.core.query.impl.mapping.reference.IndexReferenceHolder;

public final class IndexingMessageContext extends MessageContext<IndexingMessageContext> {

   // null if the embedded is not indexed
   private final DocumentElement document;
   private final TypeAggregator typeAggregator;
   private final IndexingMessageContext parentContext;

   private Map<String, List<Float>> vectorAggregators;
   private Map<String, GeoPointInfo> geoPoints;

   public IndexingMessageContext(IndexingMessageContext parentContext, FieldDescriptor fieldDescriptor,
                                 Descriptor messageDescriptor, DocumentElement document,
                                 TypeAggregator typeAggregator) {
      super(parentContext, fieldDescriptor, messageDescriptor);
      this.document = document;
      this.typeAggregator = typeAggregator;
      this.parentContext = parentContext;
   }

   public DocumentElement getDocument() {
      return document;
   }

   public TypeAggregator getTypeAggregator() {
      return typeAggregator;
   }

   public void addValue(IndexFieldReference fieldReference, Object value) {
      if (document != null) {
         // using raw type for IndexFieldReference
         // value type and fieldReference value type are supposed to match
         document.addValue(fieldReference, value);
      }
   }

   public void addArrayItem(String fieldPath, Float value) {
      if (vectorAggregators == null) {
         vectorAggregators = new HashMap<>();
      }
      vectorAggregators.putIfAbsent(fieldPath, new ArrayList<>(50)); // guess a quite large value
      vectorAggregators.get(fieldPath).add(value);
   }

   public void addGeoValue(IndexReferenceHolder.GeoIndexFieldReference geoReference, Object value) {
      if (document == null) {
         parentContext.addGeoValue(geoReference, value);
         return;
      }

      if (geoPoints == null) {
         geoPoints = new HashMap<>();
      }
      Double converted = ConvertUtils.convertDouble(value);
      String indexFieldName = geoReference.indexFieldName();
      geoPoints.putIfAbsent(indexFieldName, new GeoPointInfo(geoReference.fieldReference()));
      if (geoReference.role().equals(IndexReferenceHolder.GeoIndexFieldReference.Role.LAT)) {
         geoPoints.get(indexFieldName).latitude = converted;
      } else {
         geoPoints.get(indexFieldName).longitude = converted;
      }
   }

   public void writeVectorAggregators(IndexReferenceHolder indexReferenceHolder) {
      if (vectorAggregators == null) {
         return;
      }
      for (Map.Entry<String, List<Float>> entry : vectorAggregators.entrySet()) {
         IndexFieldReference<?> fieldReference = indexReferenceHolder.getFieldReference(entry.getKey());
         List<Float> values = entry.getValue();
         float[] value = new float[values.size()];
         for (int i=0; i<values.size(); i++) {
            value[i] = values.get(i);
         }
         addValue(fieldReference, value);
      }
   }

   public void writeGeoPoints() {
      if (geoPoints == null) {
         return;
      }

      for (GeoPointInfo geoPointInfo : geoPoints.values()) {
         geoPointInfo.addValue(document);
      }
   }

   private static class GeoPointInfo {
      private final IndexFieldReference<GeoPoint> fieldReference;
      private Double latitude;
      private Double longitude;

      private GeoPointInfo(IndexFieldReference<GeoPoint> fieldReference) {
         this.fieldReference = fieldReference;
      }

      GeoPoint geoPoint() {
         return GeoPoint.of(latitude, longitude);
      }

      void addValue(DocumentElement document) {
         document.addValue(fieldReference, geoPoint());
      }
   }
}
